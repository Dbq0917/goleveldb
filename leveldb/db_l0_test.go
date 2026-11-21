package leveldb

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/testutil"
)

func updateMax(dst *int64, n int64) {
	for {
		old := atomic.LoadInt64(dst)
		if n <= old || atomic.CompareAndSwapInt64(dst, old, n) {
			return
		}
	}
}

func TestDB_L0BurstReadWrite(t *testing.T) {
	const (
		n       = 20000
		writers = 8
	)

	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		Compression:                  opt.NoCompression,
		WriteBuffer:                  8 * 1024,
		CompactionL0Trigger:          4,
		WriteL0SlowdownTrigger:       32,
		WriteL0PauseTrigger:          128,
	})
	defer h.close()

	r := rand.New(rand.NewSource(1))
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%08d", i)
	}
	r.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	value := bytes.Repeat([]byte("v"), 512)

	var (
		next       int64
		maxL0      int64
		maxWrite   int64
		firstErr   error
		errMu      sync.Mutex
		completed  []string
		completedM sync.Mutex
		stop       = make(chan struct{})
	)
	fail := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	var writersWG sync.WaitGroup
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func() {
			defer writersWG.Done()
			for {
				i := int(atomic.AddInt64(&next, 1)) - 1
				if i >= n {
					return
				}
				start := time.Now()
				if err := h.db.Put([]byte(keys[i]), value, nil); err != nil {
					fail(err)
					return
				}
				updateMax(&maxWrite, int64(time.Since(start)))
				completedM.Lock()
				completed = append(completed, keys[i])
				completedM.Unlock()
			}
		}()
	}

	var readersWG sync.WaitGroup
	readersWG.Add(1)
	go func() {
		defer readersWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}

			completedM.Lock()
			if len(completed) == 0 {
				completedM.Unlock()
				continue
			}
			key := completed[r.Intn(len(completed))]
			completedM.Unlock()

			got, err := h.db.Get([]byte(key), nil)
			if err != nil {
				fail(fmt.Errorf("get %q: %v", key, err))
				return
			}
			if !bytes.Equal(got, value) {
				fail(fmt.Errorf("get %q: got %q", key, got))
				return
			}
		}
	}()

	readersWG.Add(1)
	go func() {
		defer readersWG.Done()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			}

			var prev []byte
			iter := h.db.NewIterator(nil, nil)
			for iter.Next() {
				key := iter.Key()
				if bytes.Equal(prev, key) {
					fail(fmt.Errorf("duplicate key %q during compaction", key))
					break
				}
				if !bytes.Equal(iter.Value(), value) {
					fail(fmt.Errorf("invalid value for %q during compaction", key))
					break
				}
				prev = append(prev[:0], key...)
			}
			if err := iter.Error(); err != nil {
				fail(err)
			}
			iter.Release()
		}
	}()

	readersWG.Add(1)
	go func() {
		defer readersWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			v := h.db.s.version()
			updateMax(&maxL0, int64(v.tLen(0)))
			v.release()
			time.Sleep(time.Millisecond)
		}
	}()

	writersWG.Wait()
	close(stop)
	readersWG.Wait()

	errMu.Lock()
	err := firstErr
	errMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("peak L0=%d max write=%v", maxL0, time.Duration(maxWrite))
	if maxL0 < 20 {
		t.Fatalf("L0 didn't build up enough to exercise burst behavior: peak=%d", maxL0)
	}
	if d := time.Duration(maxWrite); d > 500*time.Millisecond {
		t.Fatalf("write paused too long during compaction: %v", d)
	}

	h.compactMem()
	deadline := time.Now().Add(30 * time.Second)
	for {
		v := h.db.s.version()
		l0 := v.tLen(0)
		v.release()
		if l0 < h.o.GetCompactionL0Trigger() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("L0 did not converge: %d files remain", l0)
		}
		time.Sleep(10 * time.Millisecond)
	}

	count := 0
	iter := h.db.NewIterator(nil, nil)
	for iter.Next() {
		if !bytes.Equal(iter.Value(), value) {
			t.Fatalf("invalid value for %q", iter.Key())
		}
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	iter.Release()
	if count != n {
		t.Fatalf("iteration count: got=%d want=%d", count, n)
	}
}

func TestDB_L0PauseTimeout(t *testing.T) {
	h := newDbHarnessWopt(t, &opt.Options{
		DisableLargeBatchTransaction: true,
		WriteBuffer:                  1024,
		CompactionL0Trigger:          1,
		WriteL0SlowdownTrigger:       1,
		WriteL0PauseTrigger:          1,
	})
	defer h.close()

	// Keep table compaction busy so a write reaching the pause trigger has to
	// use the bounded wait instead of waiting for the whole compaction.
	h.stor.Stall(testutil.ModeRead, storage.TypeTable)
	defer h.stor.Release(testutil.ModeRead, storage.TypeTable)

	value := bytes.Repeat([]byte("v"), 700)
	var maxWrite time.Duration
	for i := 0; i < 20; i++ {
		start := time.Now()
		if err := h.db.Put([]byte(fmt.Sprintf("key-%02d", i)), value, nil); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d > maxWrite {
			maxWrite = d
		}
		time.Sleep(time.Millisecond)
	}
	if maxWrite > 200*time.Millisecond {
		t.Fatalf("write pause exceeded compaction budget: %v", maxWrite)
	}

	h.stor.Release(testutil.ModeRead, storage.TypeTable)
	h.waitCompaction()
}
