// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/journal"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type frozenMem struct {
	mem       *memDB
	journalFd storage.FileDesc
	seq       uint64
}

type memDB struct {
	db *DB
	*memdb.DB
	ref int32
}

func (m *memDB) getref() int32 {
	return atomic.LoadInt32(&m.ref)
}

func (m *memDB) incref() {
	atomic.AddInt32(&m.ref, 1)
}

func (m *memDB) decref() {
	if ref := atomic.AddInt32(&m.ref, -1); ref == 0 {
		// Only put back memdb with std capacity.
		if m.Capacity() == m.db.s.o.GetWriteBuffer() {
			m.Reset()
			m.db.mpoolPut(m.DB)
		}
		m.db = nil
		m.DB = nil
	} else if ref < 0 {
		panic("negative memdb ref")
	}
}

// Get latest sequence number.
func (db *DB) getSeq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

// Atomically adds delta to seq.
func (db *DB) addSeq(delta uint64) {
	atomic.AddUint64(&db.seq, delta)
}

func (db *DB) setSeq(seq uint64) {
	atomic.StoreUint64(&db.seq, seq)
}

func (db *DB) sampleSeek(ikey internalKey) {
	v := db.s.version()
	if v.sampleSeek(ikey) {
		// Trigger table compaction.
		db.compTrigger(db.tcompCmdC)
	}
	v.release()
}

func (db *DB) mpoolPut(mem *memdb.DB) {
	if !db.isClosed() {
		select {
		case db.memPool <- mem:
		default:
		}
	}
}

func (db *DB) mpoolGet(n int) *memDB {
	var mdb *memdb.DB
	select {
	case mdb = <-db.memPool:
	default:
	}
	if mdb == nil || mdb.Capacity() < n {
		mdb = memdb.New(db.s.icmp, maxInt(db.s.o.GetWriteBuffer(), n))
	}
	return &memDB{
		db: db,
		DB: mdb,
	}
}

func (db *DB) mpoolDrain() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			select {
			case <-db.memPool:
			default:
			}
		case <-db.closeC:
			ticker.Stop()
			// Make sure the pool is drained.
			select {
			case <-db.memPool:
			case <-time.After(time.Second):
			}
			close(db.memPool)
			return
		}
	}
}

// Create new memdb and froze the old one; need external synchronization.
// newMem only called synchronously by the writer.
func (db *DB) newMem(n int) (mem *memDB, err error) {
	fd := storage.FileDesc{Type: storage.TypeJournal, Num: db.s.allocFileNum()}
	w, err := db.s.stor.Create(fd)
	if err != nil {
		db.s.reuseFileNum(fd.Num)
		return
	}

	db.memMu.Lock()
	defer db.memMu.Unlock()

	if db.journal == nil {
		db.journal = journal.NewWriter(w)
	} else {
		if err := db.journal.Reset(w); err != nil {
			return nil, err
		}
		if err := db.journalWriter.Close(); err != nil {
			return nil, err
		}
		if db.mem != nil {
			db.frozenMems = append(db.frozenMems, frozenMem{
				mem:       db.mem,
				journalFd: db.journalFd,
				seq:       db.seq,
			})
		}
	}
	db.journalWriter = w
	db.journalFd = fd
	mem = db.mpoolGet(n)
	mem.incref() // for self
	mem.incref() // for caller
	db.mem = mem
	return
}

// Get all memdbs.
func (db *DB) getMems() (e *memDB, f []*memDB) {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	if len(db.frozenMems) > 0 {
		f = make([]*memDB, len(db.frozenMems))
		for i := range db.frozenMems {
			m := db.frozenMems[len(db.frozenMems)-1-i].mem
			m.incref()
			f[i] = m
		}
	}
	return db.mem, f
}

// Get effective memdb.
func (db *DB) getEffectiveMem() *memDB {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	return db.mem
}

// Get frozen memdb.
func (db *DB) getFrozenMem() *memDB {
	f, _ := db.getFrozenMemInfo()
	return f.mem
}

func (db *DB) getFrozenMemInfo() (f frozenMem, ok bool) {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if len(db.frozenMems) > 0 {
		f = db.frozenMems[0]
		f.mem.incref()
		ok = true
	}
	return
}

func (db *DB) getFrozenMemCount() int {
	db.memMu.RLock()
	n := len(db.frozenMems)
	db.memMu.RUnlock()
	return n
}

func (db *DB) getJournalNum() int64 {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if len(db.frozenMems) > 0 {
		return db.frozenMems[0].journalFd.Num
	}
	return db.journalFd.Num
}

func (db *DB) getNextJournalNum() int64 {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if len(db.frozenMems) > 1 {
		return db.frozenMems[1].journalFd.Num
	}
	return db.journalFd.Num
}

// Drop frozen memdb; assume that frozen memdb isn't nil.
func (db *DB) dropFrozenMem() {
	db.memMu.Lock()
	if len(db.frozenMems) == 0 {
		db.memMu.Unlock()
		panic("BUG: no frozen memdb")
	}
	f := db.frozenMems[0]
	db.frozenMems[0] = frozenMem{}
	db.frozenMems = db.frozenMems[1:]
	db.memMu.Unlock()

	if err := db.s.stor.Remove(f.journalFd); err != nil {
		db.logf("journal@remove removing @%d %q", f.journalFd.Num, err)
	} else {
		db.logf("journal@remove removed @%d", f.journalFd.Num)
	}
	f.mem.decref()
}

// Clear mems ptr; used by DB.Close().
func (db *DB) clearMems() {
	db.memMu.Lock()
	db.mem = nil
	db.frozenMems = nil
	db.memMu.Unlock()
}

// Set closed flag; return true if not already closed.
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.closed, 0, 1)
}

// Check whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

// Check read ok status.
func (db *DB) ok() error {
	if db.isClosed() {
		return ErrClosed
	}
	return nil
}
