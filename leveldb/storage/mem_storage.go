// Copyright (c) 2013, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package storage

import (
	"bytes"
	"os"
	"sync"
)

const typeShift = 4

// Verify at compile-time that typeShift is large enough to cover all FileType
// values by confirming that 0 == 0.
var _ [0]struct{} = [TypeAll >> typeShift]struct{}{}

type memStorageLock struct {
	ms *memStorage
}

func (lock *memStorageLock) Unlock() {
	ms := lock.ms
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock == lock {
		ms.slock = nil
	}
}

// memStorage is a memory-backed storage.
type memStorage struct {
	mu    sync.Mutex
	slock *memStorageLock
	files map[uint64]*memFile
	meta  FileDesc
}

// NewMemStorage returns a new memory-backed storage implementation.
func NewMemStorage() Storage {
	return &memStorage{
		files: make(map[uint64]*memFile),
	}
}

func (ms *memStorage) Lock() (Locker, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.slock != nil {
		return nil, ErrLocked
	}
	ms.slock = &memStorageLock{ms: ms}
	return ms.slock, nil
}

func (*memStorage) Log(str string) {}

func (ms *memStorage) SetMeta(fd FileDesc) error {
	if !FileDescOk(fd) {
		return ErrInvalidFile
	}

	ms.mu.Lock()
	ms.meta = fd
	ms.mu.Unlock()
	return nil
}

func (ms *memStorage) GetMeta() (FileDesc, error) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if ms.meta.Zero() {
		return FileDesc{}, os.ErrNotExist
	}
	return ms.meta, nil
}

func (ms *memStorage) List(ft FileType) ([]FileDesc, error) {
	ms.mu.Lock()
	var fds []FileDesc
	for x := range ms.files {
		if ms.files[x].removed {
			// The file is removed; it is only kept around until the
			// last referenced handle is closed.
			continue
		}
		fd := unpackFile(x)
		if fd.Type&ft != 0 {
			fds = append(fds, fd)
		}
	}
	ms.mu.Unlock()
	return fds, nil
}

func (ms *memStorage) Open(fd FileDesc) (Reader, error) {
	if !FileDescOk(fd) {
		return nil, ErrInvalidFile
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()
	x := packFile(fd)
	if m, exist := ms.files[x]; exist {
		if m.removed {
			// The file has been removed; it is only kept around until
			// the last referenced handle is closed.
			return nil, os.ErrNotExist
		}
		if m.open {
			return nil, errFileOpen
		}
		m.open = true
		m.refs++
		return &memReader{Reader: bytes.NewReader(m.Bytes()), ms: ms, m: m, x: x}, nil
	}
	return nil, os.ErrNotExist
}

func (ms *memStorage) Create(fd FileDesc) (Writer, error) {
	if !FileDescOk(fd) {
		return nil, ErrInvalidFile
	}

	x := packFile(fd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	m, exist := ms.files[x]
	if exist {
		if m.open {
			return nil, errFileOpen
		}
		m.Reset()
		// The file is being (re)created; cancel any pending removal.
		m.removed = false
	} else {
		m = &memFile{}
		ms.files[x] = m
	}
	m.open = true
	m.refs++
	return &memWriter{memFile: m, ms: ms, x: x}, nil
}

func (ms *memStorage) Remove(fd FileDesc) error {
	if !FileDescOk(fd) {
		return ErrInvalidFile
	}

	x := packFile(fd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if m, exist := ms.files[x]; exist && !m.removed {
		if m.refs > 0 {
			// The file is still referenced by open handles; defer the
			// actual removal until the last handle is closed.
			m.removed = true
			return nil
		}
		delete(ms.files, x)
		return nil
	}
	return os.ErrNotExist
}

func (ms *memStorage) Rename(oldfd, newfd FileDesc) error {
	if !FileDescOk(oldfd) || !FileDescOk(newfd) {
		return ErrInvalidFile
	}
	if oldfd == newfd {
		return nil
	}

	oldx := packFile(oldfd)
	newx := packFile(newfd)
	ms.mu.Lock()
	defer ms.mu.Unlock()
	oldm, exist := ms.files[oldx]
	if !exist || oldm.removed {
		return os.ErrNotExist
	}
	newm, exist := ms.files[newx]
	if (exist && !newm.removed && newm.open) || oldm.open {
		return errFileOpen
	}
	delete(ms.files, oldx)
	ms.files[newx] = oldm
	return nil
}

func (*memStorage) Close() error { return nil }

type memFile struct {
	bytes.Buffer
	open    bool
	refs    int  // number of open handles
	removed bool // removal pending; reclaimed once refs drops to zero
}

type memReader struct {
	*bytes.Reader
	ms     *memStorage
	m      *memFile
	x      uint64
	closed bool
}

func (mr *memReader) Close() error {
	mr.ms.mu.Lock()
	defer mr.ms.mu.Unlock()
	if mr.closed {
		return ErrClosed
	}
	mr.closed = true
	mr.m.open = false
	mr.m.refs--
	if mr.m.refs == 0 && mr.m.removed {
		// Deferred reclamation of a file whose removal was requested
		// while it was still referenced.
		delete(mr.ms.files, mr.x)
	}
	return nil
}

type memWriter struct {
	*memFile
	ms     *memStorage
	x      uint64
	closed bool
}

func (*memWriter) Sync() error { return nil }

func (mw *memWriter) Close() error {
	mw.ms.mu.Lock()
	defer mw.ms.mu.Unlock()
	if mw.closed {
		return ErrClosed
	}
	mw.closed = true
	mw.memFile.open = false
	mw.memFile.refs--
	if mw.memFile.refs == 0 && mw.memFile.removed {
		// Deferred reclamation of a file whose removal was requested
		// while it was still referenced.
		delete(mw.ms.files, mw.x)
	}
	return nil
}

func packFile(fd FileDesc) uint64 {
	return uint64(fd.Num)<<typeShift | uint64(fd.Type)
}

func unpackFile(x uint64) FileDesc {
	return FileDesc{FileType(x) & TypeAll, int64(x >> typeShift)}
}
