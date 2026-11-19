// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reservefs.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package storage

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	errFileOpen = errors.New("leveldb/storage: file still open")
	errReadOnly = errors.New("leveldb/storage: storage is read-only")
)

type fileLock interface {
	release() error
}

type fileStorageLock struct {
	fs *fileStorage
}

func (lock *fileStorageLock) Unlock() {
	if lock.fs != nil {
		lock.fs.mu.Lock()
		defer lock.fs.mu.Unlock()
		if lock.fs.slock == lock {
			lock.fs.slock = nil
		}
	}
}

type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func writeFileSynced(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Sync(); err == nil {
		err = err1
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

const logSizeThreshold = 1024 * 1024 // 1 MiB

// fileStorage is a file-system backed storage.
type fileStorage struct {
	path     string
	readOnly bool

	mu      sync.Mutex
	flock   fileLock
	slock   *fileStorageLock
	logw    *os.File
	logSize int64
	buf     []byte
	// Opened file counter; if open < 0 means closed.
	open int
	day  int

	// handles tracks the open handles of each file, so that a file that
	// is still referenced (e.g. by an iterator) will not be removed right
	// away; the removal is deferred until the last handle is closed.
	// The reference count is atomic, hence neither acquiring/releasing a
	// reference nor the reference check on the removal path requires a
	// lock.
	handles sync.Map // map[FileDesc]*fileHandleRef
}

// fileHandleRef keeps track of the open handles of a single file along
// with a pending-removal mark. All fields are manipulated atomically.
type fileHandleRef struct {
	refs    int32 // number of open handles
	removed int32 // non-zero if removal was requested while still referenced
}

// acquireHandle acquires a new handle reference for the given file. The
// reference must be released with releaseHandle once the handle is closed.
func (fs *fileStorage) acquireHandle(fd FileDesc) *fileHandleRef {
	v, _ := fs.handles.LoadOrStore(fd, &fileHandleRef{})
	h := v.(*fileHandleRef)
	atomic.AddInt32(&h.refs, 1)
	return h
}

// releaseHandle releases a handle reference previously acquired with
// acquireHandle. If the file was marked for removal while still referenced,
// the actual removal happens now that the last handle is gone.
func (fs *fileStorage) releaseHandle(fd FileDesc, h *fileHandleRef) {
	if atomic.AddInt32(&h.refs, -1) > 0 {
		return
	}
	if atomic.LoadInt32(&h.removed) == 0 {
		return
	}
	// Deferred reclamation of a file whose removal was requested while
	// it was still referenced.
	if err := fs.removeFile(fd); err != nil && !os.IsNotExist(err) {
		fs.Log(fmt.Sprintf("remove %s: %v (deferred)", fd, err))
	}
	// Drop the registry entry if it still refers to this record.
	if v, ok := fs.handles.Load(fd); ok && v == h {
		fs.handles.Delete(fd)
	}
}

// removeFile removes the given file from the file system, falling back to
// the old file name for table files.
func (fs *fileStorage) removeFile(fd FileDesc) error {
	err := os.Remove(filepath.Join(fs.path, fsGenName(fd)))
	if err != nil {
		if fsHasOldName(fd) && os.IsNotExist(err) {
			if e1 := os.Remove(filepath.Join(fs.path, fsGenOldName(fd))); !os.IsNotExist(e1) {
				fs.Log(fmt.Sprintf("remove %s: %v (old name)", fd, err))
				err = e1
			}
		} else {
			fs.Log(fmt.Sprintf("remove %s: %v", fd, err))
		}
	}
	return err
}

// OpenFile returns a new filesystem-backed storage implementation with the given
// path. This also acquire a file lock, so any subsequent attempt to open the
// same path will fail.
//
// The storage must be closed after use, by calling Close method.
func OpenFile(path string, readOnly bool) (Storage, error) {
	if fi, err := os.Stat(path); err == nil {
		if !fi.IsDir() {
			return nil, fmt.Errorf("leveldb/storage: open %s: not a directory", path)
		}
	} else if os.IsNotExist(err) && !readOnly {
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	flock, err := newFileLock(filepath.Join(path, "LOCK"), readOnly)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			if ferr := flock.release(); ferr != nil {
				err = fmt.Errorf("error opening file (%v); error unlocking file (%v)", err, ferr)
			}
		}
	}()

	var (
		logw    *os.File
		logSize int64
	)
	if !readOnly {
		logw, err = os.OpenFile(filepath.Join(path, "LOG"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		logSize, err = logw.Seek(0, os.SEEK_END)
		if err != nil {
			logw.Close()
			return nil, err
		}
	}

	fs := &fileStorage{
		path:     path,
		readOnly: readOnly,
		flock:    flock,
		logw:     logw,
		logSize:  logSize,
	}
	runtime.SetFinalizer(fs, (*fileStorage).Close)
	return fs, nil
}

func (fs *fileStorage) Lock() (Locker, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	if fs.readOnly {
		return &fileStorageLock{}, nil
	}
	if fs.slock != nil {
		return nil, ErrLocked
	}
	fs.slock = &fileStorageLock{fs: fs}
	return fs.slock, nil
}

func itoa(buf []byte, i int, wid int) []byte {
	u := uint(i)
	if u == 0 && wid <= 1 {
		return append(buf, '0')
	}

	// Assemble decimal in reverse order.
	var b [32]byte
	bp := len(b)
	for ; u > 0 || wid > 0; u /= 10 {
		bp--
		wid--
		b[bp] = byte(u%10) + '0'
	}
	return append(buf, b[bp:]...)
}

func (fs *fileStorage) printDay(t time.Time) error {
	if fs.day == t.Day() {
		return nil
	}
	fs.day = t.Day()
	_, err := fs.logw.Write([]byte("=============== " + t.Format("Jan 2, 2006 (MST)") + " ===============\n"))
	return err
}

func (fs *fileStorage) doLog(t time.Time, str string) {
	if fs.logSize > logSizeThreshold {
		// Rotate log file.
		fs.logw.Close()
		fs.logw = nil
		fs.logSize = 0
		if err := rename(filepath.Join(fs.path, "LOG"), filepath.Join(fs.path, "LOG.old")); err != nil {
			return
		}
	}
	if fs.logw == nil {
		var err error
		fs.logw, err = os.OpenFile(filepath.Join(fs.path, "LOG"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return
		}
		// Force printDay on new log file.
		fs.day = 0
	}
	if err := fs.printDay(t); err != nil {
		return
	}
	hour, min, sec := t.Clock()
	msec := t.Nanosecond() / 1e3
	// time
	fs.buf = itoa(fs.buf[:0], hour, 2)
	fs.buf = append(fs.buf, ':')
	fs.buf = itoa(fs.buf, min, 2)
	fs.buf = append(fs.buf, ':')
	fs.buf = itoa(fs.buf, sec, 2)
	fs.buf = append(fs.buf, '.')
	fs.buf = itoa(fs.buf, msec, 6)
	fs.buf = append(fs.buf, ' ')
	// write
	fs.buf = append(fs.buf, []byte(str)...)
	fs.buf = append(fs.buf, '\n')
	n, _ := fs.logw.Write(fs.buf)
	fs.logSize += int64(n)
}

func (fs *fileStorage) Log(str string) {
	if !fs.readOnly {
		t := time.Now()
		fs.mu.Lock()
		defer fs.mu.Unlock()
		if fs.open < 0 {
			return
		}
		fs.doLog(t, str)
	}
}

func (fs *fileStorage) log(str string) {
	if !fs.readOnly {
		fs.doLog(time.Now(), str)
	}
}

func (fs *fileStorage) setMeta(fd FileDesc) error {
	content := fsGenName(fd) + "\n"
	// Check and backup old CURRENT file.
	currentPath := filepath.Join(fs.path, "CURRENT")
	if _, err := os.Stat(currentPath); err == nil {
		b, err := ioutil.ReadFile(currentPath)
		if err != nil {
			fs.log(fmt.Sprintf("backup CURRENT: %v", err))
			return err
		}
		if string(b) == content {
			// Content not changed, do nothing.
			return nil
		}
		if err := writeFileSynced(currentPath+".bak", b, 0644); err != nil {
			fs.log(fmt.Sprintf("backup CURRENT: %v", err))
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	path := fmt.Sprintf("%s.%d", filepath.Join(fs.path, "CURRENT"), fd.Num)
	if err := writeFileSynced(path, []byte(content), 0644); err != nil {
		fs.log(fmt.Sprintf("create CURRENT.%d: %v", fd.Num, err))
		return err
	}
	// Replace CURRENT file.
	if err := rename(path, currentPath); err != nil {
		fs.log(fmt.Sprintf("rename CURRENT.%d: %v", fd.Num, err))
		return err
	}
	// Sync root directory.
	if err := syncDir(fs.path); err != nil {
		fs.log(fmt.Sprintf("syncDir: %v", err))
		return err
	}
	return nil
}

func (fs *fileStorage) SetMeta(fd FileDesc) error {
	if !FileDescOk(fd) {
		return ErrInvalidFile
	}
	if fs.readOnly {
		return errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	return fs.setMeta(fd)
}

func (fs *fileStorage) GetMeta() (FileDesc, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return FileDesc{}, ErrClosed
	}
	dir, err := os.Open(fs.path)
	if err != nil {
		return FileDesc{}, err
	}
	names, err := dir.Readdirnames(0)
	// Close the dir first before checking for Readdirnames error.
	if ce := dir.Close(); ce != nil {
		fs.log(fmt.Sprintf("close dir: %v", ce))
	}
	if err != nil {
		return FileDesc{}, err
	}
	// Try this in order:
	// - CURRENT.[0-9]+ ('pending rename' file, descending order)
	// - CURRENT
	// - CURRENT.bak
	//
	// Skip corrupted file or file that point to a missing target file.
	type currentFile struct {
		name string
		fd   FileDesc
	}
	tryCurrent := func(name string) (*currentFile, error) {
		b, err := ioutil.ReadFile(filepath.Join(fs.path, name))
		if err != nil {
			if os.IsNotExist(err) {
				err = os.ErrNotExist
			}
			return nil, err
		}
		var fd FileDesc
		if len(b) < 1 || b[len(b)-1] != '\n' || !fsParseNamePtr(string(b[:len(b)-1]), &fd) {
			fs.log(fmt.Sprintf("%s: corrupted content: %q", name, b))
			err := &ErrCorrupted{
				Err: errors.New("leveldb/storage: corrupted or incomplete CURRENT file"),
			}
			return nil, err
		}
		if _, err := os.Stat(filepath.Join(fs.path, fsGenName(fd))); err != nil {
			if os.IsNotExist(err) {
				fs.log(fmt.Sprintf("%s: missing target file: %s", name, fd))
				err = os.ErrNotExist
			}
			return nil, err
		}
		return &currentFile{name: name, fd: fd}, nil
	}
	tryCurrents := func(names []string) (*currentFile, error) {
		var (
			cur *currentFile
			// Last corruption error.
			lastCerr error
		)
		for _, name := range names {
			var err error
			cur, err = tryCurrent(name)
			if err == nil {
				break
			} else if err == os.ErrNotExist {
				// Fallback to the next file.
			} else if isCorrupted(err) {
				lastCerr = err
				// Fallback to the next file.
			} else {
				// In case the error is due to permission, etc.
				return nil, err
			}
		}
		if cur == nil {
			err := os.ErrNotExist
			if lastCerr != nil {
				err = lastCerr
			}
			return nil, err
		}
		return cur, nil
	}

	// Try 'pending rename' files.
	var nums []int64
	for _, name := range names {
		if strings.HasPrefix(name, "CURRENT.") && name != "CURRENT.bak" {
			i, err := strconv.ParseInt(name[8:], 10, 64)
			if err == nil {
				nums = append(nums, i)
			}
		}
	}
	var (
		pendCur   *currentFile
		pendErr   = os.ErrNotExist
		pendNames []string
	)
	if len(nums) > 0 {
		sort.Sort(sort.Reverse(int64Slice(nums)))
		pendNames = make([]string, len(nums))
		for i, num := range nums {
			pendNames[i] = fmt.Sprintf("CURRENT.%d", num)
		}
		pendCur, pendErr = tryCurrents(pendNames)
		if pendErr != nil && pendErr != os.ErrNotExist && !isCorrupted(pendErr) {
			return FileDesc{}, pendErr
		}
	}

	// Try CURRENT and CURRENT.bak.
	curCur, curErr := tryCurrents([]string{"CURRENT", "CURRENT.bak"})
	if curErr != nil && curErr != os.ErrNotExist && !isCorrupted(curErr) {
		return FileDesc{}, curErr
	}

	// pendCur takes precedence, but guards against obsolete pendCur.
	if pendCur != nil && (curCur == nil || pendCur.fd.Num > curCur.fd.Num) {
		curCur = pendCur
	}

	if curCur != nil {
		// Restore CURRENT file to proper state.
		if !fs.readOnly && (curCur.name != "CURRENT" || len(pendNames) != 0) {
			// Ignore setMeta errors, however don't delete obsolete files if we
			// catch error.
			if err := fs.setMeta(curCur.fd); err == nil {
				// Remove 'pending rename' files.
				for _, name := range pendNames {
					if err := os.Remove(filepath.Join(fs.path, name)); err != nil {
						fs.log(fmt.Sprintf("remove %s: %v", name, err))
					}
				}
			}
		}
		return curCur.fd, nil
	}

	// Nothing found.
	if isCorrupted(pendErr) {
		return FileDesc{}, pendErr
	}
	return FileDesc{}, curErr
}

func (fs *fileStorage) List(ft FileType) (fds []FileDesc, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	dir, err := os.Open(fs.path)
	if err != nil {
		return
	}
	names, err := dir.Readdirnames(0)
	// Close the dir first before checking for Readdirnames error.
	if cerr := dir.Close(); cerr != nil {
		fs.log(fmt.Sprintf("close dir: %v", cerr))
	}
	if err == nil {
		for _, name := range names {
			if fd, ok := fsParseName(name); ok && fd.Type&ft != 0 {
				fds = append(fds, fd)
			}
		}
	}
	return
}

func (fs *fileStorage) Open(fd FileDesc) (Reader, error) {
	if !FileDescOk(fd) {
		return nil, ErrInvalidFile
	}

	h := fs.acquireHandle(fd)
	if atomic.LoadInt32(&h.removed) != 0 {
		// The file has been removed; it is only kept around until the
		// last referenced handle is closed.
		fs.releaseHandle(fd, h)
		return nil, os.ErrNotExist
	}

	fs.mu.Lock()
	if fs.open < 0 {
		fs.mu.Unlock()
		fs.releaseHandle(fd, h)
		return nil, ErrClosed
	}
	of, err := os.OpenFile(filepath.Join(fs.path, fsGenName(fd)), os.O_RDONLY, 0)
	if err != nil {
		if fsHasOldName(fd) && os.IsNotExist(err) {
			of, err = os.OpenFile(filepath.Join(fs.path, fsGenOldName(fd)), os.O_RDONLY, 0)
		}
	}
	if err != nil {
		fs.mu.Unlock()
		fs.releaseHandle(fd, h)
		return nil, err
	}
	fs.open++
	fs.mu.Unlock()
	return &fileWrap{File: of, fs: fs, fd: fd, h: h}, nil
}

func (fs *fileStorage) Create(fd FileDesc) (Writer, error) {
	if !FileDescOk(fd) {
		return nil, ErrInvalidFile
	}
	if fs.readOnly {
		return nil, errReadOnly
	}

	h := fs.acquireHandle(fd)
	// The file is being (re)created; cancel any pending removal.
	atomic.StoreInt32(&h.removed, 0)

	fs.mu.Lock()
	if fs.open < 0 {
		fs.mu.Unlock()
		fs.releaseHandle(fd, h)
		return nil, ErrClosed
	}
	of, err := os.OpenFile(filepath.Join(fs.path, fsGenName(fd)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		fs.mu.Unlock()
		fs.releaseHandle(fd, h)
		return nil, err
	}
	fs.open++
	fs.mu.Unlock()
	return &fileWrap{File: of, fs: fs, fd: fd, h: h}, nil
}

func (fs *fileStorage) Remove(fd FileDesc) error {
	if !FileDescOk(fd) {
		return ErrInvalidFile
	}
	if fs.readOnly {
		return errReadOnly
	}

	fs.mu.Lock()
	if fs.open < 0 {
		fs.mu.Unlock()
		return ErrClosed
	}
	fs.mu.Unlock()

	// Lock-free reference check: a file that is still referenced by open
	// handles cannot be removed right away; mark it and defer the actual
	// removal until the last handle is closed.
	if v, ok := fs.handles.Load(fd); ok {
		if h := v.(*fileHandleRef); atomic.LoadInt32(&h.refs) > 0 {
			atomic.StoreInt32(&h.removed, 1)
			fs.Log(fmt.Sprintf("remove %s: deferred, file still referenced", fd))
			return nil
		}
	}

	err := fs.removeFile(fd)
	if err == nil || os.IsNotExist(err) {
		fs.handles.Delete(fd)
	}
	return err
}

func (fs *fileStorage) Rename(oldfd, newfd FileDesc) error {
	if !FileDescOk(oldfd) || !FileDescOk(newfd) {
		return ErrInvalidFile
	}
	if oldfd == newfd {
		return nil
	}
	if fs.readOnly {
		return errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	return rename(filepath.Join(fs.path, fsGenName(oldfd)), filepath.Join(fs.path, fsGenName(newfd)))
}

func (fs *fileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	// Clear the finalizer.
	runtime.SetFinalizer(fs, nil)

	if fs.open > 0 {
		fs.log(fmt.Sprintf("close: warning, %d files still open", fs.open))
	}
	fs.open = -1
	if fs.logw != nil {
		fs.logw.Close()
	}
	return fs.flock.release()
}

type fileWrap struct {
	*os.File
	fs     *fileStorage
	fd     FileDesc
	h      *fileHandleRef
	closed bool
}

func (fw *fileWrap) Sync() error {
	if err := fw.File.Sync(); err != nil {
		return err
	}
	if fw.fd.Type == TypeManifest {
		// Also sync parent directory if file type is manifest.
		// See: https://code.google.com/p/leveldb/issues/detail?id=190.
		if err := syncDir(fw.fs.path); err != nil {
			fw.fs.log(fmt.Sprintf("syncDir: %v", err))
			return err
		}
	}
	return nil
}

func (fw *fileWrap) Close() error {
	fw.fs.mu.Lock()
	if fw.closed {
		fw.fs.mu.Unlock()
		return ErrClosed
	}
	fw.closed = true
	fw.fs.open--
	err := fw.File.Close()
	if err != nil {
		fw.fs.log(fmt.Sprintf("close %s: %v", fw.fd, err))
	}
	fw.fs.mu.Unlock()
	// Release the handle reference; this triggers the actual removal if
	// the file was marked for removal while still referenced.
	fw.fs.releaseHandle(fw.fd, fw.h)
	return err
}

func fsGenName(fd FileDesc) string {
	switch fd.Type {
	case TypeManifest:
		return fmt.Sprintf("MANIFEST-%06d", fd.Num)
	case TypeJournal:
		return fmt.Sprintf("%06d.log", fd.Num)
	case TypeTable:
		return fmt.Sprintf("%06d.ldb", fd.Num)
	case TypeTemp:
		return fmt.Sprintf("%06d.tmp", fd.Num)
	default:
		panic("invalid file type")
	}
}

func fsHasOldName(fd FileDesc) bool {
	return fd.Type == TypeTable
}

func fsGenOldName(fd FileDesc) string {
	switch fd.Type {
	case TypeTable:
		return fmt.Sprintf("%06d.sst", fd.Num)
	default:
		return fsGenName(fd)
	}
}

func fsParseName(name string) (fd FileDesc, ok bool) {
	var tail string
	_, err := fmt.Sscanf(name, "%d.%s", &fd.Num, &tail)
	if err == nil {
		switch tail {
		case "log":
			fd.Type = TypeJournal
		case "ldb", "sst":
			fd.Type = TypeTable
		case "tmp":
			fd.Type = TypeTemp
		default:
			return
		}
		return fd, true
	}
	n, _ := fmt.Sscanf(name, "MANIFEST-%d%s", &fd.Num, &tail)
	if n == 1 {
		fd.Type = TypeManifest
		return fd, true
	}
	return
}

func fsParseNamePtr(name string, fd *FileDesc) bool {
	_fd, ok := fsParseName(name)
	if fd != nil {
		*fd = _fd
	}
	return ok
}
