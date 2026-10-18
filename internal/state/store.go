// Package state keeps the record of what has been synced.
//
// It is an in-memory index backed by an append-only JSON-lines log. Writes are
// buffered and fsynced every couple of seconds, which is gentle on SD cards.
// A crash can only lose the tail of the log; because a directory record is
// written after all of its children, any log prefix is a consistent state and
// the worst case is that some files are downloaded again.
package state

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"ncsync/internal/dav"
)

type line struct {
	R *dav.Entry `json:"r,omitempty"` // put
	X *string    `json:"x,omitempty"` // delete
}

// Store is safe for concurrent use.
type Store struct {
	mu    sync.Mutex
	recs  map[string]dav.Entry
	kids  map[string]map[string]struct{} // parent dir -> child paths
	lines int                            // lines in the log file

	f     *os.File
	bw    *bufio.Writer
	dirty bool

	lockF *os.File
	stop  chan struct{}
	done  chan struct{}
}

func parent(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return ""
}

// Open loads (or creates) the store at path and takes an exclusive lock so two
// processes can never write the same state.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	lf, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := lockFile(lf); err != nil {
		lf.Close()
		return nil, fmt.Errorf("state is in use by another ncsync process: %w", err)
	}
	s := &Store{
		recs:  make(map[string]dav.Entry),
		kids:  make(map[string]map[string]struct{}),
		lockF: lf,
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	needCompact, err := s.load(path)
	if err != nil {
		lf.Close()
		return nil, err
	}
	if needCompact || s.lines > 2*len(s.recs)+1000 {
		if err := s.compact(path); err != nil {
			lf.Close()
			return nil, err
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		lf.Close()
		return nil, err
	}
	s.f = f
	s.bw = bufio.NewWriterSize(f, 64<<10)
	go s.flusher()
	return s, nil
}

// load replays the log. It reports whether the file needs rewriting (torn or
// corrupt tail, missing final newline).
func (s *Store) load(path string) (needCompact bool, err error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		var l line
		if json.Unmarshal(sc.Bytes(), &l) != nil {
			needCompact = true
			continue
		}
		s.lines++
		switch {
		case l.R != nil:
			s.apply(*l.R)
		case l.X != nil:
			s.remove(*l.X)
		}
	}
	if err := sc.Err(); err != nil {
		return false, err
	}
	if st, err := f.Stat(); err == nil && st.Size() > 0 {
		var b [1]byte
		if _, err := f.ReadAt(b[:], st.Size()-1); err == nil && b[0] != '\n' {
			needCompact = true
		}
	}
	return needCompact, nil
}

// compact rewrites the log with one line per live record (atomic rename).
func (s *Store) compact(path string) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 64<<10)
	enc := json.NewEncoder(bw)
	for _, e := range s.recs {
		e := e
		if err := enc.Encode(line{R: &e}); err != nil {
			f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	s.lines = len(s.recs)
	return nil
}

// apply and remove update the in-memory index only; callers hold s.mu
// (or are in single-threaded load).
func (s *Store) apply(e dav.Entry) {
	s.recs[e.Path] = e
	if e.Path == "" {
		return
	}
	p := parent(e.Path)
	set := s.kids[p]
	if set == nil {
		set = make(map[string]struct{})
		s.kids[p] = set
	}
	set[e.Path] = struct{}{}
}

func (s *Store) remove(p string) {
	delete(s.recs, p)
	if p == "" {
		return
	}
	par := parent(p)
	if set := s.kids[par]; set != nil {
		delete(set, p)
		if len(set) == 0 {
			delete(s.kids, par)
		}
	}
}

func (s *Store) flusher() {
	defer close(s.done)
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.flush()
		}
	}
}

func (s *Store) flush() {
	s.mu.Lock()
	was := s.dirty
	if was {
		_ = s.bw.Flush()
		s.dirty = false
	}
	s.mu.Unlock()
	if was {
		_ = s.f.Sync() // outside the lock: don't block writers during fsync
	}
}

// Get returns the record for a path.
func (s *Store) Get(p string) (dav.Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.recs[p]
	return e, ok
}

// Put stores a record.
func (s *Store) Put(e dav.Entry) error {
	b, err := json.Marshal(line{R: &e})
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.bw.Write(b); err != nil {
		return err
	}
	if err := s.bw.WriteByte('\n'); err != nil {
		return err
	}
	s.apply(e)
	s.dirty = true
	s.lines++
	return nil
}

// DeleteTree removes a record and every record below it.
func (s *Store) DeleteTree(p string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	stack := []string{p}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for k := range s.kids[cur] {
			stack = append(stack, k)
		}
		b, err := json.Marshal(line{X: &cur})
		if err != nil {
			return err
		}
		if _, err := s.bw.Write(b); err != nil {
			return err
		}
		if err := s.bw.WriteByte('\n'); err != nil {
			return err
		}
		s.remove(cur)
		s.dirty = true
		s.lines++
	}
	return nil
}

// Children returns the recorded direct children of dir.
func (s *Store) Children(dir string) []dav.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]dav.Entry, 0, len(s.kids[dir]))
	for k := range s.kids[dir] {
		out = append(out, s.recs[k])
	}
	return out
}

// Len returns the number of records.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.recs)
}

// Close flushes, fsyncs and releases the lock.
func (s *Store) Close() error {
	close(s.stop)
	<-s.done
	s.mu.Lock()
	err := s.bw.Flush()
	s.mu.Unlock()
	if e := s.f.Sync(); err == nil {
		err = e
	}
	if e := s.f.Close(); err == nil {
		err = e
	}
	s.lockF.Close() // releases the flock
	return err
}
