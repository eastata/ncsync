// Package syncer implements one-way (server -> local) synchronisation.
//
// Strategy: Nextcloud propagates a changed etag from any modified file up to
// every parent directory including the root. So a single PROPFIND Depth:0 on
// the root tells us whether anything changed, and during a walk we only
// descend into directories whose etag differs from the one we stored after
// the last fully successful sync of that directory.
package syncer

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"ncsync/internal/config"
	"ncsync/internal/dav"
	"ncsync/internal/state"
)

// Syncer mirrors the remote tree into cfg.Dest. Run must not be called
// concurrently with itself.
type Syncer struct {
	cfg     *config.Config
	dav     *dav.Client
	st      *state.Store
	log     *slog.Logger
	listSem chan struct{}
	dlSem   chan struct{}
	bufPool sync.Pool
	run     *runStats
}

type runStats struct {
	dirs, files, bytes, deleted, skippedE2EE, errors atomic.Int64
	mu                                               sync.Mutex
	firstErr                                         error
}

// New creates a Syncer.
func New(cfg *config.Config, c *dav.Client, st *state.Store, log *slog.Logger) *Syncer {
	return &Syncer{
		cfg:     cfg,
		dav:     c,
		st:      st,
		log:     log,
		listSem: make(chan struct{}, cfg.Listers),
		dlSem:   make(chan struct{}, cfg.Downloads),
		bufPool: sync.Pool{New: func() any { b := make([]byte, 256<<10); return &b }},
		run:     &runStats{},
	}
}

func (s *Syncer) fail(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	s.log.Warn("error", "err", err)
	s.run.errors.Add(1)
	s.run.mu.Lock()
	if s.run.firstErr == nil {
		s.run.firstErr = err
	}
	s.run.mu.Unlock()
}

// Run performs one sync pass. It is very cheap when nothing changed: a single
// PROPFIND Depth:0 request.
func (s *Syncer) Run(ctx context.Context) error {
	start := time.Now()
	s.run = &runStats{}

	ents, err := s.dav.List(ctx, "", 0)
	if err != nil {
		return fmt.Errorf("check root: %w", err)
	}
	var root *dav.Entry
	for i := range ents {
		if ents[i].Path == "" {
			root = &ents[i]
		}
	}
	if root == nil {
		return errors.New("check root: server returned no entry for the root")
	}
	if old, ok := s.st.Get(""); ok && root.ETag != "" && old.ETag == root.ETag {
		s.log.Debug("no changes (root etag unchanged)")
		return nil
	}
	if err := os.MkdirAll(s.cfg.Dest, 0o755); err != nil {
		return err
	}

	s.log.Info("changes detected, syncing")
	s.syncDir(ctx, "")
	if err := ctx.Err(); err != nil {
		return err
	}
	r := s.run
	s.log.Info("pass finished",
		"duration", time.Since(start).Round(time.Millisecond),
		"dirs_listed", r.dirs.Load(), "files_downloaded", r.files.Load(),
		"mb_downloaded", r.bytes.Load()>>20, "deleted", r.deleted.Load(),
		"e2ee_skipped", r.skippedE2EE.Load(), "errors", r.errors.Load())
	if n := r.errors.Load(); n > 0 {
		return fmt.Errorf("sync incomplete: %d errors, first: %w", n, r.firstErr)
	}
	return nil
}

func parentOf(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return ""
}

// syncDir lists dir, processes changed children in parallel, and records the
// directory's etag ONLY if everything below it succeeded. It returns whether
// the directory was committed. Errors are recorded via s.fail.
func (s *Syncer) syncDir(ctx context.Context, dir string) bool {
	select {
	case s.listSem <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	ents, err := s.dav.List(ctx, dir, 1)
	<-s.listSem // released BEFORE recursing, so nested listings cannot deadlock
	if err != nil {
		s.fail(fmt.Errorf("list %q: %w", dir, err))
		return false
	}
	s.run.dirs.Add(1)

	var self *dav.Entry
	children := make([]dav.Entry, 0, len(ents))
	for i := range ents {
		switch {
		case ents[i].Path == dir:
			self = &ents[i]
		case parentOf(ents[i].Path) == dir:
			children = append(children, ents[i])
		}
	}
	if self == nil {
		s.fail(fmt.Errorf("list %q: server did not return the directory itself", dir))
		return false
	}

	local, err := s.local(dir)
	if err != nil {
		s.fail(err)
		return false
	}
	if err := ensureDir(local); err != nil {
		s.fail(err)
		return false
	}
	cleanupTemps(local)

	seen := make(map[string]struct{}, len(children))
	var failed atomic.Bool
	var wg sync.WaitGroup
	for _, e := range children {
		seen[e.Path] = struct{}{}
		if e.IsDir && e.Encrypted && s.cfg.SkipE2EE {
			s.run.skippedE2EE.Add(1)
			s.log.Info("skipping end-to-end encrypted folder", "path", e.Path)
			continue
		}
		if s.upToDate(e) {
			continue
		}
		wg.Add(1)
		go func(e dav.Entry) {
			defer wg.Done()
			var ok bool
			if e.IsDir {
				ok = s.syncDir(ctx, e.Path)
			} else {
				ok = s.download(ctx, e)
			}
			if !ok {
				failed.Store(true)
			}
		}(e)
	}
	wg.Wait()
	if failed.Load() || ctx.Err() != nil {
		return false // etag not committed -> this directory is retried next pass
	}

	if s.cfg.Delete {
		s.removeStale(dir, seen)
	}
	if err := s.st.Put(*self); err != nil {
		s.fail(err)
		return false
	}
	return true
}

// upToDate reports whether e needs no work. Unchanged directories are skipped
// without touching disk; unchanged files get one cheap stat so that a file
// deleted or modified locally is repaired whenever its folder is re-listed.
func (s *Syncer) upToDate(e dav.Entry) bool {
	old, ok := s.st.Get(e.Path)
	if !ok || e.ETag == "" || old.ETag != e.ETag || old.IsDir != e.IsDir {
		return false
	}
	if e.IsDir {
		return true
	}
	p, err := s.local(e.Path)
	if err != nil {
		return false
	}
	fi, err := os.Lstat(p)
	return err == nil && fi.Mode().IsRegular() && fi.Size() == e.Size && fi.ModTime().Unix() == e.MTime
}

func (s *Syncer) removeStale(dir string, seen map[string]struct{}) {
	for _, c := range s.st.Children(dir) {
		if _, ok := seen[c.Path]; ok {
			continue
		}
		p, err := s.local(c.Path)
		if err == nil {
			if err := os.RemoveAll(p); err != nil {
				s.fail(fmt.Errorf("delete %q: %w", c.Path, err))
				continue
			}
		}
		if err := s.st.DeleteTree(c.Path); err != nil {
			s.fail(err)
			continue
		}
		s.run.deleted.Add(1)
		s.log.Info("deleted (removed on server)", "path", c.Path)
	}
}

func (s *Syncer) download(ctx context.Context, e dav.Entry) bool {
	select {
	case s.dlSem <- struct{}{}:
	case <-ctx.Done():
		return false
	}
	defer func() { <-s.dlSem }()
	if err := s.fetch(ctx, e); err != nil {
		s.fail(fmt.Errorf("download %q: %w", e.Path, err))
		return false
	}
	return true
}

func (s *Syncer) fetch(ctx context.Context, e dav.Entry) error {
	dst, err := s.local(e.Path)
	if err != nil {
		return err
	}
	// a directory is being replaced by a file
	if fi, err := os.Lstat(dst); err == nil && fi.IsDir() {
		if err := os.RemoveAll(dst); err != nil {
			return err
		}
	}
	if old, ok := s.st.Get(e.Path); ok && old.IsDir {
		if err := s.st.DeleteTree(e.Path); err != nil {
			return err
		}
	}

	body, err := s.dav.Get(ctx, e.Path)
	if err != nil {
		return err
	}
	defer body.Close()

	tmp := tempName(dst)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}

	h, want := s.hasher(e)
	w := io.Writer(struct{ io.Writer }{f}) // hide ReadFrom so our pooled buffer is used
	if h != nil {
		w = io.MultiWriter(w, h)
	}
	bp := s.bufPool.Get().(*[]byte)
	n, err := io.CopyBuffer(w, body, *bp)
	s.bufPool.Put(bp)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil && e.Size >= 0 && n != e.Size {
		err = fmt.Errorf("size mismatch: got %d bytes, server said %d", n, e.Size)
	}
	if err == nil && h != nil && hex.EncodeToString(h.Sum(nil)) != want {
		err = errors.New("checksum mismatch")
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if e.MTime > 0 {
		t := time.Unix(e.MTime, 0)
		_ = os.Chtimes(tmp, t, t)
	}
	// no per-file fsync (SD-card friendly): a crash re-downloads at worst
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := s.st.Put(e); err != nil {
		return err
	}
	s.run.files.Add(1)
	s.run.bytes.Add(n)
	s.log.Debug("downloaded", "path", e.Path, "bytes", n)
	return nil
}

// hasher picks the strongest checksum the server reported (oc:checksums is a
// space separated list such as "SHA1:abc MD5:def ADLER32:123").
func (s *Syncer) hasher(e dav.Entry) (hash.Hash, string) {
	if !s.cfg.Verify || e.Checksum == "" {
		return nil, ""
	}
	sums := map[string]string{}
	for _, tok := range strings.Fields(e.Checksum) {
		if a, v, ok := strings.Cut(tok, ":"); ok {
			sums[strings.ToUpper(a)] = strings.ToLower(v)
		}
	}
	switch {
	case sums["SHA256"] != "":
		return sha256.New(), sums["SHA256"]
	case sums["SHA1"] != "":
		return sha1.New(), sums["SHA1"]
	case sums["MD5"] != "":
		return md5.New(), sums["MD5"]
	}
	return nil, ""
}

// local maps a root-relative remote path to a path under cfg.Dest and refuses
// anything that could escape it.
func (s *Syncer) local(rel string) (string, error) {
	if rel == "" {
		return s.cfg.Dest, nil
	}
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" || seg == "." || seg == ".." || strings.ContainsRune(seg, 0) {
			return "", fmt.Errorf("refusing unsafe remote path %q", rel)
		}
	}
	return filepath.Join(s.cfg.Dest, filepath.FromSlash(rel)), nil
}

func ensureDir(p string) error {
	if fi, err := os.Lstat(p); err == nil && !fi.IsDir() {
		if err := os.Remove(p); err != nil { // a file is being replaced by a directory
			return err
		}
	}
	return os.MkdirAll(p, 0o755)
}

// Temp files are named ".<name>.~<8 hex>", a pattern the Nextcloud desktop
// client excludes by default, so both tools can share a directory.
var tempRe = regexp.MustCompile(`^\..+\.~[0-9a-f]{8}$`)

func tempName(dst string) string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	base := filepath.Base(dst)
	if len(base) > 200 {
		base = base[:200]
	}
	return filepath.Join(filepath.Dir(dst), "."+base+".~"+hex.EncodeToString(b[:]))
}

// cleanupTemps removes leftovers of an interrupted run from one directory.
func cleanupTemps(dir string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		if !e.IsDir() && tempRe.MatchString(e.Name()) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}
