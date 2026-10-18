package syncer_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"ncsync/internal/config"
	"ncsync/internal/dav"
	"ncsync/internal/state"
	"ncsync/internal/syncer"
)

// ---- mock Nextcloud WebDAV server -----------------------------------------

type node struct {
	dir   bool
	data  []byte
	mtime int64
	etag  string
	enc   bool
}

type mock struct {
	mu       sync.Mutex
	nodes    map[string]*node
	ctr      int
	failGet  map[string]bool
	propfind atomic.Int64
	gets     atomic.Int64
}

const prefix = "/remote.php/dav/files/alice"

func newMock() *mock {
	m := &mock{nodes: map[string]*node{}, failGet: map[string]bool{}}
	m.nodes[""] = &node{dir: true, etag: "e0"}
	return m
}

func par(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[:i]
	}
	return ""
}

func (m *mock) bump(p string) {
	for {
		m.ctr++
		m.nodes[p].etag = fmt.Sprintf("e%d", m.ctr)
		if p == "" {
			return
		}
		p = par(p)
	}
}

func (m *mock) mkdirs(p string) {
	if p == "" {
		return
	}
	if _, ok := m.nodes[p]; ok {
		return
	}
	m.mkdirs(par(p))
	m.nodes[p] = &node{dir: true, mtime: 1_700_000_000}
	m.bump(p)
}

func (m *mock) put(p, content string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mkdirs(par(p))
	m.nodes[p] = &node{data: []byte(content), mtime: 1_700_000_000 + int64(m.ctr)}
	m.bump(p)
}

func (m *mock) mkEnc(p string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mkdirs(p)
	m.nodes[p].enc = true
}

func (m *mock) remove(p string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k := range m.nodes {
		if k == p || strings.HasPrefix(k, p+"/") {
			delete(m.nodes, k)
		}
	}
	m.bump(par(p))
}

func (m *mock) reset() { m.propfind.Store(0); m.gets.Store(0) }

func esc(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;").Replace(s)
}

func (m *mock) writeNode(w io.Writer, p string, n *node) {
	segs := strings.Split(p, "/")
	for i := range segs {
		segs[i] = url.PathEscape(segs[i])
	}
	href := prefix + "/" + strings.Join(segs, "/")
	if n.dir && p != "" {
		href += "/"
	}
	if p == "" {
		href = prefix + "/"
	}
	fmt.Fprintf(w, `<d:response><d:href>%s</d:href><d:propstat><d:prop>`, esc(href))
	fmt.Fprintf(w, `<d:getetag>"%s"</d:getetag>`, n.etag)
	fmt.Fprintf(w, `<d:getlastmodified>%s</d:getlastmodified>`, time.Unix(n.mtime, 0).UTC().Format(http.TimeFormat))
	if n.dir {
		fmt.Fprint(w, `<d:resourcetype><d:collection/></d:resourcetype>`)
		if n.enc {
			fmt.Fprint(w, `<nc:is-encrypted>1</nc:is-encrypted>`)
		}
	} else {
		sum := sha1.Sum(n.data)
		fmt.Fprint(w, `<d:resourcetype/>`)
		fmt.Fprintf(w, `<d:getcontentlength>%d</d:getcontentlength>`, len(n.data))
		fmt.Fprintf(w, `<oc:checksums><oc:checksum>SHA1:%s ADLER32:00000000</oc:checksum></oc:checksums>`, hex.EncodeToString(sum[:]))
	}
	fmt.Fprint(w, `</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat>`)
	// a second propstat with unknown props, like real servers send
	fmt.Fprint(w, `<d:propstat><d:prop><oc:fileid/></d:prop><d:status>HTTP/1.1 404 Not Found</d:status></d:propstat>`)
	fmt.Fprint(w, `</d:response>`)
}

func (m *mock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	u, pw, ok := r.BasicAuth()
	if !ok || u != "alice" || pw != "secret" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	rel := strings.Trim(strings.TrimPrefix(r.URL.Path, prefix), "/")
	m.mu.Lock()
	defer m.mu.Unlock()
	n, exists := m.nodes[rel]
	switch r.Method {
	case "PROPFIND":
		m.propfind.Add(1)
		if !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusMultiStatus)
		fmt.Fprint(w, `<?xml version="1.0"?><d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns" xmlns:nc="http://nextcloud.org/ns">`)
		m.writeNode(w, rel, n)
		if r.Header.Get("Depth") == "1" && n.dir {
			var names []string
			for k := range m.nodes {
				if k != "" && par(k) == rel {
					names = append(names, k)
				}
			}
			sort.Strings(names)
			for _, k := range names {
				m.writeNode(w, k, m.nodes[k])
			}
		}
		fmt.Fprint(w, `</d:multistatus>`)
	case "GET":
		m.gets.Add(1)
		if !exists || n.dir {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if m.failGet[rel] {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(n.data)))
		_, _ = w.Write(n.data)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// ---- helpers ---------------------------------------------------------------

type env struct {
	t    *testing.T
	m    *mock
	s    *syncer.Syncer
	dest string
}

func setup(t *testing.T, password string) *env {
	t.Helper()
	m := newMock()
	srv := httptest.NewServer(m)
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	cfg := &config.Config{
		URL: srv.URL, User: "alice", Password: password,
		Dest: filepath.Join(dir, "data"), StatePath: filepath.Join(dir, "state", "s.state"),
		Downloads: 4, Listers: 2, IdleTimeout: 5 * time.Second,
		Delete: true, SkipE2EE: true, Verify: true,
	}
	c, err := dav.New(dav.Options{BaseURL: cfg.URL, User: cfg.User, Password: cfg.Password, Conns: 6, IdleTimeout: cfg.IdleTimeout})
	if err != nil {
		t.Fatal(err)
	}
	st, err := state.Open(cfg.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &env{t, m, syncer.New(cfg, c, st, log), cfg.Dest}
}

func (e *env) run() error { return e.s.Run(context.Background()) }

func (e *env) read(rel string) string {
	b, err := os.ReadFile(filepath.Join(e.dest, rel))
	if err != nil {
		return "<missing>"
	}
	return string(b)
}

func (e *env) exists(rel string) bool {
	_, err := os.Lstat(filepath.Join(e.dest, rel))
	return err == nil
}

func (e *env) expect(rel, want string) {
	e.t.Helper()
	if got := e.read(rel); got != want {
		e.t.Fatalf("%s: got %q want %q", rel, got, want)
	}
}

// ---- tests -----------------------------------------------------------------

func TestSyncScenarios(t *testing.T) {
	e := setup(t, "secret")
	m := e.m

	m.put("a.txt", "hello")
	m.put("docs/b.txt", "world")
	m.put("docs/deep/c.txt", "deep")
	m.put("space dir/ünï cödé & file.txt", "unicode")
	m.mkEnc("secret")
	m.put("secret/x", "ciphertext")

	t.Log("1. initial sync")
	if err := e.run(); err != nil {
		t.Fatal(err)
	}
	e.expect("a.txt", "hello")
	e.expect("docs/b.txt", "world")
	e.expect("docs/deep/c.txt", "deep")
	e.expect("space dir/ünï cödé & file.txt", "unicode")
	if e.exists("secret") {
		t.Fatal("E2EE folder must be skipped")
	}
	fi, _ := os.Stat(filepath.Join(e.dest, "a.txt"))
	if fi.ModTime().Unix() != m.nodes["a.txt"].mtime {
		t.Fatalf("mtime not preserved: %d vs %d", fi.ModTime().Unix(), m.nodes["a.txt"].mtime)
	}

	t.Log("2. nothing changed -> exactly one request")
	m.reset()
	if err := e.run(); err != nil {
		t.Fatal(err)
	}
	if m.propfind.Load() != 1 || m.gets.Load() != 0 {
		t.Fatalf("expected 1 PROPFIND / 0 GET, got %d / %d", m.propfind.Load(), m.gets.Load())
	}

	t.Log("3. one deep change -> only the path to it is re-listed, one GET")
	m.put("docs/deep/c.txt", "deep2")
	m.reset()
	if err := e.run(); err != nil {
		t.Fatal(err)
	}
	e.expect("docs/deep/c.txt", "deep2")
	if m.propfind.Load() != 4 || m.gets.Load() != 1 {
		t.Fatalf("expected 4 PROPFIND / 1 GET, got %d / %d", m.propfind.Load(), m.gets.Load())
	}

	t.Log("4. remote delete is mirrored")
	m.remove("docs/b.txt")
	if err := e.run(); err != nil {
		t.Fatal(err)
	}
	if e.exists("docs/b.txt") {
		t.Fatal("docs/b.txt should be deleted")
	}
	m.remove("docs")
	if err := e.run(); err != nil {
		t.Fatal(err)
	}
	if e.exists("docs") {
		t.Fatal("docs/ should be deleted recursively")
	}

	t.Log("5. failing file: error reported, dir not committed, retried and repaired")
	m.put("z/new.txt", "x")
	m.mu.Lock()
	m.failGet["z/new.txt"] = true
	m.mu.Unlock()
	if err := e.run(); err == nil {
		t.Fatal("expected an error")
	}
	m.reset()
	if err := e.run(); err == nil {
		t.Fatal("expected an error again")
	}
	if m.propfind.Load() < 2 {
		t.Fatal("failed directory must be re-listed on the next pass")
	}
	if e.exists("z/new.txt") {
		t.Fatal("partial file must not appear")
	}
	m.mu.Lock()
	delete(m.failGet, "z/new.txt")
	m.mu.Unlock()
	if err := e.run(); err != nil {
		t.Fatal(err)
	}
	e.expect("z/new.txt", "x")

	t.Log("6. locally deleted file is restored when its folder is re-listed")
	_ = os.Remove(filepath.Join(e.dest, "a.txt"))
	m.put("another.txt", "y") // changes the root etag
	if err := e.run(); err != nil {
		t.Fatal(err)
	}
	e.expect("a.txt", "hello")

	t.Log("7. no leftover temp files")
	filepath.WalkDir(e.dest, func(p string, d os.DirEntry, _ error) error {
		if strings.Contains(d.Name(), ".~") {
			t.Fatalf("leftover temp file %s", p)
		}
		return nil
	})
}

func TestBadPassword(t *testing.T) {
	e := setup(t, "wrong")
	err := e.run()
	if err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("expected auth error, got %v", err)
	}
}
