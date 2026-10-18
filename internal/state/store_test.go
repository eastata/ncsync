package state

import (
	"os"
	"path/filepath"
	"testing"

	"ncsync/internal/dav"
)

func TestPersistenceAndTornTail(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.state")
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []dav.Entry{
		{Path: "", IsDir: true, ETag: "r"},
		{Path: "a", IsDir: true, ETag: "1"},
		{Path: "a/f", ETag: "2", Size: 3},
		{Path: "a/g", ETag: "3"},
		{Path: "b", ETag: "4"},
	} {
		if err := s.Put(e); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(s.Children("a")); got != 2 {
		t.Fatalf("children(a)=%d", got)
	}
	if err := s.DeleteTree("a"); err != nil {
		t.Fatal(err)
	}
	if s.Len() != 2 {
		t.Fatalf("len=%d want 2", s.Len())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// second process cannot open while locked
	s2, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	// simulate a torn write: valid-looking JSON without trailing newline
	s2.Close()
	f, _ := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"r":{"p":"torn","e":"x","s":0}}`)
	f.Close()

	s3, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s3.Get("torn"); !ok {
		t.Fatal("complete JSON tail should still be loaded")
	}
	if err := s3.Put(dav.Entry{Path: "after", ETag: "y", Size: 1}); err != nil {
		t.Fatal(err)
	}
	s3.Close()

	s4, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s4.Close()
	for _, k := range []string{"", "b", "torn", "after"} {
		if _, ok := s4.Get(k); !ok {
			t.Fatalf("missing %q after reopen", k)
		}
	}
	if _, ok := s4.Get("a/f"); ok {
		t.Fatal("deleted record resurrected")
	}
}

func TestLock(t *testing.T) {
	p := filepath.Join(t.TempDir(), "s.state")
	s, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := Open(p); err == nil {
		t.Fatal("second Open must fail while the first holds the lock")
	}
}
