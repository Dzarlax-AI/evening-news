package cloakfixture

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOriginBarrierAndIsolation(t *testing.T) {
	dir := t.TempDir()
	o, err := NewOrigin(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(dir, "release"), 0700); err != nil {
		t.Fatal(err)
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		o.ServeHTTP(w, httptest.NewRequest("GET", "https://fixture/hold/case/job", nil))
		done <- w
	}()
	deadline := time.After(2 * time.Second)
	for {
		o.mu.Lock()
		n := o.state.Active
		o.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("arrival timeout")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err = os.Symlink(filepath.Join(dir, "origin-state.json"), filepath.Join(dir, "release", "case")); err != nil {
		t.Fatal(err)
	}
	if o.released("case") {
		t.Fatal("symlink released barrier")
	}
	if err = os.Remove(filepath.Join(dir, "release", "case")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "release", "case"), []byte("release\n"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case w := <-done:
		if w.Code != 200 || !strings.Contains(w.Body.String(), fixtureBody) {
			t.Fatal("invalid article")
		}
	case <-deadline:
		t.Fatal("release timeout")
	}
	b, err := os.ReadFile(filepath.Join(dir, "origin-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var s snapshot
	if json.Unmarshal(b, &s) != nil || s.Active != 0 || s.Completed != 1 || s.MaxActive != 1 {
		t.Fatalf("bad state: %+v", s)
	}
}
func TestOriginRejectsInvalidAndReportsErrors(t *testing.T) {
	o, e := NewOrigin(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		path   string
		status int
	}{{"/hold/bad_/x", 404}, {"/healthz", 200}, {"/error/error", 503}} {
		w := httptest.NewRecorder()
		o.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("%s: %d", tc.path, w.Code)
		}
	}
	if o.state.Errors != 1 || o.state.Active != 0 {
		t.Fatal("error accounting")
	}
}
