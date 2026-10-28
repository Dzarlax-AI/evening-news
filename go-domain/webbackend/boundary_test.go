package webbackend

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type failReader struct{ called bool }

func (f *failReader) Read([]byte) (int, error) {
	f.called = true
	return 0, errors.New("read unexpectedly")
}
func (f *failReader) Close() error { return nil }
func TestBoundaryAuthAndWorkerDenyBeforeBody(t *testing.T) {
	called := false
	b := Boundary{Token: "fixture", Worker: true, Next: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })}
	for _, tc := range []struct {
		token, path string
		want        int
	}{{"wrong", "/internal/web/v1/backups/upload", 401}, {"fixture", "/internal/web/v1/backups/upload", 409}, {"fixture", "/internal/web/v1/backups/restore", 409}, {"fixture", "/internal/web/v1/backups/cleanup", 409}} {
		reader := &failReader{}
		r := httptest.NewRequest("POST", tc.path, nil)
		r.Body = reader
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		b.ServeHTTP(w, r)
		if w.Code != tc.want || reader.called || called {
			t.Fatalf("path=%s status=%d read=%v dispatch=%v", tc.path, w.Code, reader.called, called)
		}
	}
}
func TestBoundaryBoundsActualBodyAndRejectsDuplicates(t *testing.T) {
	calls := 0
	b := Boundary{Token: "fixture", Next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		data, _ := io.ReadAll(r.Body)
		if string(data) != "ok" {
			t.Errorf("body=%s", data)
		}
	})}
	request := func(body string) *http.Request {
		r := httptest.NewRequest("POST", "/internal/web/v1/test", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer fixture")
		return r
	}
	r := request("ok")
	w := httptest.NewRecorder()
	b.ServeHTTP(w, r)
	if w.Code != 200 || calls != 1 {
		t.Fatalf("status=%d calls=%d", w.Code, calls)
	}
	r = request(strings.Repeat("x", 65537))
	r.ContentLength = -1
	w = httptest.NewRecorder()
	b.ServeHTTP(w, r)
	if w.Code != 413 || calls != 1 {
		t.Fatalf("large status=%d", w.Code)
	}
	r = request("ok")
	r.Header["Authorization"] = []string{"Bearer fixture", "Bearer fixture"}
	w = httptest.NewRecorder()
	b.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("duplicate auth status=%d", w.Code)
	}
	r = request("ok")
	r.Header["Content-Length"] = []string{"2", "2"}
	w = httptest.NewRecorder()
	b.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatalf("duplicate length status=%d", w.Code)
	}
}

func TestCategoryAndCleanupRejectNullFields(t *testing.T) {
	s := &Server{Guard: testGuard{}, Worker: true, BackupRoot: t.TempDir()}
	for _, tc := range []struct {
		path, body string
		want       int
	}{
		{"/internal/web/v1/categories/ai-category-analysis", `{"limit":null}`, 422},
		{"/internal/web/v1/categories/ai-category-analysis", `{}`, 409},
		{"/internal/web/v1/backups/cleanup", `{"keep_count":null}`, 422},
	} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body)))
		if w.Code != tc.want {
			t.Fatalf("%s %s: %d", tc.path, tc.body, w.Code)
		}
	}
}
