package webbackend

import (
	"bytes"
	"context"
	"mime/multipart"

	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testGuard struct{}

func (testGuard) Check(context.Context) error { return nil }
func TestCleanupAndRestorePreserveSymlinks(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	for i, name := range []string{"old.tar.gz", "new.tar.gz"} {
		p := filepath.Join(root, name)
		if err := os.WriteFile(p, []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, now.Add(time.Duration(i)*time.Minute), now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(filepath.Join(root, "new.tar.gz"), filepath.Join(root, "link.tar.gz")); err != nil {
		t.Fatal(err)
	}
	s := &Server{Guard: testGuard{}, BackupRoot: root}
	h := s.Handler()
	call := func(path, body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("POST", path, strings.NewReader(body)))
		return w
	}
	if got := call("/internal/web/v1/backups/restore", `{"filename":"link.tar.gz"}`).Code; got != 400 {
		t.Fatalf("symlink restore: %d", got)
	}
	if got := call("/internal/web/v1/backups/restore", `{"filename":"new.tar.gz"}`).Code; got != 501 {
		t.Fatalf("restore: %d", got)
	}
	if got := call("/internal/web/v1/backups/cleanup", `{"keep_count":1}`).Code; got != 200 {
		t.Fatalf("cleanup: %d", got)
	}
	if _, err := os.Lstat(filepath.Join(root, "old.tar.gz")); !os.IsNotExist(err) {
		t.Fatalf("old backup remains: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "new.tar.gz")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, "link.tar.gz")); err != nil {
		t.Fatal(err)
	}
}

func TestUploadAtomicAndRejectsUnsafeName(t *testing.T) {
	root := t.TempDir()
	s := &Server{Guard: testGuard{}, BackupRoot: root}
	request := func(name string) *httptest.ResponseRecorder {
		var body bytes.Buffer
		m := multipart.NewWriter(&body)
		p, err := m.CreateFormFile("file", name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = p.Write([]byte("fixture"))
		_ = m.Close()
		r := httptest.NewRequest("POST", "/internal/web/v1/backups/upload", &body)
		r.Header.Set("Content-Type", m.FormDataContentType())
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	if got := request("../outside.tar.gz").Code; got != 400 {
		t.Fatalf("unsafe filename: %d", got)
	}
	if got := request("ok.tar.gz").Code; got != 200 {
		t.Fatalf("upload: %d", got)
	}
	got, err := os.ReadFile(filepath.Join(root, "ok.tar.gz"))
	if err != nil || string(got) != "fixture" {
		t.Fatalf("data=%q err=%v", got, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatalf("staging left behind: %v %v", entries, err)
	}
}

func TestCanceledUploadRemovesStagingAndDoesNotReplaceBackup(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "keep.tar.gz")
	if err := os.WriteFile(existing, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	m := multipart.NewWriter(&body)
	p, err := m.CreateFormFile("file", "keep.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = p.Write([]byte("replacement"))
	_ = m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest("POST", "/internal/web/v1/backups/upload", &body).WithContext(ctx)
	r.Header.Set("Content-Type", m.FormDataContentType())
	w := httptest.NewRecorder()
	(&Server{Guard: testGuard{}, BackupRoot: root}).Handler().ServeHTTP(w, r)
	if w.Code != 500 {
		t.Fatalf("canceled upload status=%d", w.Code)
	}
	got, err := os.ReadFile(existing)
	if err != nil || string(got) != "existing" {
		t.Fatalf("existing backup changed: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 1 {
		t.Fatalf("staging not cleaned: %v", err)
	}
}
