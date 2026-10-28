package webbackend

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundaryRealSocketPartialBodyTimeoutAndDisconnect(t *testing.T) {
	var calls atomic.Int32
	handler := Boundary{Token: "fixture", timeoutOverride: 80 * time.Millisecond, Next: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) })}
	server := httptest.NewServer(handler)
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(conn, "POST /internal/web/v1/test HTTP/1.1\r\nHost: fixture\r\nAuthorization: Bearer fixture\r\nContent-Length: 40\r\n\r\npartial")
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil || resp.StatusCode != 504 {
		t.Fatalf("partial timeout: response=%v error=%v", resp, err)
	}
	_ = resp.Body.Close()
	_ = conn.Close()
	conn, err = net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(conn, "POST /internal/web/v1/test HTTP/1.1\r\nHost: fixture\r\nAuthorization: Bearer fixture\r\nContent-Length: 40\r\n\r\npartial")
	_ = conn.Close()
	time.Sleep(120 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("partial body reached handler")
	}
}

func TestBoundaryHandlerDeadlineCancelsAndStreamsWithoutBuffering(t *testing.T) {
	cleaned := make(chan struct{})
	b := Boundary{Token: "fixture", timeoutOverride: 40 * time.Millisecond, Next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(cleaned)
		<-r.Context().Done()
	})}
	server := httptest.NewServer(b)
	defer server.Close()
	req, _ := http.NewRequestWithContext(context.Background(), "POST", server.URL+"/internal/web/v1/test", nil)
	req.Header.Set("Authorization", "Bearer fixture")
	resp, err := server.Client().Do(req)
	if err != nil || resp.StatusCode != 504 {
		t.Fatalf("stalled handler: response=%v error=%v", resp, err)
	}
	_ = resp.Body.Close()
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("handler not cancelled")
	}
}

func TestBoundaryStreamsBeforeDeadlineAndCancelsLateWrites(t *testing.T) {
	stopped := make(chan struct{})
	b := Boundary{Token: "fixture", timeoutOverride: 80 * time.Millisecond, Next: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(stopped)
		_, _ = w.Write([]byte("fixture chunk"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		_, _ = w.Write([]byte("late chunk"))
	})}
	server := httptest.NewServer(b)
	defer server.Close()
	req, _ := http.NewRequest("GET", server.URL+"/internal/web/v1/backups/fixture", nil)
	req.Header.Set("Authorization", "Bearer fixture")
	resp, err := server.Client().Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("stream response %v %v", resp, err)
	}
	data := make([]byte, len("fixture chunk"))
	if _, err = io.ReadFull(resp.Body, data); err != nil || string(data) != "fixture chunk" {
		t.Fatalf("stream chunk %q %v", data, err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stream handler not cancelled")
	}
	rest, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if strings.Contains(string(rest), "late chunk") {
		t.Fatal("late write delivered")
	}
}

func TestBoundaryUploadDeadlineCleansStaging(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	stopped := make(chan struct{})
	b := Boundary{Token: "fixture", timeoutOverride: 40 * time.Millisecond, Next: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done(); close(stopped) })}
	server := httptest.NewServer(b)
	defer server.Close()
	req, _ := http.NewRequest("POST", server.URL+"/internal/web/v1/backups/upload", strings.NewReader("synthetic"))
	req.Header.Set("Authorization", "Bearer fixture")
	resp, err := server.Client().Do(req)
	if err != nil || resp.StatusCode != 504 {
		t.Fatalf("upload timeout %v %v", resp, err)
	}
	_ = resp.Body.Close()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upload handler not cancelled")
	}
	deadline := time.Now().Add(time.Second)
	for {
		files, err := os.ReadDir(dir)
		if err == nil && len(files) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("upload staging not removed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBoundaryTimeoutKeepsHandlerAndStagingTrackedUntilExit(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	release := make(chan struct{})
	entered := make(chan struct{})
	returned := make(chan struct{})
	b := Boundary{Token: "fixture", timeoutOverride: 50 * time.Millisecond, Next: http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) { close(entered); <-release })}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(returned); b.ServeHTTP(w, r) }))
	defer server.Close()
	req, _ := http.NewRequest("POST", server.URL+"/internal/web/v1/backups/upload", strings.NewReader("synthetic"))
	req.Header.Set("Authorization", "Bearer fixture")
	resp, err := server.Client().Do(req)
	if err != nil || resp.StatusCode != 504 {
		close(release)
		t.Fatalf("response %v %v", resp, err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	<-entered
	select {
	case <-returned:
		close(release)
		t.Fatal("request tracker returned before handler exit")
	default:
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 {
		close(release)
		t.Fatal("staging removed before handler exit")
	}
	close(release)
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("request did not finish")
	}
	files, err = os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatal("staging not cleaned after exit")
	}
}
