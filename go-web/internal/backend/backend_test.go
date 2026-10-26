package backend

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestClientUsesFixedAuthenticatedOperation(t *testing.T) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got := r.Header.Get("Authorization"); got != "Bearer private-token" {
			t.Errorf("authorization = %q", got)
		}
		if r.Method != http.MethodPost || r.URL.Path != "/internal/web/v1/categories/ai-category-analysis" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["limit"] != float64(50) || body["include_examples"] != true {
			t.Errorf("body = %#v", body)
		}
		return response(http.StatusOK, `{"suggestions":[],"total_analyzed":0,"execution_time_seconds":0}`), nil
	})
	client, err := NewClient(Config{BaseURL: "http://backend.internal", Token: "private-token"})
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = transport
	got, err := client.AnalyzeCategories(context.Background(), CategoryAnalysisRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(got) {
		t.Fatalf("invalid JSON: %s", got)
	}
}

func TestCacheInvalidationUsesDedicatedPrivateOperationOnce(t *testing.T) {
	calls := 0
	client, err := NewClient(Config{BaseURL: "http://backend.internal", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodPost || req.URL.Path != "/internal/web/v1/categories/invalidate-cache" {
			t.Errorf("request = %s %s", req.Method, req.URL.Path)
		}
		if req.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("authorization missing")
		}
		return response(http.StatusOK, `{"success":true}`), nil
	})
	if err := client.InvalidateCategoryCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("expected one invalidation request, got %d", calls)
	}
}

func TestTelegramConfigurationUsesSanitizedPrivateEndpoint(t *testing.T) {
	client, err := NewClient(Config{BaseURL: "http://backend.internal", Token: "service-token"})
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet || req.URL.Path != "/internal/web/v1/telegram/configuration" {
			t.Errorf("request = %s %s", req.Method, req.URL.Path)
		}
		if req.Header.Get("Authorization") != "Bearer service-token" {
			t.Errorf("authorization missing")
		}
		return response(http.StatusOK, `{"token_configured":true,"chat_id":"chat","chat_id_source":"db","service_chat_id":"service","service_chat_id_source":"db"}`), nil
	})
	out, err := client.TelegramConfiguration(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out), "telegram-secret") || !json.Valid(out) {
		t.Fatalf("unexpected configuration body: %s", out)
	}
}

func TestClientSanitizesBackendErrorsAndPreservesStatus(t *testing.T) {
	client, _ := NewClient(Config{BaseURL: "http://backend.internal", Token: "token"})
	calls := 0
	client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method != http.MethodPost || req.URL.Path != "/internal/web/v1/telegram/test" {
			t.Errorf("private request = %s %s", req.Method, req.URL.Path)
		}
		if req.Header.Get("Authorization") != "Bearer token" {
			t.Errorf("private backend token missing")
		}
		return response(http.StatusConflict, "private traceback with secret"), nil
	})
	_, err := client.TestTelegram(context.Background())
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Status != http.StatusConflict || strings.Contains(err.Error(), "traceback") || strings.Contains(err.Error(), "secret") || calls != 1 {
		t.Fatalf("error = %#v calls=%d", err, calls)
	}
}

func TestWorkerModeCategoryAnalysisAndProcessCleanupPreserveConflictOnce(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		call       func(*Client) error
	}{
		{
			name: "AI category analysis",
			path: "/internal/web/v1/categories/ai-category-analysis",
			call: func(client *Client) error {
				_, err := client.AnalyzeCategories(context.Background(), CategoryAnalysisRequest{})
				return err
			},
		},
		{
			name: "process cleanup",
			path: "/internal/web/v1/system/process-monitor/cleanup",
			call: func(client *Client) error { _, err := client.CleanupProcesses(context.Background()); return err },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client, err := NewClient(Config{BaseURL: "http://backend.internal", Token: "private-token"})
			if err != nil {
				t.Fatal(err)
			}
			client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				if req.Method != http.MethodPost || req.URL.Path != tc.path {
					t.Errorf("private request = %s %s", req.Method, req.URL.Path)
				}
				if req.Header.Get("Authorization") != "Bearer private-token" {
					t.Errorf("private backend token missing")
				}
				return response(http.StatusConflict, `{"detail":"operation is not supported in worker mode"}`), nil
			})
			err = tc.call(client)
			apiErr, ok := err.(*APIError)
			if !ok || apiErr.Status != http.StatusConflict || calls != 1 {
				t.Fatalf("error=%#v private calls=%d", err, calls)
			}
		})
	}
}

func TestClientDoesNotFollowRedirectsOrRetry(t *testing.T) {
	calls := 0
	client, err := NewClient(Config{BaseURL: "http://backend.internal", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		resp := response(http.StatusTemporaryRedirect, "redirect")
		resp.Header.Set("Location", "https://untrusted.example/collect")
		return resp, nil
	})
	_, err = client.TestTelegram(context.Background())
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Status != http.StatusBadGateway || calls != 1 {
		t.Fatalf("error=%#v calls=%d", err, calls)
	}
}

func TestClientBoundsJSONAndPreservesRestoreConflictStatus(t *testing.T) {
	client, err := NewClient(Config{BaseURL: "http://backend.internal", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusNotImplemented, `{"detail":"restore is not implemented"}`), nil
	})
	_, err = client.RestoreBackup(context.Background(), "backup.tar.gz")
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Status != http.StatusNotImplemented {
		t.Fatalf("error=%#v", err)
	}

	client.http.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusOK, strings.Repeat("x", maxJSONResponse+1)), nil
	})
	_, err = client.ListBackups(context.Background())
	apiErr, ok = err.(*APIError)
	if !ok || apiErr.Status != http.StatusBadGateway {
		t.Fatalf("error=%#v", err)
	}
}

func TestUploadStreamsMultipartWithBound(t *testing.T) {
	client, err := NewClient(Config{BaseURL: "http://backend.internal", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if !strings.HasPrefix(req.Header.Get("Content-Type"), "multipart/form-data; boundary=") {
			t.Errorf("content type = %q", req.Header.Get("Content-Type"))
		}
		body, readErr := io.ReadAll(req.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !strings.Contains(string(body), "name=\"file\"") || !strings.Contains(string(body), "test.tar.gz") || !strings.Contains(string(body), "archive bytes") {
			t.Errorf("multipart body missing expected form data")
		}
		return response(http.StatusOK, `{"success":true}`), nil
	})
	got, err := client.UploadBackup(context.Background(), "test.tar.gz", strings.NewReader("archive bytes"), 13)
	if err != nil || !json.Valid(got) {
		t.Fatalf("result=%s err=%v", got, err)
	}
}

func TestCanceledUploadStopsWithoutRetry(t *testing.T) {
	client, err := NewClient(Config{BaseURL: "http://backend.internal", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	started := make(chan struct{})
	client.http.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		var buf [64]byte
		_, _ = req.Body.Read(buf[:])
		close(started)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := client.UploadBackup(ctx, "test.tar.gz", strings.NewReader("archive-data"), 12)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("backend request did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled upload did not stop")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one request, got %d", calls.Load())
	}
}

func TestClientRequiresTokenAndSafeFilename(t *testing.T) {
	for _, base := range []string{"http://backend.internal/internal/web/v1", "http://backend.internal/%2e", "http://backend.internal?token=secret"} {
		if _, err := NewClient(Config{BaseURL: base, Token: "t"}); err == nil {
			t.Fatalf("expected non-origin backend URL to fail closed")
		}
	}
	if _, err := NewClient(Config{BaseURL: "http://127.0.0.1:8000"}); err == nil {
		t.Fatal("expected absent token to fail closed")
	}
	client, err := NewClient(Config{BaseURL: "http://127.0.0.1:8000", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DownloadBackup(context.Background(), "../secret.tar.gz"); err == nil {
		t.Fatal("expected traversal rejection")
	}
}

func TestUploadLimit(t *testing.T) {
	client, _ := NewClient(Config{BaseURL: "http://127.0.0.1:8000", Token: "t"})
	_, err := client.UploadBackup(context.Background(), "large.tar.gz", strings.NewReader("x"), MaxBackupBytes+1)
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.Status != http.StatusRequestEntityTooLarge {
		t.Fatalf("error = %#v", err)
	}
}
