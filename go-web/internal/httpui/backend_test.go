package httpui

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
)

type fakeBackendOps struct {
	calls            int
	raw              json.RawMessage
	err              error
	download         *backend.Download
	uploadReaderType string
}

func (f *fakeBackendOps) result() (json.RawMessage, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.raw, nil
}
func (f *fakeBackendOps) AnalyzeCategories(context.Context, backend.CategoryAnalysisRequest) (json.RawMessage, error) {
	return f.result()
}
func (f *fakeBackendOps) InvalidateCategoryCache(context.Context) error { f.calls++; return f.err }
func (f *fakeBackendOps) TelegramConfiguration(context.Context) (json.RawMessage, error) {
	return f.result()
}
func (f *fakeBackendOps) TestTelegram(context.Context) (json.RawMessage, error) { return f.result() }
func (f *fakeBackendOps) TelegramSettingsUpdated(context.Context) error         { f.calls++; return f.err }
func (f *fakeBackendOps) ListBackups(context.Context) (json.RawMessage, error)  { return f.result() }
func (f *fakeBackendOps) DownloadBackup(context.Context, string) (*backend.Download, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	if f.download != nil {
		return f.download, nil
	}
	return &backend.Download{Body: io.NopCloser(strings.NewReader("archive")), ContentType: "application/gzip", Filename: "db.tar.gz", Size: 7}, nil
}
func (f *fakeBackendOps) UploadBackup(_ context.Context, _ string, data io.Reader, _ int64) (json.RawMessage, error) {
	f.uploadReaderType = fmt.Sprintf("%T", data)
	return f.result()
}
func (f *fakeBackendOps) RestoreBackup(context.Context, string) (json.RawMessage, error) {
	return f.result()
}
func (f *fakeBackendOps) CleanupBackups(context.Context, int) (json.RawMessage, error) {
	return f.result()
}
func (f *fakeBackendOps) BackupSchedule(context.Context) (json.RawMessage, error) { return f.result() }
func (f *fakeBackendOps) UpdateBackupSchedule(context.Context, backend.BackupScheduleRequest) (json.RawMessage, error) {
	return f.result()
}
func (f *fakeBackendOps) ProcessMonitor(context.Context) (json.RawMessage, error) { return f.result() }
func (f *fakeBackendOps) CleanupProcesses(context.Context) (json.RawMessage, error) {
	return f.result()
}
func (f *fakeBackendOps) RunMigrations(context.Context) (json.RawMessage, error) { return f.result() }

func TestBackendRoutesRequireAdminBeforeCallingOperation(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	ops := &fakeBackendOps{raw: json.RawMessage(`{"success":true}`)}
	mux := http.NewServeMux()
	RegisterBackend(mux, service, ops)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/v1/telegram/test", nil))
	if resp.Code != http.StatusUnauthorized || ops.calls != 0 {
		t.Fatalf("status=%d backend calls=%d", resp.Code, ops.calls)
	}
}

func TestTelegramDiagnosticDelegatesOnceAndPreservesBackendStatus(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, err := service.Mint("admin", auth.LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	ops := &fakeBackendOps{raw: json.RawMessage(`{"success":true}`)}
	mux := http.NewServeMux()
	RegisterBackend(mux, service, ops)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/telegram/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != 200 || ops.calls != 1 || resp.Body.String() != `{"success":true}` {
		t.Fatalf("status=%d calls=%d body=%q", resp.Code, ops.calls, resp.Body.String())
	}

	ops.err = &backend.APIError{Status: http.StatusConflict, Detail: "operation conflict"}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/telegram/test", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusConflict || strings.Contains(resp.Body.String(), "traceback") {
		t.Fatalf("status=%d body=%q", resp.Code, resp.Body.String())
	}
}

func TestWorkerModeBackendConflictsPreserveAnalysisAndCleanupContracts(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, err := service.Mint("admin", auth.LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, method, path, body string
	}{
		{"AI category analysis is unavailable in queue-only mode", http.MethodPost, "/api/v1/categories/ai-category-analysis", `{}`},
		{"process cleanup remains owned by collector", http.MethodPost, "/api/v1/system/process-monitor/cleanup", ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ops := &fakeBackendOps{err: &backend.APIError{Status: http.StatusConflict, Detail: "operation is not supported in worker mode"}}
			mux := http.NewServeMux()
			RegisterBackend(mux, service, ops)
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer "+token)
			resp := httptest.NewRecorder()
			mux.ServeHTTP(resp, req)
			if resp.Code != http.StatusConflict || ops.calls != 1 || !strings.Contains(resp.Body.String(), "operation is not supported") {
				t.Fatalf("status=%d calls=%d body=%q", resp.Code, ops.calls, resp.Body.String())
			}
		})
	}
}

func TestBackupDownloadAndUnsafeFilename(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	ops := &fakeBackendOps{}
	mux := http.NewServeMux()
	RegisterBackend(mux, service, ops)
	call := func(uri string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, uri, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}
	resp := call("/api/v1/backup/download/db.tar.gz")
	if resp.Code != 200 || resp.Body.String() != "archive" || resp.Header().Get("Content-Type") != "application/gzip" {
		t.Fatalf("status=%d body=%q headers=%v", resp.Code, resp.Body.String(), resp.Header())
	}
	resp = call("/api/v1/backup/download/%2e%2e%2fsecret.tar.gz")
	if resp.Code != http.StatusBadRequest || ops.calls != 1 {
		t.Fatalf("status=%d backend calls=%d", resp.Code, ops.calls)
	}
}

type observedReader struct {
	remaining int
	readBytes int
}

func (r *observedReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if len(p) > 1024 {
		p = p[:1024]
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = 'x'
	}
	r.remaining -= len(p)
	r.readBytes += len(p)
	return len(p), nil
}

type observingWriter struct {
	header         http.Header
	status         int
	reader         *observedReader
	wroteBeforeEOF bool
}

func (w *observingWriter) Header() http.Header  { return w.header }
func (w *observingWriter) WriteHeader(code int) { w.status = code }
func (w *observingWriter) Write(p []byte) (int, error) {
	if w.reader.remaining > 0 {
		w.wroteBeforeEOF = true
	}
	return len(p), nil
}

func TestBackupDownloadStreamsAndRejectsUnknownLength(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	reader := &observedReader{remaining: 8 << 20}
	ops := &fakeBackendOps{download: &backend.Download{Body: io.NopCloser(reader), ContentType: "application/gzip", Filename: "db.tar.gz", Size: int64(reader.remaining)}}
	h := &backendHandler{ops: ops}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/backup/download/db.tar.gz", nil)
	req.SetPathValue("filename", "db.tar.gz")
	req.Header.Set("Authorization", "Bearer "+token)
	w := &observingWriter{header: make(http.Header), reader: reader}
	h.downloadBackup(w, req)
	if w.status != http.StatusOK || !w.wroteBeforeEOF || reader.remaining != 0 {
		t.Fatalf("status=%d streamed-before-eof=%t remaining=%d", w.status, w.wroteBeforeEOF, reader.remaining)
	}
	if got := w.header.Get("Content-Length"); got != "8388608" {
		t.Fatalf("content length = %q", got)
	}

	ops.download = &backend.Download{Body: io.NopCloser(strings.NewReader("archive")), ContentType: "application/gzip", Filename: "db.tar.gz", Size: -1}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/backup/download/db.tar.gz", nil)
	req.SetPathValue("filename", "db.tar.gz")
	w2 := httptest.NewRecorder()
	h.downloadBackup(w2, req)
	if w2.Code != http.StatusBadGateway {
		t.Fatalf("unknown length status=%d", w2.Code)
	}
}

func TestBackendUnavailableIs503(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	mux := http.NewServeMux()
	RegisterBackend(mux, service, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/system/process-monitor", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestUnsafeBackupUploadIsRejectedBeforeBackend(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	ops := &fakeBackendOps{raw: json.RawMessage(`{"success":true}`)}
	mux := http.NewServeMux()
	RegisterBackend(mux, service, ops)
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreatePart(textproto.MIMEHeader{
		"Content-Disposition": {`form-data; name="file"; filename="../backup.tar.gz"`},
		"Content-Type":        {"application/gzip"},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(part, "fixture")
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backup/upload", &body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", form.FormDataContentType())
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusBadRequest || ops.calls != 0 {
		t.Fatalf("status=%d backend calls=%d body=%s", resp.Code, ops.calls, resp.Body.String())
	}
}

type repeatByteReader struct{ remaining int64 }

func (r *repeatByteReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = 'x'
	}
	r.remaining -= int64(len(p))
	return len(p), nil
}

func TestOversizedBackupUploadIsRejectedBeforeBackend(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	ops := &fakeBackendOps{raw: json.RawMessage(`{"success":true}`)}
	mux := http.NewServeMux()
	RegisterBackend(mux, service, ops)
	boundary := "oversized-boundary"
	prefix := "--" + boundary + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\"large.tar.gz\"\r\nContent-Type: application/gzip\r\n\r\n"
	body := io.MultiReader(strings.NewReader(prefix), &repeatByteReader{remaining: maxBackupUpload + (128 << 10) + 1})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backup/upload", body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusRequestEntityTooLarge || ops.calls != 0 {
		t.Fatalf("status=%d backend calls=%d", resp.Code, ops.calls)
	}
}

func TestBackupUploadUsesDiskBackedMultipartSpool(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	ops := &fakeBackendOps{raw: json.RawMessage(`{"success":true}`)}
	mux := http.NewServeMux()
	RegisterBackend(mux, service, ops)
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	part, err := form.CreateFormFile("file", "backup.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	// Above ParseMultipartForm's 1 MiB memory budget, the handler must pass a
	// disk-backed file to the streaming backend client.
	_, _ = io.Copy(part, &repeatByteReader{remaining: (2 << 20)})
	if err := form.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/backup/upload", &body)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", form.FormDataContentType())
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK || ops.calls != 1 {
		t.Fatalf("status=%d backend calls=%d body=%s", resp.Code, ops.calls, resp.Body.String())
	}
	// Multipart FormFile returns an *os.File when the request exceeds the
	// memory budget; upload handling can then stream it without an extra copy.
	if ops.uploadReaderType != "*os.File" {
		t.Fatalf("upload reader type = %q, want *os.File", ops.uploadReaderType)
	}
}
