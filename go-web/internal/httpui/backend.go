package httpui

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
)

const maxBackupUpload = 64 << 20

// RegisterBackend wires the explicitly supported private-backend operations.
// Job-owned backup creation and Telegram digest delivery are registered by the
// durable job lane and deliberately do not appear here.
func RegisterBackend(mux *http.ServeMux, service *auth.Service, ops backend.Operations) {
	h := &backendHandler{ops: ops}
	admin := func(pattern string, fn http.HandlerFunc) { mux.Handle(pattern, RequireAPIAdmin(service, fn)) }
	admin("GET /api/v1/ai/settings", h.aiSettingsGet)
	admin("PUT /api/v1/ai/settings", h.aiSettingsPut)
	admin("GET /api/v1/credentials/{provider}", h.credential)
	admin("PUT /api/v1/credentials/{provider}", h.credential)
	admin("DELETE /api/v1/credentials/{provider}", h.credential)
	admin("GET /api/v1/configuration/readiness", h.configuration)
	admin("GET /api/v1/configuration/telegram", h.configuration)
	admin("PUT /api/v1/configuration/telegram", h.configuration)
	admin("GET /api/v1/configuration/operational", h.configuration)
	admin("PUT /api/v1/configuration/operational", h.configuration)
	admin("POST /api/v1/categories/ai-category-analysis", h.analyzeCategories)
	admin("POST /api/v1/telegram/test", h.testTelegram)
	admin("GET /api/v1/backup", h.listBackups)
	admin("GET /api/v1/backups", h.listBackups)
	admin("GET /api/v1/backup/download/{filename}", h.downloadBackup)
	admin("POST /api/v1/backup/upload", h.uploadBackup)
	admin("POST /api/v1/backup/restore", h.restoreBackup)
	admin("POST /api/v1/backup/cleanup", h.cleanupBackups)
	admin("GET /api/v1/backup/schedule", h.backupSchedule)
	admin("POST /api/v1/backup/schedule", h.updateBackupSchedule)
	admin("GET /api/v1/system/process-monitor", h.processMonitor)
	admin("POST /api/v1/system/process-monitor/cleanup", h.cleanupProcesses)
	admin("POST /api/v1/system/migrations/run", h.runMigrations)
}

type backendHandler struct{ ops backend.Operations }

func (h *backendHandler) configuration(w http.ResponseWriter, r *http.Request) {
	ops, ok := h.ops.(backend.ConfigurationOperations)
	if !ok {
		writeProblem(w, 503, "Private operation unavailable")
		return
	}
	group := strings.TrimPrefix(r.URL.Path, "/api/v1/configuration/")
	var body json.RawMessage
	if r.Method == http.MethodPut {
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		data, err := io.ReadAll(r.Body)
		if err != nil || !json.Valid(data) || hasDuplicateJSONKeys(data) {
			writeProblem(w, 422, "Invalid configuration")
			return
		}
		body = data
	}
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) {
		return ops.Configuration(ctx, r.Method, group, body)
	})
}

func (h *backendHandler) credential(w http.ResponseWriter, r *http.Request) {
	ops, ok := h.ops.(backend.CredentialOperations)
	if !ok {
		writeProblem(w, 503, "Private operation unavailable")
		return
	}
	provider := r.PathValue("provider")
	if provider != "gemini" && provider != "openai" && provider != "telegram" {
		writeProblem(w, 422, "Invalid provider")
		return
	}
	var body json.RawMessage
	if r.Method != http.MethodGet {
		r.Body = http.MaxBytesReader(w, r.Body, 8192)
		data, err := io.ReadAll(r.Body)
		if err != nil || !json.Valid(data) || hasDuplicateJSONKeys(data) {
			writeProblem(w, 422, "Invalid credential request")
			return
		}
		body = data
	}
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) {
		out, err := ops.Credential(ctx, r.Method, provider, body)
		if err != nil {
			return nil, err
		}
		var meta struct {
			Provider   string `json:"provider"`
			Revision   int64  `json:"revision"`
			Configured bool   `json:"configured"`
		}
		if json.Unmarshal(out, &meta) != nil || meta.Provider != provider || meta.Revision < 0 {
			return nil, &backend.APIError{Status: 502, Detail: "Invalid credential metadata"}
		}
		return json.Marshal(meta)
	})
}

func (h *backendHandler) call(w http.ResponseWriter, r *http.Request, fn func(context.Context) (json.RawMessage, error)) {
	if h.ops == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Private operation unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 70*time.Second)
	defer cancel()
	out, err := fn(ctx)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}
func (h *backendHandler) analyzeCategories(w http.ResponseWriter, r *http.Request) {
	var in backend.CategoryAnalysisRequest
	if err := decodeBackendJSON(w, r, &in); err != nil {
		writeProblem(w, 422, "Invalid request body")
		return
	}
	if in.Limit == 0 {
		in.Limit = 50
	}
	if in.IncludeExamples == nil {
		v := true
		in.IncludeExamples = &v
	}
	if in.ConfidenceThreshold == nil {
		v := .7
		in.ConfidenceThreshold = &v
	}
	if in.Limit < 1 || in.Limit > 50 || *in.ConfidenceThreshold < 0 || *in.ConfidenceThreshold > 1 {
		writeProblem(w, 422, "Invalid category analysis options")
		return
	}
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.AnalyzeCategories(ctx, in) })
}
func (h *backendHandler) testTelegram(w http.ResponseWriter, r *http.Request) {
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.TestTelegram(ctx) })
}
func (h *backendHandler) listBackups(w http.ResponseWriter, r *http.Request) {
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.ListBackups(ctx) })
}
func (h *backendHandler) downloadBackup(w http.ResponseWriter, r *http.Request) {
	if h.ops == nil {
		writeProblem(w, 503, "Private operation unavailable")
		return
	}
	escaped := strings.ToLower(r.URL.EscapedPath())
	if strings.Contains(escaped, "%2f") || strings.Contains(escaped, "%5c") {
		writeProblem(w, http.StatusBadRequest, "Invalid filename")
		return
	}
	filename := r.PathValue("filename")
	if !backend.ValidBackupFilename(filename) {
		writeProblem(w, http.StatusBadRequest, "Invalid filename")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 70*time.Second)
	defer cancel()
	file, err := h.ops.DownloadBackup(ctx, filename)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	defer file.Body.Close()
	// A known positive length lets us set the response headers before streaming.
	// Unknown-length files cannot be safely bounded after the status is sent.
	if file.Size <= 0 {
		writeProblem(w, http.StatusBadGateway, "Private backend returned an invalid file length")
		return
	}
	if file.Size > maxBackupUpload {
		writeProblem(w, 502, "Private backend response exceeded the file limit")
		return
	}
	w.Header().Set("Content-Type", file.ContentType)
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": file.Filename}))
	w.Header().Set("Content-Length", strconv.FormatInt(file.Size, 10))
	w.WriteHeader(http.StatusOK)
	// The declared length is checked above; copy only that many bytes so a
	// broken private response cannot exceed the public 64 MiB limit.
	if _, err := io.CopyN(w, file.Body, file.Size); err != nil {
		// The status and Content-Length are already committed. Abort the HTTP
		// stream so clients cannot mistake a truncated backup for a complete one.
		panic(http.ErrAbortHandler)
	}
}
func (h *backendHandler) uploadBackup(w http.ResponseWriter, r *http.Request) {
	if h.ops == nil {
		writeProblem(w, 503, "Private operation unavailable")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBackupUpload+(128<<10))
	err := r.ParseMultipartForm(1 << 20)
	// ParseMultipartForm can create temp files before returning an error. Clean
	// them up even on malformed or oversized requests.
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeProblem(w, 413, "Backup file exceeds the 64 MiB limit")
		} else {
			writeProblem(w, 422, "Invalid multipart upload")
		}
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeProblem(w, 422, "A backup file is required")
		return
	}
	defer file.Close()
	_, dispositionParams, dispositionErr := mime.ParseMediaType(header.Header.Get("Content-Disposition"))
	rawFilename := dispositionParams["filename"]
	if dispositionErr != nil || !backend.ValidBackupFilename(rawFilename) || !strings.HasSuffix(rawFilename, ".tar.gz") {
		writeProblem(w, http.StatusBadRequest, "Invalid file type. Only .tar.gz files are allowed.")
		return
	}
	if header.Size > maxBackupUpload {
		writeProblem(w, 413, "Backup file exceeds the 64 MiB limit")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 70*time.Second)
	defer cancel()
	out, err := h.ops.UploadBackup(ctx, rawFilename, file, header.Size)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(out)
}
func (h *backendHandler) restoreBackup(w http.ResponseWriter, r *http.Request) {
	filename := r.URL.Query().Get("filename")
	if filename == "" {
		writeProblem(w, 422, "filename is required")
		return
	}
	if !backend.ValidBackupFilename(filename) {
		writeProblem(w, http.StatusBadRequest, "Invalid filename")
		return
	}
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.RestoreBackup(ctx, filename) })
}
func (h *backendHandler) cleanupBackups(w http.ResponseWriter, r *http.Request) {
	keep := 5
	if raw := r.URL.Query().Get("keep_count"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeProblem(w, 422, "keep_count must be an integer")
			return
		}
		keep = n
	}
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.CleanupBackups(ctx, keep) })
}
func (h *backendHandler) backupSchedule(w http.ResponseWriter, r *http.Request) {
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.BackupSchedule(ctx) })
}
func (h *backendHandler) updateBackupSchedule(w http.ResponseWriter, r *http.Request) {
	var in backend.BackupScheduleRequest
	if err := decodeBackendJSON(w, r, &in); err != nil {
		writeProblem(w, 422, "Invalid request body")
		return
	}
	if in.KeepCount < 1 || in.KeepCount > 100 || (in.Frequency != "daily" && in.Frequency != "weekly" && in.Frequency != "monthly") {
		writeProblem(w, 422, "Invalid backup schedule settings")
		return
	}
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.UpdateBackupSchedule(ctx, in) })
}
func (h *backendHandler) processMonitor(w http.ResponseWriter, r *http.Request) {
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.ProcessMonitor(ctx) })
}
func (h *backendHandler) cleanupProcesses(w http.ResponseWriter, r *http.Request) {
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.CleanupProcesses(ctx) })
}
func (h *backendHandler) runMigrations(w http.ResponseWriter, r *http.Request) {
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) { return h.ops.RunMigrations(ctx) })
}

func decodeBackendJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("request must contain one JSON value")
	}
	return nil
}
func writeBackendError(w http.ResponseWriter, err error) {
	var apiErr *backend.APIError
	if errors.As(err, &apiErr) {
		writeProblem(w, apiErr.Status, apiErr.Detail)
		return
	}
	if errors.Is(err, backend.ErrUnavailable) {
		writeProblem(w, http.StatusServiceUnavailable, "Private operation unavailable")
		return
	}
	writeProblem(w, http.StatusBadGateway, "Private operation failed")
}
