// Package backend contains the narrow, authenticated client for private Python
// operations used by the Go web application.
package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	maxJSONResponse = 5 << 20
	MaxBackupBytes  = 64 << 20
)

var ErrUnavailable = errors.New("private backend is unavailable")

type Config struct {
	BaseURL string
	Token   string
	Timeout time.Duration
}

// Operations lists every private operation explicitly. It intentionally has no
// arbitrary URL, path, SQL, or generic proxy method.
type Operations interface {
	AnalyzeCategories(context.Context, CategoryAnalysisRequest) (json.RawMessage, error)
	InvalidateCategoryCache(context.Context) error
	TelegramConfiguration(context.Context) (json.RawMessage, error)
	TestTelegram(context.Context) (json.RawMessage, error)
	TelegramSettingsUpdated(context.Context) error
	ListBackups(context.Context) (json.RawMessage, error)
	DownloadBackup(context.Context, string) (*Download, error)
	UploadBackup(context.Context, string, io.Reader, int64) (json.RawMessage, error)
	RestoreBackup(context.Context, string) (json.RawMessage, error)
	CleanupBackups(context.Context, int) (json.RawMessage, error)
	BackupSchedule(context.Context) (json.RawMessage, error)
	UpdateBackupSchedule(context.Context, BackupScheduleRequest) (json.RawMessage, error)
	ProcessMonitor(context.Context) (json.RawMessage, error)
	CleanupProcesses(context.Context) (json.RawMessage, error)
	RunMigrations(context.Context) (json.RawMessage, error)
}

type CategoryAnalysisRequest struct {
	Limit               int      `json:"limit"`
	IncludeExamples     *bool    `json:"include_examples,omitempty"`
	ConfidenceThreshold *float64 `json:"confidence_threshold,omitempty"`
}
type BackupScheduleRequest struct {
	Enabled   bool   `json:"enabled"`
	Frequency string `json:"frequency"`
	Time      string `json:"time"`
	KeepCount int    `json:"keep_count"`
}
type Download struct {
	Body        io.ReadCloser
	ContentType string
	Filename    string
	Size        int64
}

// APIError preserves the private service status while keeping upstream error
// text out of the web response.
type APIError struct {
	Status int
	Detail string
}

func (e *APIError) Error() string { return e.Detail }

type Client struct {
	base  *url.URL
	token string
	http  *http.Client
}

func NewClient(cfg Config) (*Client, error) {
	cfg.Token = strings.TrimSpace(cfg.Token)
	if cfg.Token == "" || strings.ContainsAny(cfg.Token, "\r\n") {
		return nil, fmt.Errorf("%w: WEB_BACKEND_TOKEN is not configured", ErrUnavailable)
	}
	parsed, err := url.Parse(strings.TrimSpace(cfg.BaseURL))
	if err != nil || parsed == nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("invalid private backend base URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("private backend URL must use http or https")
	}
	if parsed.Path != "" && parsed.Path != "/" || parsed.RawPath != "" {
		return nil, errors.New("private backend URL must be an origin without a path")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 70 * time.Second
	}
	return &Client{base: parsed, token: cfg.Token, http: &http.Client{Timeout: cfg.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}

// NewClientFromEnv reads only the private backend origin and its independent
// service token. An absent token fails closed and leaves the adapter unavailable.
func NewClientFromEnv() (*Client, error) {
	base := strings.TrimSpace(os.Getenv("WEB_BACKEND_URL"))
	token := strings.TrimSpace(os.Getenv("WEB_BACKEND_TOKEN"))
	if base == "" {
		return nil, errors.New("WEB_BACKEND_URL is required")
	}
	if token == "" {
		return nil, errors.New("WEB_BACKEND_TOKEN is required")
	}
	return NewClient(Config{BaseURL: base, Token: token, Timeout: 70 * time.Second})
}

func (c *Client) AnalyzeCategories(ctx context.Context, in CategoryAnalysisRequest) (json.RawMessage, error) {
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
	return c.json(ctx, http.MethodPost, "/categories/ai-category-analysis", in)
}

// AISettingsOperations is optional to preserve compatibility with other backend implementations.
type AISettingsOperations interface {
	GetAISettings(context.Context) (json.RawMessage, error)
	UpdateAISettings(context.Context, json.RawMessage) (json.RawMessage, error)
}

type CredentialOperations interface {
	Credential(context.Context, string, string, json.RawMessage) (json.RawMessage, error)
}

type ConfigurationOperations interface {
	Configuration(context.Context, string, string, json.RawMessage) (json.RawMessage, error)
}

func (c *Client) Configuration(ctx context.Context, method, group string, body json.RawMessage) (json.RawMessage, error) {
	if group != "telegram" && group != "viber" && group != "operational" && group != "readiness" {
		return nil, &APIError{Status: 422, Detail: "Invalid configuration group"}
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	return c.rawJSON(ctx, method, "/configuration/"+group, reader, "application/json")
}

func (c *Client) Credential(ctx context.Context, method, provider string, body json.RawMessage) (json.RawMessage, error) {
	if provider != "gemini" && provider != "openai" && provider != "telegram" && provider != "viber" {
		return nil, &APIError{Status: 422, Detail: "Invalid provider"}
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	return c.rawJSON(ctx, method, "/credentials/"+provider, reader, "application/json")
}

func (c *Client) GetAISettings(ctx context.Context) (json.RawMessage, error) {
	return c.json(ctx, http.MethodGet, "/ai/settings", nil)
}
func (c *Client) UpdateAISettings(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
	return c.rawJSON(ctx, http.MethodPut, "/ai/settings", bytes.NewReader(in), "application/json")
}
func (c *Client) TestTelegram(ctx context.Context) (json.RawMessage, error) {
	return c.json(ctx, http.MethodPost, "/telegram/test", struct{}{})
}
func (c *Client) TelegramConfiguration(ctx context.Context) (json.RawMessage, error) {
	return c.json(ctx, http.MethodGet, "/telegram/configuration", nil)
}
func (c *Client) InvalidateCategoryCache(ctx context.Context) error {
	_, err := c.json(ctx, http.MethodPost, "/categories/invalidate-cache", struct{}{})
	return err
}

// NotifyCategoryMappingsChanged satisfies the management notifier contract.
func (c *Client) NotifyCategoryMappingsChanged(ctx context.Context) error {
	return c.InvalidateCategoryCache(ctx)
}
func (c *Client) TelegramSettingsUpdated(ctx context.Context) error {
	_, err := c.json(ctx, http.MethodPost, "/telegram/settings-updated", struct{}{})
	return err
}
func (c *Client) ListBackups(ctx context.Context) (json.RawMessage, error) {
	return c.json(ctx, http.MethodGet, "/backups", nil)
}
func (c *Client) DownloadBackup(ctx context.Context, filename string) (*Download, error) {
	if !ValidBackupFilename(filename) {
		return nil, &APIError{Status: 400, Detail: "Invalid filename"}
	}
	resp, err := c.request(ctx, http.MethodGet, "/backups/"+url.PathEscape(filename), nil, "")
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, upstreamError(resp)
	}
	if resp.ContentLength > MaxBackupBytes {
		resp.Body.Close()
		return nil, &APIError{Status: 502, Detail: "Private backend response exceeded the file limit"}
	}
	return &Download{Body: &boundedReadCloser{ReadCloser: resp.Body, remaining: MaxBackupBytes}, ContentType: "application/gzip", Filename: filename, Size: resp.ContentLength}, nil
}
func (c *Client) UploadBackup(ctx context.Context, filename string, data io.Reader, size int64) (json.RawMessage, error) {
	if !ValidBackupFilename(filename) || !strings.HasSuffix(filename, ".tar.gz") {
		return nil, &APIError{Status: 400, Detail: "Invalid file type. Only .tar.gz files are allowed."}
	}
	if size < 0 || size > MaxBackupBytes {
		return nil, &APIError{Status: 413, Detail: "Backup file exceeds the 64 MiB limit"}
	}
	reader, pipe := io.Pipe()
	writer := multipart.NewWriter(pipe)
	contentType := writer.FormDataContentType()
	copyDone := make(chan error, 1)
	go func() {
		part, err := writer.CreateFormFile("file", filename)
		if err == nil {
			var n int64
			n, err = io.Copy(part, io.LimitReader(data, MaxBackupBytes+1))
			if n > MaxBackupBytes {
				err = &APIError{Status: http.StatusRequestEntityTooLarge, Detail: "Backup file exceeds the 64 MiB limit"}
			} else if err == nil && n != size {
				err = &APIError{Status: http.StatusBadRequest, Detail: "Backup upload size did not match the request"}
			}
		}
		if err == nil {
			err = writer.Close()
		}
		_ = pipe.CloseWithError(err)
		copyDone <- err
	}()
	result, err := c.rawJSON(ctx, http.MethodPost, "/backups/upload", reader, contentType)
	_ = reader.Close()
	copyErr := <-copyDone
	if err != nil {
		return nil, err
	}
	if copyErr != nil {
		return nil, copyErr
	}
	return result, nil
}
func (c *Client) RestoreBackup(ctx context.Context, filename string) (json.RawMessage, error) {
	if !ValidBackupFilename(filename) {
		return nil, &APIError{Status: 400, Detail: "Invalid filename"}
	}
	return c.json(ctx, http.MethodPost, "/backups/restore", map[string]string{"filename": filename})
}
func (c *Client) CleanupBackups(ctx context.Context, keepCount int) (json.RawMessage, error) {
	if keepCount < 0 || keepCount > 100 {
		return nil, &APIError{Status: 422, Detail: "keep_count must be between 0 and 100"}
	}
	return c.json(ctx, http.MethodPost, "/backups/cleanup", map[string]int{"keep_count": keepCount})
}
func (c *Client) BackupSchedule(ctx context.Context) (json.RawMessage, error) {
	return c.json(ctx, http.MethodGet, "/backups/schedule", nil)
}
func (c *Client) UpdateBackupSchedule(ctx context.Context, in BackupScheduleRequest) (json.RawMessage, error) {
	if in.KeepCount < 1 || in.KeepCount > 100 || (in.Frequency != "daily" && in.Frequency != "weekly" && in.Frequency != "monthly") || !validClock(in.Time) {
		return nil, &APIError{Status: 422, Detail: "Invalid backup schedule settings"}
	}
	return c.json(ctx, http.MethodPut, "/backups/schedule", in)
}
func (c *Client) ProcessMonitor(ctx context.Context) (json.RawMessage, error) {
	return c.json(ctx, http.MethodGet, "/system/process-monitor", nil)
}
func (c *Client) CleanupProcesses(ctx context.Context) (json.RawMessage, error) {
	return c.json(ctx, http.MethodPost, "/system/process-monitor/cleanup", struct{}{})
}
func (c *Client) RunMigrations(ctx context.Context) (json.RawMessage, error) {
	return c.json(ctx, http.MethodPost, "/migrations/run", struct{}{})
}

func (c *Client) json(ctx context.Context, method, endpoint string, value any) (json.RawMessage, error) {
	var body io.Reader
	if value != nil {
		b, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	return c.rawJSON(ctx, method, endpoint, body, "application/json")
}
func (c *Client) rawJSON(ctx context.Context, method, endpoint string, body io.Reader, contentType string) (json.RawMessage, error) {
	resp, err := c.request(ctx, method, endpoint, body, contentType)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, upstreamError(resp)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxJSONResponse+1))
	if err != nil {
		return nil, fmt.Errorf("%w: response read failed", ErrUnavailable)
	}
	if len(data) > maxJSONResponse {
		return nil, &APIError{Status: 502, Detail: "Private backend response exceeded the response limit"}
	}
	if !json.Valid(data) {
		return nil, &APIError{Status: 502, Detail: "Private backend returned invalid JSON"}
	}
	return json.RawMessage(data), nil
}
func (c *Client) request(ctx context.Context, method, endpoint string, body io.Reader, contentType string) (*http.Response, error) {
	if c == nil || c.base == nil || c.token == "" {
		return nil, fmt.Errorf("%w: private backend is not configured", ErrUnavailable)
	}
	if !strings.HasPrefix(endpoint, "/") {
		return nil, errors.New("invalid private operation")
	}
	operationURL, err := url.ParseRequestURI(endpoint)
	if err != nil || operationURL.IsAbs() || operationURL.Host != "" || operationURL.Fragment != "" {
		return nil, errors.New("invalid private operation")
	}
	u := *c.base
	u.Path = path.Join(strings.TrimSuffix(c.base.Path, "/"), "/internal/web/v1", operationURL.Path)
	u.RawQuery = operationURL.RawQuery
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: private operation failed", ErrUnavailable)
	}
	return resp, nil
}
func upstreamError(resp *http.Response) error {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	status := resp.StatusCode
	if status < 400 || status > 599 {
		status = http.StatusBadGateway
	}
	return &APIError{Status: status, Detail: "Private operation failed"}
}

// ValidBackupFilename accepts a single safe basename and rejects encoded
// separators as well as direct traversal markers.
func ValidBackupFilename(name string) bool {
	decoded, err := url.PathUnescape(name)
	if err != nil {
		return false
	}
	return name != "" && decoded != "" && decoded != "." && decoded != ".." && filepath.Base(decoded) == decoded && !strings.ContainsAny(decoded, `/\`) && !strings.ContainsRune(decoded, 0)
}
func validClock(value string) bool {
	if len(value) != 5 || value[2] != ':' {
		return false
	}
	h, e1 := time.Parse("15:04", value)
	return e1 == nil && h.Format("15:04") == value
}

type boundedReadCloser struct {
	io.ReadCloser
	remaining int64
}

func (b *boundedReadCloser) Read(p []byte) (int, error) {
	if b.remaining < 0 {
		return 0, errors.New("private backend response exceeded the file limit")
	}
	if int64(len(p)) > b.remaining+1 {
		p = p[:b.remaining+1]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	if b.remaining < 0 {
		return n, errors.New("private backend response exceeded the file limit")
	}
	return n, err
}

var _ Operations = (*Client)(nil)

var _ AISettingsOperations = (*Client)(nil)
