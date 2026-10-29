package webbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-domain/privateapi"
	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"
)

type Guard interface{ Check(context.Context) error }
type Server struct {
	Guard                                            Guard
	Pool                                             *pgxpool.Pool
	Runtime                                          *airuntime.Server
	Credentials                                      *credentialstore.Store
	Worker, AllowCreateAll                           bool
	BackupRoot, TelegramToken, ChatID, ServiceChatID string
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/web/v1/configuration/telegram", s.telegramConfiguration)
	mux.HandleFunc("PUT /internal/web/v1/configuration/telegram", s.telegramConfiguration)
	mux.HandleFunc("GET /internal/web/v1/configuration/operational", s.operationalConfiguration)
	mux.HandleFunc("PUT /internal/web/v1/configuration/operational", s.operationalConfiguration)
	mux.HandleFunc("GET /internal/web/v1/configuration/readiness", s.configurationReadiness)
	mux.HandleFunc("GET /internal/web/v1/credentials/{provider}", s.credential)
	mux.HandleFunc("PUT /internal/web/v1/credentials/{provider}", s.credential)
	mux.HandleFunc("DELETE /internal/web/v1/credentials/{provider}", s.credential)
	mux.HandleFunc("GET /internal/web/v1/ai/settings", s.aiSettingsGet)
	mux.HandleFunc("PUT /internal/web/v1/ai/settings", s.aiSettingsPut)
	mux.HandleFunc("POST /internal/web/v1/categories/ai-category-analysis", s.categoryAnalysis)
	mux.HandleFunc("POST /internal/web/v1/categories/invalidate-cache", s.invalidate)
	mux.HandleFunc("GET /internal/web/v1/telegram/settings", s.telegramSettings)
	mux.HandleFunc("GET /internal/web/v1/telegram/configuration", s.telegramSettings)
	mux.HandleFunc("POST /internal/web/v1/telegram/settings-updated", s.settingsUpdated)
	mux.HandleFunc("GET /internal/web/v1/backups/schedule", s.scheduleGet)
	mux.HandleFunc("PUT /internal/web/v1/backups/schedule", s.schedulePut)
	mux.HandleFunc("GET /internal/web/v1/backups", s.list)
	mux.HandleFunc("GET /internal/web/v1/backups/{filename}", s.download)
	mux.HandleFunc("GET /internal/web/v1/system/process-monitor", s.processMonitor)
	mux.HandleFunc("GET /internal/web/v1/system/info", s.systemInfo)
	mux.HandleFunc("POST /internal/web/v1/system/process-monitor/cleanup", s.processCleanup)
	mux.HandleFunc("POST /internal/web/v1/telegram/test", s.telegramTest)
	mux.HandleFunc("POST /internal/web/v1/migrations/run", s.runMigrations)
	mux.HandleFunc("POST /internal/web/v1/backups/upload", s.upload)
	mux.HandleFunc("POST /internal/web/v1/backups/restore", s.restore)
	mux.HandleFunc("POST /internal/web/v1/backups/cleanup", s.cleanup)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.Guard == nil || s.Guard.Check(r.Context()) != nil {
			reject(w, 503, "Executor instance is not the database owner")
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func timestamp() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000000") }
func emptyBody(r *http.Request) bool {
	var raw map[string]any
	return privateapi.DecodeJSON(readAll(r), &raw) == nil && raw != nil && len(raw) == 0
}
func decodeObject(data []byte, out any) error {
	if !bytes.HasPrefix(bytes.TrimSpace(data), []byte("{")) {
		return privateapi.ErrInvalidJSON
	}
	return privateapi.DecodeJSON(data, out)
}
func hasNullField(data []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return false
	}
	for _, v := range fields {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return true
		}
	}
	return false
}
func readAll(r *http.Request) []byte { b, _ := io.ReadAll(io.LimitReader(r.Body, 65537)); return b }
func (s *Server) unavailable(w http.ResponseWriter, r *http.Request) {
	reject(w, 503, "Operation is not implemented")
}
func (s *Server) categoryAnalysis(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Limit               *int     `json:"limit"`
		IncludeExamples     *bool    `json:"include_examples"`
		ConfidenceThreshold *float64 `json:"confidence_threshold"`
	}
	data := readAll(r)
	if decodeObject(data, &req) != nil || hasNullField(data) || (req.Limit != nil && (*req.Limit < 1 || *req.Limit > 50)) || (req.ConfidenceThreshold != nil && (*req.ConfidenceThreshold < 0 || *req.ConfidenceThreshold > 1)) {
		reject(w, 422, "Invalid request")
		return
	}
	if s.Worker {
		reject(w, 409, "AI category analysis is unavailable in queue-only web mode")
		return
	}
	if s.Pool == nil || s.Runtime == nil {
		reject(w, 503, "Executor runtime is unavailable")
		return
	}
	limit := 50
	if req.Limit != nil {
		limit = *req.Limit
	}
	include := true
	if req.IncludeExamples != nil {
		include = *req.IncludeExamples
	}
	result, err := s.analyzeCategories(r.Context(), limit, include)
	if err != nil {
		reject(w, 500, "Category analysis failed")
		return
	}
	write(w, 200, result)
}
func (s *Server) invalidate(w http.ResponseWriter, r *http.Request) {
	if !emptyBody(r) {
		reject(w, 422, "Invalid request")
		return
	}
	if s.Runtime == nil {
		reject(w, 503, "Executor runtime is unavailable")
		return
	}
	s.Runtime.InvalidateCategories()
	write(w, 200, map[string]any{"success": true})
}
func (s *Server) settingsUpdated(w http.ResponseWriter, r *http.Request) {
	if !emptyBody(r) {
		reject(w, 422, "Invalid request")
		return
	}
	write(w, 200, map[string]any{"success": true})
}
func (s *Server) telegramSettings(w http.ResponseWriter, r *http.Request) {
	if s.Pool == nil {
		reject(w, 503, "Domain database is unavailable")
		return
	}
	rows, err := s.Pool.Query(r.Context(), `SELECT key,CASE WHEN jsonb_typeof(value::jsonb)='string' THEN value::jsonb #>> '{}' ELSE NULL END FROM settings WHERE key IN ('telegram_chat_id','telegram_service_chat_id')`)
	if err != nil {
		reject(w, 503, "Domain database is unavailable")
		return
	}
	values := map[string]string{}
	for rows.Next() {
		var key string
		var value *string
		if err = rows.Scan(&key, &value); err != nil {
			break
		}
		if value != nil {
			values[key] = *value
		}
	}
	rows.Close()
	if err != nil || rows.Err() != nil {
		reject(w, 503, "Domain database is unavailable")
		return
	}
	chat, service := values["telegram_chat_id"], values["telegram_service_chat_id"]
	chatSource, serviceSource := "env", "db"
	if chat != "" {
		chatSource = "db"
	} else {
		chat = s.ChatID
	}
	if service == "" {
		if s.ServiceChatID != "" {
			service = s.ServiceChatID
			serviceSource = "env"
		} else {
			service = chat
			serviceSource = "fallback"
		}
	}
	var chatValue, serviceValue any
	if chat != "" {
		chatValue = chat
	}
	if service != "" {
		serviceValue = service
	}
	write(w, 200, map[string]any{"token_configured": s.TelegramToken != "", "chat_id": chatValue, "chat_id_source": chatSource, "service_chat_id": serviceValue, "service_chat_id_source": serviceSource})
}
func (s *Server) scheduleGet(w http.ResponseWriter, r *http.Request) {
	write(w, 200, map[string]any{"enabled": false, "frequency": "daily", "time": "02:00", "keep_count": 5, "last_backup": nil, "next_backup": nil, "status": "disabled"})
}

type schedule struct {
	Enabled   *bool  `json:"enabled"`
	Frequency string `json:"frequency"`
	Time      string `json:"time"`
	KeepCount int    `json:"keep_count"`
}

func (s *Server) schedulePut(w http.ResponseWriter, r *http.Request) {
	var v schedule
	if decodeObject(readAll(r), &v) != nil || v.Enabled == nil || v.KeepCount < 1 || v.KeepCount > 100 {
		reject(w, 422, "Invalid request")
		return
	}
	v.Frequency = strings.TrimSpace(v.Frequency)
	v.Time = strings.TrimSpace(v.Time)
	if v.Frequency != "daily" && v.Frequency != "weekly" && v.Frequency != "monthly" {
		reject(w, 422, "Invalid request")
		return
	}
	parsed, e := time.Parse("15:04", v.Time)
	if e != nil || parsed.Format("15:04") != v.Time {
		reject(w, 422, "Invalid request")
		return
	}
	write(w, 200, map[string]any{"message": "Backup schedule updated successfully", "settings": v, "timestamp": timestamp()})
}
func (s *Server) processCleanup(w http.ResponseWriter, r *http.Request) {
	if !emptyBody(r) {
		reject(w, 422, "Invalid request")
		return
	}
	reject(w, 409, "Browser process cleanup is owned by the Go collector")
}
func (s *Server) processMonitor(w http.ResponseWriter, r *http.Request) {
	if !s.Worker {
		write(w, 200, map[string]any{"status": map[string]any{"owner": "go-collector", "browser_status": "unknown"}, "timestamp": timestamp()})
		return
	}
	var healthy bool
	err := s.Pool.QueryRow(r.Context(), `SELECT status='running' AND heartbeat_at IS NOT NULL AND heartbeat_at >= (clock_timestamp() AT TIME ZONE 'UTC')-interval '20 seconds' FROM background_worker_status WHERE id=1`).Scan(&healthy)
	if err != nil {
		healthy = false
	}
	write(w, 200, map[string]any{"status": map[string]any{"running": healthy, "mode": "worker", "worker_available": healthy}, "timestamp": timestamp()})
}

func (s *Server) telegramTest(w http.ResponseWriter, r *http.Request) {
	if !emptyBody(r) {
		reject(w, 422, "Invalid request")
		return
	}
	if s.Runtime == nil {
		reject(w, 503, "Executor runtime is unavailable")
		return
	}
	message := "🧪 Test message from Evening News v2\n⏰ " + time.Now().UTC().Format("2006-01-02 15:04:05") + " UTC"
	result, err := s.Runtime.SendServiceTest(r.Context(), message)
	if err != nil {
		reject(w, 500, "Telegram configuration incomplete")
		return
	}
	success := result.State == telegramdelivery.Sent
	var detail any
	if !success {
		parts := []string{}
		if result.StatusCode >= 100 && result.StatusCode <= 599 {
			parts = append(parts, fmt.Sprintf("HTTP %d", result.StatusCode))
		}
		if result.ErrorCode >= 100 && result.ErrorCode <= 599 && result.ErrorCode != result.StatusCode {
			parts = append(parts, fmt.Sprintf("Telegram %d", result.ErrorCode))
		}
		if result.State == telegramdelivery.Rejected {
			parts = append(parts, "delivery rejected")
		} else {
			parts = append(parts, "delivery unknown")
		}
		detail = strings.Join(parts, ": ")
	}
	messageText := "Ошибка отправки"
	if success {
		messageText = "Тест отправлен"
	}
	write(w, 200, map[string]any{"success": success, "message": messageText, "error": detail, "timestamp": timestamp()})
}

func (s *Server) runMigrations(w http.ResponseWriter, r *http.Request) {
	if !emptyBody(r) {
		reject(w, 422, "Invalid request")
		return
	}
	if s.Pool == nil {
		reject(w, 503, "Domain database is unavailable")
		return
	}
	result, err := migration.Run(r.Context(), s.Pool, s.AllowCreateAll, false)
	if err != nil {
		reject(w, 500, "Migration failed")
		return
	}
	write(w, 200, map[string]any{"message": "Migrations completed", "result": result, "timestamp": timestamp()})
}

func (s *Server) systemInfo(w http.ResponseWriter, r *http.Request) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs(".", &fs); err != nil {
		reject(w, 500, "System information unavailable")
		return
	}
	total := fs.Blocks * uint64(fs.Bsize)
	used := (fs.Blocks - fs.Bfree) * uint64(fs.Bsize)
	free := fs.Bavail * uint64(fs.Bsize)
	commit := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, item := range info.Settings {
			if item.Key == "vcs.revision" && len(item.Value) >= 7 {
				commit = item.Value[:7]
			}
		}
	}
	write(w, 200, map[string]any{"python_version": "not used", "go_version": runtime.Version(), "git_commit": commit, "disk_usage": map[string]any{"total": total, "used": used, "free": free}, "timestamp": timestamp()})
}
