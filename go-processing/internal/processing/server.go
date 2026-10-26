package processing

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

const maxEnvelopeBytes = 8192

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89aAbB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)

type Runner interface {
	Run(context.Context, store.Fence) (map[string]any, error)
}
type Config struct{ Token string }
type Handler struct {
	DB     *store.Store
	Runner Runner
	cfg    Config
	parent context.Context
	mu     sync.Mutex
	active map[string]context.CancelFunc
	wg     sync.WaitGroup
	closed bool
}

func NewHandler(db *store.Store, runner Runner, cfg Config) *Handler {
	return NewHandlerWithContext(context.Background(), db, runner, cfg)
}

func NewHandlerWithContext(parent context.Context, db *store.Store, runner Runner, cfg Config) *Handler {
	if parent == nil {
		parent = context.Background()
	}
	return &Handler{DB: db, Runner: runner, cfg: cfg, parent: parent, active: map[string]context.CancelFunc{}}
}

// Close cancels active background runs and waits for their bounded cleanup.
// Call it after HTTP serving has stopped and before closing the database pool.
func (h *Handler) Close(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	for _, cancel := range h.active {
		cancel()
	}
	h.mu.Unlock()
	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (h *Handler) Routes() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /healthz", h.health)
	m.HandleFunc("POST /internal/v1/processing/runs", h.create)
	m.HandleFunc("GET /internal/v1/processing/runs/{run_id}", h.get)
	m.HandleFunc("POST /internal/v1/processing/runs/{run_id}/cancel", h.cancel)
	return m
}
func (h *Handler) authorized(r *http.Request) bool {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	want := h.cfg.Token
	return want != "" && got != r.Header.Get("Authorization") && len(got) == len(want) && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.DB.Pool.Ping(ctx); err != nil {
		write(w, 503, map[string]string{"status": "unavailable"})
		return
	}
	var ready bool
	err := h.DB.Pool.QueryRow(ctx, `SELECT to_regclass('article_processing_runs') IS NOT NULL AND to_regclass('article_processing_items') IS NOT NULL AND EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=current_schema() AND p.proname='fence_article_processing_attempt')`).Scan(&ready)
	if err != nil || !ready {
		write(w, 503, map[string]string{"status": "unavailable"})
		return
	}
	write(w, 200, map[string]string{"status": "ok"})
}
func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		write(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxEnvelopeBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		write(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var e store.Envelope
	if dec.Decode(&e) != nil || dec.Decode(new(any)) != io.EOF || !validEnvelope(e) {
		write(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	f := store.NewFence(e)
	if existing, found, err := h.DB.ExistingRun(r.Context(), f); err != nil {
		write(w, 500, map[string]string{"error": "run_lookup_failed"})
		return
	} else if found {
		if existing["id"] != f.RunID || existing["envelope_sha256"] != f.EnvelopeSHA256 {
			write(w, 409, map[string]string{"error": "envelope_conflict"})
			return
		}
		if existing["state"] == "running" {
			h.launch(f)
		}
		write(w, 200, existing)
		return
	}
	tx, err := h.DB.Pool.Begin(r.Context())
	if err != nil {
		write(w, 503, map[string]string{"error": "database_unavailable"})
		return
	}
	defer func() {
		rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer rollbackCancel()
		_ = tx.Rollback(rollbackCtx)
	}()
	if _, _, err = h.DB.FenceTx(r.Context(), tx, f); err != nil {
		write(w, 409, map[string]string{"error": "stale_fence"})
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		write(w, 503, map[string]string{"error": "database_unavailable"})
		return
	}
	if !h.launch(f) {
		w.Header().Set("Retry-After", "1")
		write(w, 429, map[string]string{"error": "busy"})
		return
	}
	status, err := h.DB.GetRun(r.Context(), f.RunID)
	if err != nil {
		write(w, 500, map[string]string{"error": "run_lookup_failed"})
		return
	}
	write(w, 200, status)
}

func (h *Handler) launch(f store.Fence) bool {
	h.mu.Lock()
	if _, ok := h.active[f.RunID]; ok {
		h.mu.Unlock()
		return true
	}
	if h.closed {
		h.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithDeadline(h.parent, f.Envelope.DeadlineAt)
	h.active[f.RunID] = cancel
	h.wg.Add(1)
	h.mu.Unlock()
	go func() {
		defer h.wg.Done()
		defer func() { cancel(); h.mu.Lock(); delete(h.active, f.RunID); h.mu.Unlock() }()
		lockCtx, lockCancel := context.WithTimeout(ctx, 5*time.Second)
		release, acquired, lockErr := h.DB.AcquireRunLock(lockCtx, f.RunID)
		lockCancel()
		if lockErr != nil {
			log.Printf("article processing failed phase=run_lock error_code=run_lock_failed")
			h.failBounded(f, "run_lock_failed")
			return
		}
		if !acquired {
			return
		}
		defer release()
		receipt, runErr := h.Runner.Run(ctx, f)
		if runErr != nil {
			log.Printf("article processing failed phase=%s error_code=processing_failed", diagnosticPhase(runErr))
			h.failBounded(f, "processing_failed")
			return
		}
		finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := h.DB.FinishRun(finishCtx, f, receipt)
		finishCancel()
		if err != nil {
			log.Printf("article processing failed phase=finalize error_code=finalization_failed")
			h.failBounded(f, "finalization_failed")
		}
	}()
	return true
}
func (h *Handler) failBounded(f store.Fence, code string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.DB.FailRun(ctx, f, code); err != nil {
		log.Printf("article processing failed phase=record_failure error_code=receipt_update_failed")
	}
}
func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		write(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	id := r.PathValue("run_id")
	if !uuidPattern.MatchString(id) {
		write(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	status, err := h.DB.GetRun(r.Context(), id)
	if err != nil {
		write(w, 404, map[string]string{"error": "run_not_found"})
		return
	}
	write(w, 200, status)
}
func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		write(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	id := r.PathValue("run_id")
	if !uuidPattern.MatchString(id) {
		write(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	h.mu.Lock()
	if cancel := h.active[id]; cancel != nil {
		cancel()
	}
	h.mu.Unlock()
	if err := h.DB.Cancel(r.Context(), id); err != nil {
		write(w, 503, map[string]string{"error": "cancel_failed"})
		return
	}
	status, err := h.DB.GetRun(r.Context(), id)
	if err != nil {
		write(w, 404, map[string]string{"error": "run_not_found"})
		return
	}
	write(w, 200, status)
}
func validEnvelope(e store.Envelope) bool {
	return e.Version == 1 && uuidPattern.MatchString(e.JobID) && e.AttemptGeneration > 0 && uuidPattern.MatchString(e.OwnerID) && e.Generation > 0 && e.BackendPID > 0 && uuidPattern.MatchString(e.ExecutorInstanceID) && e.DeadlineAt.After(time.Now())
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
