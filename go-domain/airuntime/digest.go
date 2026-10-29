package airuntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/digestworkflow"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type digestEnvelope struct {
	Version    int    `json:"version"`
	JobID      string `json:"job_id"`
	Attempt    int    `json:"attempt_generation"`
	OwnerID    string `json:"owner_id"`
	Generation int    `json:"generation"`
	BackendPID int    `json:"backend_pid"`
	ExecutorID string `json:"executor_instance_id"`
	Deadline   string `json:"deadline_at"`
}
type digestInput struct {
	Operation          string          `json:"operation"`
	OperationID        string          `json:"operation_id"`
	DomainInstanceID   string          `json:"domain_instance_id"`
	Envelope           digestEnvelope  `json:"envelope"`
	Fingerprint        string          `json:"input_fingerprint"`
	Input              json.RawMessage `json:"input"`
	Kind               string          `json:"kind"`
	Payload            json.RawMessage `json:"payload"`
	ExecutorBackendPID int             `json:"executor_backend_pid"`
}
type digestRequest struct {
	commonRequest
	Input digestInput `json:"input"`
}
type digestReceipt struct {
	input   digestInput
	state   string
	result  map[string]any
	code    string
	expires time.Time
}
type digestRegistry struct {
	mu       sync.Mutex
	entries  map[string]*digestReceipt
	active   bool
	prepared digestworkflow.PreparedCache
}

func digestKey(v digestInput) string {
	return fmt.Sprintf("%s/%d/%s/%s", v.Envelope.JobID, v.Envelope.Attempt, v.Operation, v.OperationID)
}
func digestResponse(v *digestReceipt) map[string]any {
	return map[string]any{"job_id": v.input.Envelope.JobID, "attempt_generation": v.input.Envelope.Attempt, "operation": v.input.Operation, "operation_id": v.input.OperationID, "domain_instance_id": v.input.DomainInstanceID, "input_fingerprint": v.input.Fingerprint, "state": v.state, "result": v.result, "error_code": v.code}
}
func validDigest(v digestInput) (time.Time, bool) {
	e := v.Envelope
	deadline, err := time.Parse(time.RFC3339Nano, e.Deadline)
	if err != nil || !deadline.After(time.Now()) || deadline.After(time.Now().Add(24*time.Hour)) || deadline.Nanosecond()%1000 != 0 || e.Version != 1 || !uuidPattern.MatchString(e.JobID) || !uuidPattern.MatchString(e.OwnerID) || !uuidPattern.MatchString(e.ExecutorID) || !uuidPattern.MatchString(v.OperationID) || !uuidPattern.MatchString(v.DomainInstanceID) || e.Attempt <= 0 || e.Attempt > 2147483647 || e.Generation <= 0 || e.Generation > 2147483647 || e.BackendPID <= 0 || e.BackendPID > 2147483647 || v.ExecutorBackendPID <= 0 || v.ExecutorBackendPID > 2147483647 || len(v.Payload) > 16384 || !json.Valid(v.Payload) {
		return deadline, false
	}
	switch v.Operation {
	case "summary_status", "summaries_generate", "digest_prepare", "digest_publish":
	default:
		return deadline, false
	}
	var input map[string]any
	if json.Unmarshal(v.Input, &input) != nil || input == nil {
		return deadline, false
	}
	if v.Operation == "digest_publish" {
		token, ok := input["prepared_token"].(string)
		if !ok || len(input) != 1 || !uuidPattern.MatchString(token) {
			return deadline, false
		}
	} else if len(input) != 0 {
		return deadline, false
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		return deadline, false
	}
	h := sha256.Sum256(canonical)
	if hex.EncodeToString(h[:]) != v.Fingerprint {
		return deadline, false
	}
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(v.Payload))
	decoder.UseNumber()
	if decoder.Decode(&payload) != nil || payload == nil {
		return deadline, false
	}
	if v.Kind == "summaries_generate" {
		if v.Operation != "summaries_generate" || len(payload) != 0 {
			return deadline, false
		}
	} else if v.Kind == "news_digest" || v.Kind == "telegram_digest" {
		allowed := map[string]bool{"schedule_id": true, "scheduled_for": true, "timeout_seconds": true}
		if v.Kind == "news_digest" {
			allowed["run_processing"] = true
			allowed["send_telegram"] = true
		}
		for key, val := range payload {
			if !allowed[key] {
				return deadline, false
			}
			switch key {
			case "run_processing", "send_telegram":
				b, ok := val.(bool)
				if !ok || key == "send_telegram" && !b {
					return deadline, false
				}
			case "schedule_id", "timeout_seconds":
				n, ok := val.(json.Number)
				if !ok {
					return deadline, false
				}
				i, e := strconv.ParseInt(string(n), 10, 64)
				if e != nil || i < 1 || (key == "timeout_seconds" && i > 86400) {
					return deadline, false
				}
			case "scheduled_for":
				if _, ok := val.(string); !ok {
					return deadline, false
				}
			}
		}
		_, a := payload["schedule_id"]
		_, b := payload["scheduled_for"]
		if a != b {
			return deadline, false
		}
	} else {
		return deadline, false
	}
	return deadline, true
}
func (s *Server) digestAccept(w http.ResponseWriter, r *http.Request) {
	var req digestRequest
	if !decodeTyped(r, &req) {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	_, cancel, ok := s.contextFor(r, w, req.commonRequest)
	if !ok {
		return
	}
	defer cancel()
	deadline, ok := validDigest(req.Input)
	if !ok {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	reg := s.digest
	key := digestKey(req.Input)
	reg.mu.Lock()
	for k, v := range reg.entries {
		if v.state != "running" && !v.expires.After(time.Now()) {
			delete(reg.entries, k)
		}
	}
	if old := reg.entries[key]; old != nil {
		a, _ := json.Marshal(old.input)
		b, _ := json.Marshal(req.Input)
		if string(a) != string(b) {
			reg.mu.Unlock()
			writeJSON(w, 409, errorEnvelope("operation_conflict", 409))
			return
		}
		out := digestResponse(old)
		reg.mu.Unlock()
		writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": out})
		return
	}
	if reg.active || len(reg.entries) >= 512 {
		reg.mu.Unlock()
		writeJSON(w, 503, errorEnvelope("resource_limit", 503))
		return
	}
	receipt := &digestReceipt{input: req.Input, state: "running", expires: deadline.Add(5 * time.Minute)}
	reg.entries[key] = receipt
	reg.active = true
	out := digestResponse(receipt)
	reg.mu.Unlock()
	go s.runDigest(key, receipt, deadline)
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": out})
}
func (s *Server) digestStatus(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	instance := r.URL.Query().Get("domain_instance_id")
	reg := s.digest
	reg.mu.Lock()
	v := reg.entries[key]
	if v == nil || v.input.DomainInstanceID != instance || !v.expires.After(time.Now()) {
		reg.mu.Unlock()
		writeJSON(w, 404, errorEnvelope("operation_missing", 404))
		return
	}
	out := digestResponse(v)
	reg.mu.Unlock()
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": out})
}
func (s *Server) runDigest(key string, receipt *digestReceipt, deadline time.Time) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	go func() {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		}
	}()
	v := receipt.input
	e := v.Envelope
	fence := authority.Fence{Pool: s.db, JobID: e.JobID, OwnerID: e.OwnerID, ExecutorInstanceID: e.ExecutorID, Kind: v.Kind, AttemptGeneration: e.Attempt, Generation: e.Generation, WorkerBackendPID: e.BackendPID, ExecutorBackendPID: v.ExecutorBackendPID, Deadline: deadline, Payload: v.Payload}
	var result map[string]any
	var err error
	if s.db == nil {
		err = authority.ErrStale
	} else {
		result, err = s.executeDigest(ctx, v, fence)
	}
	reg := s.digest
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.active = false
	if err != nil {
		receipt.state = "failed"
		receipt.code = digestErrorCode(err)
		if v.Operation == "digest_publish" {
			receipt.code = "external_outcome_unknown"
		}
	} else {
		receipt.state = "succeeded"
		receipt.result = result
	}
}

func digestErrorCode(err error) string {
	var providerErr *ai.Error
	if errors.As(err, &providerErr) {
		switch providerErr.Kind {
		case "timeout", "cancelled":
			return "editorial_provider_timeout"
		case "request_too_large_or_invalid", "response_too_large":
			return "editorial_resource_limit"
		case "incomplete", "refusal", "invalid_response", "invalid_json", "invalid_schema":
			return "editorial_invalid_result"
		default:
			return "editorial_provider_unavailable"
		}
	}
	if errors.Is(err, ErrAISettingsInvalid) {
		return "editorial_provider_unavailable"
	}

	switch {
	case errors.Is(err, authority.ErrStale):
		return "stale_fence"
	case errors.Is(err, context.DeadlineExceeded):
		return "editorial_provider_timeout"
	case errors.Is(err, digestworkflow.ErrNotReady):
		return "editorial_not_ready"
	case errors.Is(err, digestworkflow.ErrLimit):
		return "editorial_resource_limit"
	case errors.Is(err, digestworkflow.ErrInvalid):
		return "editorial_invalid_result"
	case errors.Is(err, digestworkflow.ErrPrepared):
		return "prepared_digest_missing"
	}
	switch err.Error() {
	case "provider_summary_missing":
		return "editorial_not_ready"
	case "provider_timeout", "provider_cancelled":
		return "editorial_provider_timeout"
	case "provider_not_configured", "provider_invalid_configuration", "provider_transport_error", "provider_rejected_credentials", "provider_rate_limited", "provider_unavailable":
		return "editorial_provider_unavailable"
	case "provider_request_too_large", "provider_response_too_large", "render_story_too_large":
		return "editorial_resource_limit"
	case "provider_invalid_response", "provider_empty_response", "provider_incomplete_response", "provider_invalid_editorial_json", "provider_invalid_editorial_edition":
		return "editorial_invalid_result"
	}
	return "domain_operation_failed"
}
