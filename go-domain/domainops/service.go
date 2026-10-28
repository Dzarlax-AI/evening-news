// Package domainops implements bounded private operation receipts.
package domainops

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/privateapi"
	"github.com/jackc/pgx/v5/pgxpool"
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var shaRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

type Guard interface{ Check(context.Context) error }
type ContextReader interface {
	Get(context.Context, string, int) (authority.ExecutionContext, error)
}
type MediaRefresher interface {
	Refresh(context.Context, authority.Fence, int64) (bool, error)
}
type SourceProber interface {
	Test(context.Context, authority.Fence, int64) (bool, error)
}
type BackupCreator interface {
	Create(context.Context, authority.Fence) (int64, error)
}
type Request struct {
	Version          int                `json:"version"`
	Operation        string             `json:"operation"`
	OperationID      string             `json:"operation_id"`
	DomainInstanceID string             `json:"domain_instance_id"`
	Envelope         authority.Envelope `json:"envelope"`
	InputFingerprint string             `json:"input_fingerprint"`
	Input            json.RawMessage    `json:"input"`
}
type record struct {
	request Request
	state   string
	result  map[string]any
	code    *string
	expires time.Time
	size    int
	cancel  context.CancelFunc
}
type Service struct {
	Token, InstanceID string
	Guard             Guard
	Authority         ContextReader
	Pool              *pgxpool.Pool
	Runtime           *airuntime.Server
	Media             MediaRefresher
	Source            SourceProber
	Backup            BackupCreator
	mu                sync.Mutex
	records           map[string]*record
	bytes             int
	active            int
	draining          bool
	wg                sync.WaitGroup
	root              context.Context
}

func New(ctx context.Context, token, instance string, g Guard, a ContextReader, pool *pgxpool.Pool, runtime *airuntime.Server) *Service {
	return &Service{Token: token, InstanceID: instance, Guard: g, Authority: a, Pool: pool, Runtime: runtime, records: map[string]*record{}, root: ctx}
}

// Status reports a snapshot of active operations and drain state.
func (s *Service) Status() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active, !s.draining
}

func key(job string, attempt int, op, id string) string {
	return fmt.Sprintf("%s/%d/%s/%s", job, attempt, op, id)
}
func response(rec *record) map[string]any {
	return map[string]any{"version": 1, "job_id": rec.request.Envelope.JobID, "attempt_generation": rec.request.Envelope.AttemptGeneration, "operation": rec.request.Operation, "operation_id": rec.request.OperationID, "domain_instance_id": rec.request.DomainInstanceID, "input_fingerprint": rec.request.InputFingerprint, "state": rec.state, "result": rec.result, "error_code": rec.code}
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func reject(w http.ResponseWriter, status int, reason string) {
	write(w, status, map[string]string{"detail": reason})
}
func (s *Service) auth(w http.ResponseWriter, r *http.Request) bool {
	if s.Token == "" {
		reject(w, 503, "Executor authentication is unavailable")
		return false
	}
	expected, given := "Bearer "+s.Token, r.Header.Get("Authorization")
	if len(expected) != len(given) || subtle.ConstantTimeCompare([]byte(expected), []byte(given)) != 1 {
		reject(w, 401, "Unauthorized")
		return false
	}
	if s.Guard == nil || s.Guard.Check(r.Context()) != nil {
		reject(w, 503, "Executor instance is not the database owner")
		return false
	}
	return true
}
func (s *Service) prune(now time.Time) {
	for k, v := range s.records {
		if v.cancel == nil && !v.expires.After(now) {
			s.bytes -= v.size
			delete(s.records, k)
		}
	}
}
func supported(op string) bool {
	return op == "summary_status" || op == "summaries_generate" || op == "digest_prepare" || op == "digest_publish" || op == "stats_publish" || op == "media_refresh" || op == "source_test" || op == "backup"
}
func (s *Service) Post(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 65537))
	if err != nil {
		reject(w, 400, "Request body is invalid")
		return
	}
	if len(raw) > 65536 {
		reject(w, 413, "Request body exceeds limit")
		return
	}
	var req Request
	var fields map[string]json.RawMessage
	if privateapi.DecodeJSON(raw, &req) != nil || json.Unmarshal(raw, &fields) != nil || len(fields) != 7 || req.Version != 1 || !uuidRE.MatchString(req.OperationID) || !uuidRE.MatchString(req.DomainInstanceID) || !shaRE.MatchString(req.InputFingerprint) {
		reject(w, 422, "Domain operation request is invalid")
		return
	}
	if !supported(req.Operation) {
		reject(w, 422, "domain operation is unsupported")
		return
	}
	deadline, err := req.Envelope.Deadline()
	if err != nil || !deadline.After(time.Now()) || deadline.After(time.Now().Add(24*time.Hour)) {
		reject(w, 422, "Domain operation request is invalid")
		return
	}
	var input map[string]any
	if json.Unmarshal(req.Input, &input) != nil || input == nil {
		reject(w, 422, "Domain operation request is invalid")
		return
	}
	canonical, err := canonicalDomainJSON(req.Input)
	if err != nil || len(canonical) > 16384 {
		reject(w, 422, "Domain operation request is invalid")
		return
	}
	sum := sha256.Sum256(canonical)
	if hex.EncodeToString(sum[:]) != req.InputFingerprint {
		reject(w, 422, "Domain operation request is invalid")
		return
	}
	if req.DomainInstanceID != s.InstanceID || r.Header.Get("X-Domain-Instance-Id") != s.InstanceID {
		reject(w, 409, "domain instance does not match this service")
		return
	}
	k := key(req.Envelope.JobID, req.Envelope.AttemptGeneration, req.Operation, req.OperationID)
	s.mu.Lock()
	s.prune(time.Now())
	if old := s.records[k]; old != nil {
		if old.request.InputFingerprint != req.InputFingerprint || old.request.DomainInstanceID != req.DomainInstanceID || !reflect.DeepEqual(old.request.Envelope, req.Envelope) {
			s.mu.Unlock()
			reject(w, 409, "operation key conflicts with its receipt")
			return
		}
		if !old.expires.After(time.Now()) {
			s.mu.Unlock()
			reject(w, 409, "operation receipt expired; do not retry")
			return
		}
		out := response(old)
		s.mu.Unlock()
		write(w, 202, out)
		return
	}
	if s.draining || s.active >= 1 || len(s.records) >= 512 || s.bytes+len(raw) > 4*1024*1024 {
		s.mu.Unlock()
		reject(w, 429, "Domain operation capacity is full")
		return
	}
	ctx, cancel := context.WithDeadline(s.root, deadline)
	rec := &record{request: req, state: "accepted", expires: deadline.Add(5 * time.Minute), size: len(raw), cancel: cancel}
	s.records[k] = rec
	s.bytes += len(raw)
	s.active++
	s.wg.Add(1)
	out := response(rec)
	s.mu.Unlock()
	go s.run(ctx, k, rec)
	write(w, 202, out)
}
func (s *Service) Get(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	if r.Header.Get("X-Domain-Instance-Id") != s.InstanceID {
		reject(w, 409, "Domain instance does not match this service")
		return
	}
	attempt, err := strconv.Atoi(r.PathValue("attempt"))
	if err != nil || attempt <= 0 || !uuidRE.MatchString(r.PathValue("job")) || !uuidRE.MatchString(r.PathValue("id")) {
		reject(w, 422, "Domain operation request is invalid")
		return
	}
	s.mu.Lock()
	s.prune(time.Now())
	rec := s.records[key(r.PathValue("job"), attempt, r.PathValue("operation"), r.PathValue("id"))]
	if rec == nil || !rec.expires.After(time.Now()) {
		s.mu.Unlock()
		reject(w, 404, "Domain operation receipt not found")
		return
	}
	out := response(rec)
	s.mu.Unlock()
	write(w, 200, out)
}
func samePayload(a, b json.RawMessage) bool {
	var x, y any
	return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && reflect.DeepEqual(x, y)
}
func (s *Service) getContext(ctx context.Context, req Request) (authority.ExecutionContext, error) {
	v, err := s.Authority.Get(ctx, req.Envelope.JobID, req.Envelope.AttemptGeneration)
	if err != nil || !v.Envelope.Matches(req.Envelope) {
		return authority.ExecutionContext{}, authority.ErrAuthorityUnavailable
	}
	return v, nil
}
func (s *Service) run(ctx context.Context, k string, rec *record) {
	defer s.wg.Done()
	req := rec.request
	s.mu.Lock()
	rec.state = "running"
	s.mu.Unlock()
	current, err := s.getContext(ctx, req)
	var result map[string]any
	if err == nil {
		f := current.Fence(s.Pool)
		err = f.Check(ctx)
		if err == nil {
			if req.Operation == "backup" {
				var input map[string]any
				if current.Kind != "backup" || json.Unmarshal(req.Input, &input) != nil || len(input) != 0 || s.Backup == nil {
					err = errors.New("invalid backup operation")
				} else {
					var size int64
					size, err = s.Backup.Create(ctx, f)
					result = map[string]any{"backup_bytes": size}
				}
			} else if req.Operation == "source_test" {
				id, e := sourceID(req.Input, current)
				if e != nil || s.Source == nil {
					err = errors.New("invalid source probe")
				} else {
					var ok bool
					ok, err = s.Source.Test(ctx, f, id)
					result = map[string]any{"success": ok}
				}
			} else if req.Operation == "media_refresh" {
				id, e := mediaArticleID(req.Input, current)
				if e != nil || s.Media == nil {
					err = errors.New("invalid media refresh")
				} else {
					var refreshed bool
					refreshed, err = s.Media.Refresh(ctx, f, id)
					result = map[string]any{"refreshed": refreshed}
				}
			} else if req.Operation == "stats_publish" {
				result, err = publishStats(ctx, f, req.Input)
			} else {
				result, err = s.Runtime.ExecuteDigest(ctx, airuntime.DigestInput{Operation: req.Operation, OperationID: req.OperationID, DomainInstanceID: req.DomainInstanceID, Fingerprint: req.InputFingerprint, Input: req.Input, Envelope: req.Envelope, Fence: f})
			}
		}
	}
	if err == nil {
		var after authority.ExecutionContext
		after, err = s.getContext(ctx, req)
		if err == nil && (after.ExecutorBackendPID != current.ExecutorBackendPID || after.Kind != current.Kind || !samePayload(after.Payload, current.Payload)) {
			err = authority.ErrStale
		}
		if err == nil {
			err = after.Fence(s.Pool).Check(ctx)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	defer rec.cancel()
	rec.cancel = nil
	s.active--
	if err != nil {
		code := airuntime.DigestErrorCode(err)
		if errors.Is(err, authority.ErrAuthorityUnavailable) {
			code = "authority_unavailable"
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			code = "stale_fence"
		}
		if req.Operation == "digest_publish" {
			code = "external_outcome_unknown"
		}
		rec.code = &code
		rec.state = "failed"
		return
	}
	bytes, encodeErr := json.Marshal(result)
	if encodeErr != nil || len(bytes) > 4096 || s.bytes+len(bytes) > 4*1024*1024 {
		code := "domain_operation_failed"
		rec.code = &code
		rec.state = "failed"
		return
	}
	rec.size += len(bytes)
	s.bytes += len(bytes)
	rec.result = result
	rec.state = "succeeded"
}
func (s *Service) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	for _, r := range s.records {
		if r.cancel != nil {
			r.cancel()
		}
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Service) Active() int { s.mu.Lock(); defer s.mu.Unlock(); return s.active }
