// Package jobs defines the language-neutral acceptance and observation API for durable jobs.
package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	MaxPendingJobs                 = 32
	MaxSelectedIDs                 = 100
	MaxPayloadBytes                = 4096
	MaxIdempotencyAliasesPerJob    = 32
	MaxSchedulerOccurrenceReceipts = 100000
	HeartbeatStaleSeconds          = 20
)

var (
	ErrQueueFull           = errors.New("background queue is full")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with existing work")
	ErrWorkerUnavailable   = errors.New("background worker unavailable")
	ErrNotFound            = errors.New("job not found")
	keyPattern             = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:+-]{0,199}$`)
	schedulePattern        = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	occurrenceTimePattern  = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z$`)
)

type SubmitCommand struct {
	Kind           string
	Payload        map[string]any
	IdempotencyKey string
}
type SubmitResult struct {
	Job     Job
	Created bool
}
type OccurrenceCommand struct {
	Version          int
	OccurrenceID     string
	ScheduleKey      string
	ScheduleRevision int
	ScheduledFor     string
	Operation        string
	Payload          map[string]any
	IdempotencyKey   string
	WorkerMode       bool
	DispatchMode     string
	ReceiptLimit     int
}
type AcceptedOccurrence struct {
	OccurrenceID string
	JobID        string
	Created      bool
}
type Job struct {
	ID              string         `json:"job_id"`
	Kind            string         `json:"kind"`
	Status          string         `json:"status"`
	CreatedAt       *time.Time     `json:"created_at"`
	StartedAt       *time.Time     `json:"started_at"`
	FinishedAt      *time.Time     `json:"finished_at"`
	Result          map[string]any `json:"result"`
	ErrorCode       *string        `json:"error_code"`
	WorkerAvailable bool           `json:"worker_available"`
}
type WorkerStatus struct {
	Generation  int64
	Status      string
	HeartbeatAt *time.Time
	Healthy     bool
}

type OccurrenceMetadata struct {
	ScheduleKey      string
	ScheduleRevision int
	OccurrenceID     string
	ScheduledFor     time.Time
}
type Repository interface {
	Submit(context.Context, SubmitCommand, map[string]any, string) (SubmitResult, error)
	Get(context.Context, string) (Job, error)
	Status(context.Context) (WorkerStatus, error)
	AcceptOccurrence(context.Context, OccurrenceCommand, map[string]any, OccurrenceMetadata, string, int64) (AcceptedOccurrence, error)
}
type Service struct{ Repository Repository }

func New(repo Repository) *Service { return &Service{Repository: repo} }

func (s *Service) Submit(ctx context.Context, cmd SubmitCommand) (SubmitResult, error) {
	payload, err := ValidatePayload(cmd.Kind, cmd.Payload)
	if err != nil {
		return SubmitResult{}, err
	}
	if !keyPattern.MatchString(cmd.IdempotencyKey) {
		return SubmitResult{}, errors.New("idempotency key must be a safe 1-200 character token")
	}
	sum := sha256.Sum256([]byte(cmd.IdempotencyKey))
	return s.Repository.Submit(ctx, cmd, payload, hex.EncodeToString(sum[:]))
}
func (s *Service) Get(ctx context.Context, id string) (Job, error)  { return s.Repository.Get(ctx, id) }
func (s *Service) Status(ctx context.Context) (WorkerStatus, error) { return s.Repository.Status(ctx) }

func (s *Service) AcceptOccurrence(ctx context.Context, cmd OccurrenceCommand) (AcceptedOccurrence, error) {
	if err := ValidateOccurrence(cmd); err != nil {
		return AcceptedOccurrence{}, err
	}
	payload, err := ValidatePayload(cmd.Operation, cmd.Payload)
	if err != nil {
		return AcceptedOccurrence{}, errors.New("invalid scheduled job envelope")
	}
	if _, exists := payload["schedule_id"]; exists {
		return AcceptedOccurrence{}, errors.New("invalid scheduled job envelope")
	}
	if _, exists := payload["scheduled_for"]; exists {
		return AcceptedOccurrence{}, errors.New("invalid scheduled job envelope")
	}
	parsed, _ := time.Parse(time.RFC3339Nano, cmd.ScheduledFor)
	due := parsed.UTC().Truncate(time.Microsecond)
	canonical, err := CanonicalJSON(map[string]any{"version": cmd.Version, "occurrence_id": cmd.OccurrenceID, "schedule_key": cmd.ScheduleKey, "schedule_revision": cmd.ScheduleRevision, "scheduled_for": cmd.ScheduledFor, "operation": cmd.Operation, "payload": cmd.Payload})
	if err != nil {
		return AcceptedOccurrence{}, errors.New("invalid scheduled job envelope")
	}
	hash := sha256.Sum256(canonical)
	return s.Repository.AcceptOccurrence(ctx, cmd, payload, OccurrenceMetadata{cmd.ScheduleKey, cmd.ScheduleRevision, cmd.OccurrenceID, due}, hex.EncodeToString(hash[:]), int64(minPositive(cmd.ReceiptLimit, MaxSchedulerOccurrenceReceipts)))
}
func minPositive(a, b int) int {
	if a <= 0 {
		a = b
	}
	if a > b {
		return b
	}
	return a
}

func ValidateOccurrence(c OccurrenceCommand) error {
	if c.Version != 1 || !isCanonicalUUID(c.OccurrenceID) || c.IdempotencyKey != c.OccurrenceID || !schedulePattern.MatchString(c.ScheduleKey) || c.ScheduleRevision < 1 || !occurrenceTimePattern.MatchString(c.ScheduledFor) {
		return errors.New("invalid scheduled job envelope")
	}
	if _, err := time.Parse(time.RFC3339Nano, c.ScheduledFor); err != nil {
		return errors.New("invalid scheduled job envelope")
	}
	switch c.Operation {
	case "news_processing", "news_digest", "telegram_digest", "backup", "reprocess_failed":
	default:
		return errors.New("invalid scheduled job envelope")
	}
	if c.Payload == nil {
		return errors.New("invalid scheduled job envelope")
	}
	return nil
}
func isCanonicalUUID(s string) bool {
	if len(s) != 36 || strings.ToLower(s) != s || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return false
	}
	_, err := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return err == nil
}

func ValidatePayload(kind string, payload map[string]any) (map[string]any, error) {
	allowedByKind := map[string]map[string]bool{
		"news_processing":    keys("schedule_id", "scheduled_for", "timeout_seconds"),
		"news_digest":        keys("schedule_id", "scheduled_for", "run_processing", "send_telegram", "timeout_seconds"),
		"telegram_digest":    keys("schedule_id", "scheduled_for", "timeout_seconds"),
		"backup":             keys("schedule_id", "scheduled_for", "timeout_seconds"),
		"reprocess_failed":   keys("schedule_id", "scheduled_for", "limit", "dry_run", "timeout_seconds"),
		"article_reprocess":  keys("article_ids", "force", "reextract_media"),
		"summaries_generate": {}, "source_test": keys("source_id"),
	}
	allowed, ok := allowedByKind[kind]
	if !ok {
		return nil, errors.New("unsupported job kind")
	}
	if payload == nil {
		return nil, errors.New("job payload must be an object")
	}
	for k := range payload {
		if !allowed[k] {
			return nil, errors.New("unsupported job payload field")
		}
	}
	out := map[string]any{}
	_, sid := payload["schedule_id"]
	_, sf := payload["scheduled_for"]
	if sid != sf {
		return nil, errors.New("schedule_id and scheduled_for must be supplied together")
	}
	if sid {
		id, ok := integer(payload["schedule_id"])
		if !ok || id <= 0 {
			return nil, errors.New("schedule_id must be a positive integer")
		}
		due, ok := payload["scheduled_for"].(string)
		if !ok {
			return nil, errors.New("scheduled_for must be an ISO timestamp")
		}
		if _, ok := parsePythonISOTime(due); !ok {
			return nil, errors.New("scheduled_for must be a canonical ISO timestamp")
		}
		out["schedule_id"] = id
		out["scheduled_for"] = due
	}
	if v, ok := payload["timeout_seconds"]; ok {
		n, yes := integer(v)
		if !yes || n < 1 || n > 86400 {
			return nil, errors.New("timeout_seconds must be an integer from 1 to 86400")
		}
		out["timeout_seconds"] = n
	}
	if kind == "news_digest" {
		for _, k := range []string{"run_processing", "send_telegram"} {
			if v, ok := payload[k]; ok {
				b, yes := v.(bool)
				if !yes {
					return nil, fmt.Errorf("%s must be a boolean", k)
				}
				out[k] = b
			}
		}
	}
	if kind == "reprocess_failed" {
		if v, ok := payload["limit"]; ok {
			n, yes := integer(v)
			if !yes || n < 1 || n > 100 {
				return nil, errors.New("limit must be an integer from 1 to 100")
			}
			out["limit"] = n
		}
		if v, ok := payload["dry_run"]; ok {
			b, yes := v.(bool)
			if !yes {
				return nil, errors.New("dry_run must be a boolean")
			}
			out["dry_run"] = b
		}
	}
	if kind == "article_reprocess" {
		var ids []any
		switch typed := payload["article_ids"].(type) {
		case []any:
			ids = typed
		case []int:
			ids = make([]any, len(typed))
			for i, n := range typed {
				ids[i] = n
			}
		case []int64:
			ids = make([]any, len(typed))
			for i, n := range typed {
				ids[i] = n
			}
		default:
			return nil, errors.New("article_ids must contain 1 to 100 IDs")
		}
		if len(ids) < 1 || len(ids) > MaxSelectedIDs {
			return nil, errors.New("article_ids must contain 1 to 100 IDs")
		}
		ints := make([]int, 0, len(ids))
		seen := map[int]bool{}
		for _, x := range ids {
			n, yes := integer(x)
			if !yes || n <= 0 {
				return nil, errors.New("article_ids must contain positive integer IDs")
			}
			if seen[n] {
				return nil, errors.New("article_ids must not contain duplicates")
			}
			seen[n] = true
			ints = append(ints, n)
		}
		sort.Ints(ints)
		out["article_ids"] = ints
		for k, d := range map[string]bool{"force": true, "reextract_media": false} {
			v, ok := payload[k]
			if !ok {
				v = d
			}
			b, yes := v.(bool)
			if !yes {
				return nil, fmt.Errorf("%s must be a boolean", k)
			}
			out[k] = b
		}
	}
	if kind == "source_test" {
		id, ok := integer(payload["source_id"])
		if !ok || id <= 0 {
			return nil, errors.New("source_id must be a positive integer")
		}
		out["source_id"] = id
	}
	encoded, err := CanonicalJSON(out)
	if err != nil || len(encoded) > MaxPayloadBytes {
		return nil, errors.New("job payload exceeds the 4096-byte limit")
	}
	return out, nil
}
func keys(values ...string) map[string]bool {
	m := map[string]bool{}
	for _, v := range values {
		m[v] = true
	}
	return m
}
func parsePythonISOTime(value string) (time.Time, bool) {
	layouts := []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"}
	for _, layout := range layouts {
		t, err := time.Parse(layout, value)
		if err != nil {
			continue
		}
		canonical := t.Format("2006-01-02T15:04:05")
		micro := t.Nanosecond() / 1000
		if micro != 0 {
			canonical += fmt.Sprintf(".%06d", micro)
		}
		if strings.ContainsAny(value, "Z+") || strings.LastIndex(value, "-") > 9 {
			canonical += t.Format("-07:00")
		}
		if canonical == value {
			return t, true
		}
	}
	return time.Time{}, false
}
func integer(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), int64(int(n)) == int64(n)
	case int64:
		return int(n), int64(int(n)) == n
	case float64:
		if math.Trunc(n) == n && n >= math.MinInt && n <= math.MaxInt {
			return int(n), true
		}
	}
	return 0, false
}

// CanonicalJSON matches Python json.dumps(sort_keys=True,separators=(',',':')) for these DTOs.
func CanonicalJSON(value any) ([]byte, error) { return json.Marshal(value) }

// MarshalJSON keeps API timestamps in the Python service's naive-UTC isoformat.
func (j Job) MarshalJSON() ([]byte, error) {
	type wire struct {
		ID              string         `json:"job_id"`
		Kind            string         `json:"kind"`
		Status          string         `json:"status"`
		CreatedAt       *string        `json:"created_at"`
		StartedAt       *string        `json:"started_at"`
		FinishedAt      *string        `json:"finished_at"`
		Result          map[string]any `json:"result"`
		ErrorCode       *string        `json:"error_code"`
		WorkerAvailable bool           `json:"worker_available"`
	}
	stamp := func(value *time.Time) *string {
		if value == nil {
			return nil
		}
		formatted := value.UTC().Format("2006-01-02T15:04:05")
		if micros := value.UTC().Nanosecond() / 1000; micros != 0 {
			formatted += fmt.Sprintf(".%06d", micros)
		}
		return &formatted
	}
	return json.Marshal(wire{j.ID, j.Kind, j.Status, stamp(j.CreatedAt), stamp(j.StartedAt), stamp(j.FinishedAt), j.Result, j.ErrorCode, j.WorkerAvailable})
}
