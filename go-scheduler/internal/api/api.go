package api

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/calendar"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/jsonstrict"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/store"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type API struct {
	Store      *store.Store
	Token      string
	Targets    map[string]model.Target
	PollEvery  time.Duration
	BuildID    string
	LastTickNS atomic.Int64
}

func (a *API) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && r.Method == http.MethodGet {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		if r.URL.Path == "/readyz" && r.Method == http.MethodGet {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			last := a.LastTickNS.Load()
			if last == 0 || time.Since(time.Unix(0, last)) > 3*a.PollEvery || a.Store.Ping(ctx) != nil {
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
			return
		}
		if r.URL.Path == "/version" && r.Method == http.MethodGet {
			writeJSON(w, http.StatusOK, map[string]string{"version": "1", "build_id": a.BuildID})
			return
		}
		if !strings.HasPrefix(r.URL.Path, "/v1/") {
			writeAPIError(w, http.StatusNotFound, "not_found", "route not found", nil)
			return
		}
		if !a.authorized(r) {
			writeAPIError(w, http.StatusUnauthorized, "unauthorized", "bearer token required", nil)
			return
		}
		a.serveV1(w, r)
	})
}

func (a *API) authorized(r *http.Request) bool {
	parts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) != len(a.Token) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(parts[1]), []byte(a.Token)) == 1
}

func (a *API) serveV1(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	if path == "schedules" {
		a.schedules(w, r)
		return
	}
	if path == "control" {
		a.control(w, r)
		return
	}
	if path == "status" {
		a.status(w, r)
		return
	}
	if path == "import" {
		a.importSchedules(w, r)
		return
	}
	if path == "occurrences" {
		a.occurrences(w, r)
		return
	}
	if strings.HasPrefix(path, "schedules/") {
		a.schedule(w, r, strings.TrimPrefix(path, "schedules/"))
		return
	}
	if strings.HasPrefix(path, "occurrences/") && strings.HasSuffix(path, "/retry") {
		id := strings.TrimSuffix(strings.TrimPrefix(path, "occurrences/"), "/retry")
		a.retry(w, r, id)
		return
	}
	writeAPIError(w, http.StatusNotFound, "not_found", "route not found", nil)
}

func (a *API) schedules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	limit, cursor, err := pageArgs(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "invalid pagination parameters", nil)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, next, err := a.Store.ListSchedules(ctx, limit, cursor)
	if err != nil {
		if errors.Is(err, store.ErrInvalidCursor) {
			writeAPIError(w, http.StatusBadRequest, "invalid_json", "invalid pagination cursor", nil)
			return
		}
		writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "schedule list unavailable", nil)
		return
	}
	var nextValue any = next
	if next == "" {
		nextValue = nil
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": items, "next_cursor": nextValue})
}

func (a *API) schedule(w http.ResponseWriter, r *http.Request, key string) {
	if len(key) == 0 || strings.Contains(key, "/") {
		writeAPIError(w, http.StatusNotFound, "schedule_not_found", "schedule not found", nil)
		return
	}
	switch r.Method {
	case http.MethodGet:
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		item, err := a.Store.GetSchedule(ctx, key)
		if errors.Is(err, store.ErrNotFound) {
			writeAPIError(w, http.StatusNotFound, "schedule_not_found", "schedule not found", nil)
			return
		}
		if err != nil {
			writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "schedule lookup unavailable", nil)
			return
		}
		writeJSON(w, http.StatusOK, item)
	case http.MethodPut:
		var body struct {
			Expected  *int64         `json:"expected_revision"`
			Enabled   *bool          `json:"enabled"`
			Calendar  model.Calendar `json:"calendar"`
			Target    string         `json:"target_alias"`
			Operation string         `json:"operation"`
			Payload   map[string]any `json:"payload"`
		}
		if err := decodeJSON(w, r, &body, 16<<10); err != nil {
			writeDecodeError(w, err)
			return
		}
		if body.Expected == nil || body.Enabled == nil || body.Calendar.Type == "" || body.Target == "" || body.Operation == "" || body.Payload == nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_json", "missing required schedule fields", nil)
			return
		}
		if err := calendar.Validate(body.Calendar); err != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "invalid_schedule", "invalid schedule calendar", nil)
			return
		}
		mutation := model.Mutation{ExpectedRevision: *body.Expected, Enabled: *body.Enabled, Calendar: body.Calendar, Target: body.Target, Operation: body.Operation, Payload: body.Payload}
		if *body.Expected < 0 || calendar.ValidateResource(key, body.Target, body.Operation, body.Payload, a.Targets) != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "invalid_schedule", "invalid schedule configuration", nil)
			return
		}
		payloadJSON, marshalErr := json.Marshal(body.Payload)
		if marshalErr != nil || len(payloadJSON) > 4096 {
			writeAPIError(w, http.StatusUnprocessableEntity, "invalid_schedule", "payload exceeds 4096 bytes", nil)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		item, created, err := a.Store.PutSchedule(ctx, key, mutation, a.Targets, time.Now().UTC())
		if errors.Is(err, store.ErrConflict) {
			current := int64(0)
			if found, e := a.Store.GetSchedule(ctx, key); e == nil {
				current = found.Revision
			}
			writeAPIError(w, http.StatusConflict, "revision_conflict", "schedule revision does not match", map[string]any{"current_revision": current})
			return
		}
		if err != nil {
			writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "schedule save unavailable", nil)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		writeJSON(w, status, item)
	case http.MethodDelete:
		var body struct {
			Expected *int64 `json:"expected_revision"`
		}
		if err := decodeJSON(w, r, &body, 16<<10); err != nil {
			writeDecodeError(w, err)
			return
		}
		if body.Expected == nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_json", "expected_revision is required", nil)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		err := a.Store.DeleteSchedule(ctx, key, *body.Expected)
		if errors.Is(err, store.ErrNotFound) {
			writeAPIError(w, http.StatusNotFound, "schedule_not_found", "schedule not found", nil)
			return
		}
		if errors.Is(err, store.ErrConflict) {
			writeAPIError(w, http.StatusConflict, "revision_conflict", "schedule revision does not match", nil)
			return
		}
		if err != nil {
			writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "schedule delete unavailable", nil)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(w)
	}
}

func (a *API) control(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		methodNotAllowed(w)
		return
	}
	var body struct {
		Expected *int64 `json:"expected_revision"`
		Paused   *bool  `json:"paused"`
	}
	if err := decodeJSON(w, r, &body, 16<<10); err != nil {
		writeDecodeError(w, err)
		return
	}
	if body.Expected == nil || body.Paused == nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "expected_revision and paused are required", nil)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := a.Store.SetPaused(ctx, *body.Expected, *body.Paused, time.Now().UTC())
	if errors.Is(err, store.ErrConflict) {
		writeAPIError(w, http.StatusConflict, "revision_conflict", "control revision does not match", nil)
		return
	}
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "control update unavailable", nil)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) status(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := a.Store.GetStatus(ctx)
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "scheduler status unavailable", nil)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) importSchedules(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	var raw struct {
		Expected *int64            `json:"expected_control_revision"`
		Items    []json.RawMessage `json:"items"`
	}
	if err := decodeJSON(w, r, &raw, 512<<10); err != nil {
		writeDecodeError(w, err)
		return
	}
	if raw.Expected == nil || len(raw.Items) == 0 || len(raw.Items) > 100 {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "import requires expected_control_revision and 1 through 100 items", nil)
		return
	}
	items := make([]store.ImportItem, 0, len(raw.Items))
	for _, entry := range raw.Items {
		var obj map[string]json.RawMessage
		if err := decodeRaw(entry, &obj); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_json", "invalid import item", nil)
			return
		}
		allowed := map[string]bool{"schedule_key": true, "enabled": true, "calendar": true, "target_alias": true, "operation": true, "payload": true, "next_due_at": true}
		if len(obj) != len(allowed) {
			writeAPIError(w, http.StatusBadRequest, "invalid_json", "import item fields are incomplete or unknown", nil)
			return
		}
		for k := range obj {
			if !allowed[k] {
				writeAPIError(w, http.StatusBadRequest, "invalid_json", "import item fields are incomplete or unknown", nil)
				return
			}
		}
		var item struct {
			Key       string          `json:"schedule_key"`
			Enabled   *bool           `json:"enabled"`
			Calendar  model.Calendar  `json:"calendar"`
			Target    string          `json:"target_alias"`
			Operation string          `json:"operation"`
			Payload   map[string]any  `json:"payload"`
			NextDueAt json.RawMessage `json:"next_due_at"`
		}
		if err := decodeRaw(entry, &item); err != nil || item.Enabled == nil || item.Calendar.Type == "" || item.Target == "" || item.Operation == "" || item.Payload == nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_json", "invalid import item", nil)
			return
		}
		if calendar.Validate(item.Calendar) != nil || calendar.ValidateResource(item.Key, item.Target, item.Operation, item.Payload, a.Targets) != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "invalid_schedule", "invalid imported schedule", nil)
			return
		}
		payloadJSON, marshalErr := json.Marshal(item.Payload)
		if marshalErr != nil || len(payloadJSON) > 4096 {
			writeAPIError(w, http.StatusUnprocessableEntity, "invalid_schedule", "import payload exceeds 4096 bytes", nil)
			return
		}
		var due *time.Time
		if string(obj["next_due_at"]) != "null" {
			var ts string
			if err := json.Unmarshal(obj["next_due_at"], &ts); err != nil {
				writeAPIError(w, http.StatusBadRequest, "invalid_json", "next_due_at must be a UTC timestamp or null", nil)
				return
			}
			t, err := parseUTCTimestamp(ts)
			if err != nil {
				writeAPIError(w, http.StatusBadRequest, "invalid_json", "next_due_at must be a canonical UTC timestamp with at most six fractional digits", nil)
				return
			}
			due = &t
		}
		if !*item.Enabled && due != nil {
			writeAPIError(w, http.StatusUnprocessableEntity, "invalid_schedule", "disabled imports must have null next_due_at", nil)
			return
		}
		items = append(items, store.ImportItem{Key: item.Key, Enabled: *item.Enabled, Calendar: item.Calendar, Target: item.Target, Operation: item.Operation, Payload: item.Payload, NextDueAt: due})
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	result, err := a.Store.Import(ctx, *raw.Expected, items, a.Targets, time.Now().UTC())
	if errors.Is(err, store.ErrImportPaused) {
		writeAPIError(w, http.StatusConflict, "import_requires_pause", "scheduler must be paused before import", nil)
		return
	}
	if errors.Is(err, store.ErrConflict) {
		writeAPIError(w, http.StatusConflict, "revision_conflict", "control revision does not match", nil)
		return
	}
	if errors.Is(err, store.ErrImportConflict) {
		writeAPIError(w, http.StatusConflict, "import_conflict", "import conflicts with existing schedule", nil)
		return
	}
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "schedule import unavailable", nil)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (a *API) occurrences(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w)
		return
	}
	limit, cursor, err := pageArgs(r)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "invalid pagination parameters", nil)
		return
	}
	key := r.URL.Query().Get("schedule_key")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	items, next, err := a.Store.ListOccurrences(ctx, key, cursor, limit)
	if err != nil {
		if errors.Is(err, store.ErrInvalidCursor) {
			writeAPIError(w, http.StatusBadRequest, "invalid_json", "invalid pagination cursor", nil)
			return
		}
		if key != "" {
			if _, e := a.Store.GetSchedule(ctx, key); errors.Is(e, store.ErrNotFound) {
				writeAPIError(w, http.StatusNotFound, "schedule_not_found", "schedule not found", nil)
				return
			}
		}
		writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "occurrence inventory unavailable", nil)
		return
	}
	var nextValue any = next
	if next == "" {
		nextValue = nil
	}
	writeJSON(w, http.StatusOK, map[string]any{"occurrences": items, "next_cursor": nextValue})
}

func (a *API) retry(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w)
		return
	}
	if !uuidPattern.MatchString(id) {
		writeAPIError(w, http.StatusNotFound, "occurrence_not_found", "occurrence not found", nil)
		return
	}
	var empty map[string]any
	if err := decodeJSON(w, r, &empty, 16<<10); err != nil {
		writeDecodeError(w, err)
		return
	}
	if empty == nil || len(empty) != 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid_json", "retry body must be an empty JSON object", nil)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	status, err := a.Store.RetryBlocked(ctx, id, time.Now().UTC())
	if errors.Is(err, store.ErrOccurrenceNotFound) {
		writeAPIError(w, http.StatusNotFound, "occurrence_not_found", "occurrence not found", nil)
		return
	}
	if errors.Is(err, store.ErrOccurrenceNotRetryable) {
		writeAPIError(w, http.StatusConflict, "occurrence_not_retryable", "accepted occurrence cannot be replayed", nil)
		return
	}
	if err != nil {
		writeAPIError(w, http.StatusServiceUnavailable, "unavailable", "occurrence retry unavailable", nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"occurrence_id": id, "status": status})
}

type decodeError struct {
	status        int
	code, message string
}

func (e decodeError) Error() string { return e.code }

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any, max int64) error {
	if r.Body == nil {
		return decodeError{http.StatusBadRequest, "invalid_json", "request body required"}
	}
	r.Body = http.MaxBytesReader(w, r.Body, max)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return decodeError{http.StatusBadRequest, "invalid_json", "request body invalid or too large"}
	}
	if !utf8.Valid(data) {
		return decodeError{http.StatusBadRequest, "invalid_json", "request body must be valid UTF-8"}
	}
	return decodeRaw(data, dst)
}
func decodeRaw(data []byte, dst any) error {
	if err := jsonstrict.RejectDuplicateKeys(data); err != nil {
		return decodeError{http.StatusBadRequest, "invalid_json", "duplicate or invalid JSON member"}
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return decodeError{http.StatusBadRequest, "invalid_json", "malformed JSON or unknown field"}
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return decodeError{http.StatusBadRequest, "invalid_json", "request must contain exactly one JSON value"}
	}
	return nil
}
func writeDecodeError(w http.ResponseWriter, err error) {
	var de decodeError
	if errors.As(err, &de) {
		writeAPIError(w, de.status, de.code, de.message, nil)
		return
	}
	writeAPIError(w, http.StatusBadRequest, "invalid_json", "malformed JSON", nil)
}
func pageArgs(r *http.Request) (int, string, error) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			return 0, "", errors.New("invalid limit")
		}
		limit = n
	}
	cursor := r.URL.Query().Get("cursor")
	if cursor != "" {
		if _, err := base64.RawURLEncoding.DecodeString(cursor); err != nil {
			return 0, "", err
		}
	}
	return limit, cursor, nil
}
func parseUTCTimestamp(v string) (time.Time, error) {
	if !strings.HasSuffix(v, "Z") {
		return time.Time{}, errors.New("timestamp must be UTC")
	}
	dot := strings.LastIndexByte(v, '.')
	if dot >= 0 {
		fraction := strings.TrimSuffix(v[dot+1:], "Z")
		if len(fraction) < 1 || len(fraction) > 6 {
			return time.Time{}, errors.New("timestamp precision exceeds microseconds")
		}
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}
func methodNotAllowed(w http.ResponseWriter) {
	writeAPIError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", nil)
}
func writeAPIError(w http.ResponseWriter, status int, code, message string, extra map[string]any) {
	payload := map[string]any{"error": map[string]string{"code": code, "message": message}}
	for k, v := range extra {
		payload[k] = v
	}
	writeJSON(w, status, payload)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
