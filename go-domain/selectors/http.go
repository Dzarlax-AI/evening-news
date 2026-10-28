package selectors

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/privateapi"
	"github.com/jackc/pgx/v5/pgxpool"
)

var shaRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

type GuardChecker interface{ Check(context.Context) error }
type ContextReader interface {
	Get(context.Context, string, int) (authority.ExecutionContext, error)
}
type Store interface {
	PageStructure(context.Context, string) (Structure, error)
	RecordStructure(context.Context, string, *Analysis, SourceFence) (bool, error)
}
type Handler struct {
	Token     string
	Guard     GuardChecker
	Authority ContextReader
	Pool      *pgxpool.Pool
	Analyzer  *Analyzer
	Store     Store
}
type request struct {
	authority.Envelope
	Operation         string          `json:"operation"`
	SourceID          int64           `json:"source_id"`
	SourceFingerprint string          `json:"source_fingerprint"`
	StudyHTML         json.RawMessage `json:"study_html"`
}

func detail(w http.ResponseWriter, status int, s string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"detail": s})
}
func sourceDomain(s string) string {
	s = strings.TrimLeftFunc(s, func(r rune) bool { return r <= 0x20 })
	s = strings.NewReplacer("\r", "", "\n", "", "\t", "").Replace(s)
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	} else if strings.HasPrefix(s, "//") {
		s = s[2:]
	} else {
		return ""
	}
	if i := strings.IndexAny(s, "/?#"); i >= 0 {
		s = s[:i]
	}
	return strings.ToLower(s)
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.Token == "" {
		detail(w, 503, "Executor authentication is unavailable")
		return
	}
	auth, expected := r.Header.Get("Authorization"), "Bearer "+h.Token
	if len(auth) != len(expected) || subtle.ConstantTimeCompare([]byte(auth), []byte(expected)) != 1 {
		detail(w, 401, "Unauthorized")
		return
	}
	if h.Guard == nil || h.Guard.Check(r.Context()) != nil {
		detail(w, 503, "Executor instance is not the database owner")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 5*1024*1024+8193))
	if err != nil {
		detail(w, 400, "Request body is invalid")
		return
	}
	if len(raw) > 5*1024*1024+8192 {
		detail(w, 413, "Request body exceeds limit")
		return
	}
	var body request
	var fields map[string]json.RawMessage
	if privateapi.DecodeJSON(raw, &body) != nil || json.Unmarshal(raw, &fields) != nil || body.Version != 1 || (body.Operation != "load" && body.Operation != "study") || body.AttemptGeneration <= 0 || body.Generation <= 0 || body.BackendPID <= 0 || body.SourceID <= 0 || !shaRE.MatchString(body.SourceFingerprint) || h.Authority == nil || h.Pool == nil || h.Store == nil || h.Analyzer == nil {
		detail(w, 422, "Selector callback request is invalid")
		return
	}
	required := []string{"version", "job_id", "attempt_generation", "owner_id", "generation", "backend_pid", "executor_instance_id", "deadline_at", "operation", "source_id", "source_fingerprint"}
	if body.Operation == "study" {
		required = append(required, "study_html")
	}
	if len(fields) != len(required) {
		detail(w, 422, "Selector callback request is invalid")
		return
	}
	for _, name := range required {
		if _, ok := fields[name]; !ok {
			detail(w, 422, "Selector callback request is invalid")
			return
		}
	}
	deadline, err := body.Envelope.Deadline()
	if err != nil {
		detail(w, 422, "Selector callback fence is invalid")
		return
	}
	var studyHTML string
	if body.Operation == "study" {
		if json.Unmarshal(body.StudyHTML, &studyHTML) != nil || studyHTML == "" || len([]byte(studyHTML)) > 5*1024*1024 {
			detail(w, 413, "Captured HTML exceeds its limit")
			return
		}
	}
	get := func() (SourceFence, error) {
		current, e := h.Authority.Get(r.Context(), body.JobID, body.AttemptGeneration)
		if e != nil || !current.Envelope.Matches(body.Envelope) || (current.Kind != "news_processing" && current.Kind != "news_digest") {
			return SourceFence{}, authority.ErrStale
		}
		if current.Kind == "news_digest" {
			var p map[string]any
			if json.Unmarshal(current.Payload, &p) != nil || p["run_processing"] == false {
				return SourceFence{}, authority.ErrStale
			}
		}
		return SourceFence{Fence: current.Fence(h.Pool), SourceID: body.SourceID, Fingerprint: body.SourceFingerprint}, nil
	}
	f, err := get()
	if err != nil {
		detail(w, 409, "Selector callback execution authority is not current")
		return
	}
	if err = f.Fence.Check(r.Context()); err != nil {
		detail(w, 409, "Selector callback execution fence is stale")
		return
	}
	src, err := f.Validate(r.Context())
	if err != nil {
		detail(w, 409, "Ingestion callback fence is stale")
		return
	}
	domain := sourceDomain(src.URL)
	if domain == "" {
		detail(w, 409, "Page Monitor source URL is invalid")
		return
	}
	f, err = get()
	if err != nil {
		detail(w, 409, "Selector callback execution fence is stale")
		return
	}
	if _, err = f.Validate(r.Context()); err != nil {
		detail(w, 409, "Ingestion callback fence is stale")
		return
	}
	structure, err := h.Store.PageStructure(r.Context(), domain)
	if err != nil {
		detail(w, 502, "Selector memory returned invalid data")
		return
	}
	if body.Operation == "study" {
		if enabled, ok := src.Config["enable_ai_analysis"].(bool); ok && !enabled {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), deadline)
		defer cancel()
		analysis, e := h.Analyzer.analyze(ctx, src.URL, studyHTML, "news", domain, false)
		if ctx.Err() != nil {
			return
		}
		f, err = get()
		if err != nil {
			detail(w, 409, "Selector callback execution fence is stale")
			return
		}
		if _, err = f.Validate(r.Context()); err != nil {
			detail(w, 409, "Ingestion callback fence is stale")
			return
		}
		if e != nil || analysis == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		recorded, recordErr := h.Store.RecordStructure(ctx, domain, analysis, f)
		err = recordErr
		if errors.Is(err, authority.ErrStale) {
			detail(w, 409, "Ingestion callback fence is stale")
			return
		}
		if ctx.Err() != nil {
			return
		}
		f, err = get()
		if err != nil {
			detail(w, 409, "Selector callback execution fence is stale")
			return
		}
		if _, err = f.Validate(r.Context()); err != nil {
			detail(w, 409, "Ingestion callback fence is stale")
			return
		}
		if recordErr != nil || !recorded {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if txErr := h.publishAnalysis(ctx, f, src.URL, analysis); txErr != nil {
			detail(w, 409, "Ingestion callback fence is stale")
			return
		}

		structure, err = h.Store.PageStructure(r.Context(), domain)
		if err != nil {
			detail(w, 502, "Selector memory returned invalid data")
			return
		}
	}
	arrays := [][]string{structure.Container, structure.Title, structure.Link, structure.Date}
	for _, arr := range arrays {
		if len(arr) > 64 {
			detail(w, 502, "Selector memory returned invalid data")
			return
		}
		for _, v := range arr {
			if strings.TrimSpace(v) == "" || len([]byte(v)) > 2048 {
				detail(w, 502, "Selector memory returned invalid data")
				return
			}
		}
	}
	for i := range arrays {
		if arrays[i] == nil {
			arrays[i] = []string{}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "learned_container_selectors": arrays[0], "learned_title_selectors": arrays[1], "learned_link_selectors": arrays[2], "learned_date_selectors": arrays[3]})
}

func (h *Handler) publishAnalysis(ctx context.Context, f SourceFence, url string, analysis *Analysis) error {
	return h.withPublicationFence(ctx, f, func() { h.Analyzer.put(cacheKey(url, "news"), analysis) })
}
func (h *Handler) withPublicationFence(ctx context.Context, f SourceFence, publish func()) error {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = f.ValidateTx(ctx, tx); err != nil {
		return err
	}
	if ctx.Err() != nil || h.Guard.Check(ctx) != nil {
		return authority.ErrStale
	}
	publish()
	return nil
}
