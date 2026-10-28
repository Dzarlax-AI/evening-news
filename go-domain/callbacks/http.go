package callbacks

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/privateapi"
	"github.com/jackc/pgx/v5/pgxpool"
)

var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var hashRE = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ErrTextLimit identifies the legacy 413 response for overlong article text.
var ErrTextLimit = errors.New("article text exceeds limit")

type Request struct {
	Version          int                `json:"version"`
	RunID            string             `json:"run_id"`
	Envelope         authority.Envelope `json:"envelope"`
	ArticleID        int64              `json:"article_id"`
	ArticleAttempt   int                `json:"article_attempt"`
	InputFingerprint string             `json:"input_fingerprint"`
	Operation        string             `json:"operation"`
	Input            json.RawMessage    `json:"input"`
}

type GuardChecker interface{ Check(context.Context) error }
type ContextReader interface {
	Get(context.Context, string, int) (authority.ExecutionContext, error)
}

// Compute must implement all three operations and their precise input semantics.
// It is invoked only after a successful current-claim check and must not hold a
// database connection over network calls. It is responsible for fencing writes.
type Compute interface {
	Validate(operation string, input json.RawMessage) error
	Run(context.Context, Request, authority.Fence) (any, error)
}
type Handler struct {
	Token     string
	Guard     GuardChecker
	Authority ContextReader
	Pool      *pgxpool.Pool
	Admission *Admission
	Compute   Compute
}

func runID(job string, attempt int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("evening-news-processing-v1\x00%s\x00%d", job, attempt)))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
func canonical(data json.RawMessage) ([]byte, error) {
	var value any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	value = normalizeNumbers(value)
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// CanonicalJSON is the compact sorted JSON representation used for callback
// cache identities. Numeric spellings are normalized after decoding.
func CanonicalJSON(data json.RawMessage) ([]byte, error) { return canonical(data) }

// Python json.loads converts decimal/exponent literals to float and its
// serializer emits a normalized representation. This makes equivalent retry
// payloads share one cache key without conflating integer and float JSON types.
func normalizeNumbers(value any) any {
	switch v := value.(type) {
	case map[string]any:
		for k, item := range v {
			v[k] = normalizeNumbers(item)
		}
		return v
	case []any:
		for i, item := range v {
			v[i] = normalizeNumbers(item)
		}
		return v
	case json.Number:
		text := string(v)
		if !strings.ContainsAny(text, ".eE") {
			return v
		}
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return v
		}
		scientific := strconv.FormatFloat(f, 'e', -1, 64)
		index := strings.LastIndexByte(scientific, 'e')
		exponent, err := strconv.Atoi(scientific[index+1:])
		if err != nil {
			return v
		}
		if exponent >= -4 && exponent < 16 {
			text = strconv.FormatFloat(f, 'f', -1, 64)
			if !strings.Contains(text, ".") {
				text += ".0"
			}
		} else {
			text = scientific
		}
		return json.Number(text)
	default:
		return value
	}
}
func hash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func analysisFingerprint(input map[string]any) (string, error) {
	ident := map[string]any{"title": input["title"], "content": input["content"], "url": input["url"]}
	if value, ok := input["original_context"]; ok {
		ident["original_context"] = value
	}
	wire, err := json.Marshal(ident)
	if err != nil {
		return "", err
	}
	canonicalWire, err := canonical(wire)
	if err != nil {
		return "", err
	}
	return hash(canonicalWire), nil
}
func detail(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"detail": value})
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.Token == "" {
		detail(w, 503, "Article AI authentication is unavailable")
		return
	}
	auth := r.Header.Get("Authorization")
	expected := "Bearer " + h.Token
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
	var req Request
	if privateapi.DecodeJSON(raw, &req) != nil || req.Version != 1 || !uuidRE.MatchString(req.RunID) || req.ArticleID <= 0 || req.ArticleAttempt <= 0 || !hashRE.MatchString(req.InputFingerprint) || (req.Operation != "analyze" && req.Operation != "extraction_context" && req.Operation != "extraction_feedback") || h.Compute == nil || h.Admission == nil || h.Pool == nil || h.Authority == nil {
		detail(w, 422, "Article AI callback request is invalid")
		return
	}
	deadline, err := req.Envelope.Deadline()
	if err != nil {
		detail(w, 422, "Article AI callback request is invalid")
		return
	}
	if err = h.Compute.Validate(req.Operation, req.Input); err != nil {
		if errors.Is(err, ErrTextLimit) {
			detail(w, 413, "Article AI text exceeds its limit")
			return
		}
		detail(w, 422, "Article AI callback request is invalid")
		return
	}
	var input map[string]any
	dec := json.NewDecoder(bytes.NewReader(req.Input))
	dec.UseNumber()
	if dec.Decode(&input) != nil || input == nil {
		detail(w, 422, "Article AI callback request is invalid")
		return
	}
	urlValue, ok := input["url"].(string)
	if !ok {
		detail(w, 422, "Article AI callback request is invalid")
		return
	}
	if req.Operation == "analyze" {
		fingerprint, e := analysisFingerprint(input)
		if e != nil || fingerprint != req.InputFingerprint {
			detail(w, 422, "Article AI callback request is invalid")
			return
		}
	}
	if req.RunID != runID(req.Envelope.JobID, req.Envelope.AttemptGeneration) {
		detail(w, 409, "Article AI run identifier does not match its job")
		return
	}
	envJSON, _ := json.Marshal(req.Envelope)
	envDigest := hash(envJSON)
	getContext := func() (authority.ExecutionContext, error) {
		current, e := h.Authority.Get(r.Context(), req.Envelope.JobID, req.Envelope.AttemptGeneration)
		if e != nil || !current.Envelope.Matches(req.Envelope) {
			return authority.ExecutionContext{}, authority.ErrStale
		}
		if current.Kind != "news_processing" && current.Kind != "news_digest" && current.Kind != "article_reprocess" && current.Kind != "reprocess_failed" {
			return authority.ExecutionContext{}, authority.ErrStale
		}
		if current.Kind == "news_digest" {
			var p map[string]any
			if json.Unmarshal(current.Payload, &p) != nil || p["run_processing"] == false {
				return authority.ExecutionContext{}, authority.ErrStale
			}
		}
		return current, nil
	}
	current, err := getContext()
	if err != nil {
		detail(w, 409, "Article AI execution authority is not current")
		return
	}
	if err = current.Fence(h.Pool).Check(r.Context()); err != nil {
		detail(w, 409, "Article AI execution fence is stale")
		return
	}
	validate := func() (authority.Fence, error) {
		current, e := getContext()
		if e != nil {
			return authority.Fence{}, e
		}
		fence := current.Fence(h.Pool)
		claim := Claim{Fence: fence, RunID: req.RunID, EnvelopeDigest: envDigest, ArticleID: req.ArticleID, ArticleAttempt: req.ArticleAttempt, InputFingerprint: req.InputFingerprint, URL: urlValue}
		if e = claim.Validate(r.Context()); e != nil {
			return authority.Fence{}, e
		}
		return fence, nil
	}
	fence, err := validate()
	if err != nil {
		detail(w, 409, "Article AI item claim is stale")
		return
	}
	inputBytes, err := canonical(req.Input)
	if err != nil {
		detail(w, 422, "Article AI callback request is invalid")
		return
	}
	key := fmt.Sprintf("%s/%d/%d/%s/%s/%s", req.RunID, req.ArticleID, req.ArticleAttempt, req.InputFingerprint, req.Operation, hash(inputBytes))
	cached, hit, err := h.Admission.Begin(key)
	if err != nil {
		w.Header().Set("Retry-After", "1")
		detail(w, 429, err.Error())
		return
	}
	if hit {
		if _, err = validate(); err != nil {
			detail(w, 409, "Article AI item claim is stale")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(cached)
		return
	}
	var response []byte
	finished := false
	defer func() {
		if !finished {
			h.Admission.Finish(key, nil)
		}
	}()
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	result, err := h.Compute.Run(ctx, req, fence)
	if err != nil {
		if errors.Is(err, ErrNotImplemented) {
			detail(w, 503, "Article AI operation is unavailable")
			return
		} else if errors.Is(err, authority.ErrStale) {
			detail(w, 409, "Article AI learning claim is stale")
			return
		} else if ctx.Err() != nil {
			return
		} else if req.Operation == "extraction_feedback" {
			result = map[string]any{"recorded": false}
		} else {
			detail(w, 502, "Article AI result is invalid")
			return
		}
	}
	if _, err = validate(); err != nil {
		detail(w, 409, "Article AI item claim is stale")
		return
	}
	response, err = json.Marshal(map[string]any{"version": 1, "run_id": req.RunID, "article_id": req.ArticleID, "article_attempt": req.ArticleAttempt, "input_fingerprint": req.InputFingerprint, "operation": req.Operation, "result": result})
	if err != nil || !utf8.Valid(response) {
		detail(w, 502, "Article AI result is invalid")
		return
	}
	if len(response) > 1024*1024 {
		detail(w, 502, "Article AI result exceeded its response limit")
		return
	}
	h.Admission.Finish(key, response)
	finished = true
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(response)
}
