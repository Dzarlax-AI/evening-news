package airuntime

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxRPCBody = 5 << 20

type Config struct {
	Socket, Token, DatabaseURL, APIKey, Endpoint, Model, LocalCategory string
	RPS                                                                int
	InputRate, OutputRate, CachedRate                                  float64
	DBMaxConns                                                         int32
}

func ConfigFromEnv() (Config, error) {
	c := Config{Socket: os.Getenv("NEWS_AI_SOCKET"), Token: os.Getenv("NEWS_AI_TOKEN"), DatabaseURL: normalizeDSN(os.Getenv("DATABASE_URL")), APIKey: os.Getenv("GEMINI_API_KEY"), Endpoint: os.Getenv("GEMINI_API_ENDPOINT"), Model: os.Getenv("SUMMARIZATION_MODEL"), LocalCategory: os.Getenv("LOCAL_CATEGORY"), RPS: 3, DBMaxConns: 2}
	if c.Socket == "" || c.Token == "" || c.DatabaseURL == "" || c.APIKey == "" || c.Endpoint == "" || c.Model == "" {
		return c, errors.New("ai_runtime_missing_configuration")
	}
	if len(c.Token) < 32 || len(c.Token) > 256 {
		return c, errors.New("ai_runtime_token_configuration")
	}
	if _, err := providerURL(c.Endpoint, c.Model); err != nil {
		return c, errors.New("ai_runtime_provider_configuration")
	}
	if raw := os.Getenv("RPS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 {
			return c, errors.New("ai_runtime_rps_configuration")
		}
		c.RPS = n
	}
	if raw := os.Getenv("NEWS_AI_DB_MAX_CONNS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 2 {
			return c, errors.New("ai_runtime_database_pool_configuration")
		}
		c.DBMaxConns = int32(n)
	}
	c.InputRate = parseRate(os.Getenv("AI_INPUT_COST_PER_1M"))
	c.OutputRate = parseRate(os.Getenv("AI_OUTPUT_COST_PER_1M"))
	c.CachedRate = parseRate(os.Getenv("AI_CACHED_INPUT_COST_PER_1M"))
	return c, nil
}
func parseRate(s string) float64 {
	v, e := strconv.ParseFloat(s, 64)
	if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0
	}
	return v
}
func normalizeDSN(s string) string {
	return strings.Replace(s, "postgresql+asyncpg://", "postgres://", 1)
}

type Server struct {
	config       Config
	db           *pgxpool.Pool
	limiter      *SlidingWindow
	breaker      *Breaker
	metadata     *MetadataStore
	client       *http.Client
	usage        chan usageRecord
	workers      sync.WaitGroup
	closing      chan struct{}
	closeOnce    sync.Once
	mux          *http.ServeMux
	usageMu      sync.RWMutex
	usageClosed  bool
	usageFailed  atomic.Uint64
	usageDropped atomic.Uint64
	usagePending atomic.Int64
	ready        atomic.Bool
	draining     atomic.Bool
}

const articleAnalysisType = "combined_analysis"

type usageRecord struct {
	domain, analysisType string
	usage                analysis.Usage
	accountingTokens     int
	accountingTokenSet   bool
}

func NewServer(c Config, db *pgxpool.Pool) *Server {
	s := &Server{config: c, db: db, limiter: NewSlidingWindow(c.RPS), breaker: NewBreaker(), metadata: NewMetadataStore(db), client: &http.Client{}, usage: make(chan usageRecord, 64), closing: make(chan struct{}), mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /v1/status", s.status)
	s.mux.HandleFunc("POST /v1/analyze", s.analyze)
	s.mux.HandleFunc("POST /v1/generate", s.generate)
	s.mux.HandleFunc("POST /v1/permit", s.permit)
	s.mux.HandleFunc("POST /v1/categories/invalidate", s.invalidate)
	return s
}
func (s *Server) Handler() http.Handler { return s.auth(s.mux) }
func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		provided := ""
		if strings.HasPrefix(header, "Bearer ") {
			provided = strings.TrimPrefix(header, "Bearer ")
		}
		if s.config.Token == "" || len(provided) != len(s.config.Token) || subtle.ConstantTimeCompare([]byte(provided), []byte(s.config.Token)) != 1 {
			writeJSON(w, http.StatusUnauthorized, errorEnvelope("unauthorized", http.StatusUnauthorized))
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": map[string]any{"ready": s.ready.Load() && !s.draining.Load(), "protocol_version": 1, "usage_failed": s.usageFailed.Load(), "usage_dropped": s.usageDropped.Load(), "usage_pending": s.usagePending.Load()}})
}
func (s *Server) permit(w http.ResponseWriter, r *http.Request) {
	var v commonRequest
	if !decodeTyped(r, &v) {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	ctx, cancel, ok := s.contextFor(r, w, v)
	if !ok {
		return
	}
	defer cancel()
	if err := s.limiter.Acquire(ctx); err != nil {
		writeJSON(w, 504, errorEnvelope("provider_timeout", 504))
		return
	}
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": map[string]any{"admitted": true}})
}
func (s *Server) invalidate(w http.ResponseWriter, r *http.Request) {
	var v commonRequest
	if !decodeTyped(r, &v) {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	_, cancel, ok := s.contextFor(r, w, v)
	if !ok {
		return
	}
	defer cancel()
	s.metadata.Invalidate()
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": map[string]any{"invalidated": true}})
}

type commonRequest struct {
	Version    int    `json:"version"`
	RequestID  string `json:"request_id"`
	DeadlineAt string `json:"deadline_at"`
}
type analyzeInput struct {
	Title           string  `json:"title"`
	Content         string  `json:"content"`
	URL             string  `json:"url"`
	OriginalContext *string `json:"original_context"`
	MaxTextChars    int     `json:"max_text_chars"`
}
type analyzeRequest struct {
	commonRequest
	Input analyzeInput `json:"input"`
	Model string       `json:"model"`
}
type generateOptions struct {
	Temperature float64 `json:"temperature"`
	MaxTokens   int     `json:"max_tokens"`
}
type generateRequest struct {
	commonRequest
	Mode         string          `json:"mode"`
	Prompt       string          `json:"prompt"`
	Model        string          `json:"model"`
	Schema       json.RawMessage `json:"schema,omitempty"`
	Options      generateOptions `json:"options"`
	AnalysisType string          `json:"analysis_type"`
	Domain       string          `json:"domain"`
}

func decodeTyped(r *http.Request, v any) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRPCBody+1))
	if err != nil || len(body) > maxRPCBody {
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err = dec.Decode(v); err != nil {
		return false
	}
	return dec.Decode(new(any)) == io.EOF
}
func (s *Server) contextFor(r *http.Request, w http.ResponseWriter, base commonRequest) (context.Context, context.CancelFunc, bool) {
	if s.draining.Load() {
		writeJSON(w, 503, errorEnvelope("runtime_draining", 503))
		return nil, nil, false
	}
	if base.Version != 1 || len(base.RequestID) < 1 || len(base.RequestID) > 64 {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return nil, nil, false
	}
	deadline, err := time.Parse(time.RFC3339Nano, base.DeadlineAt)
	if err != nil || !deadline.After(time.Now()) || deadline.After(time.Now().Add(5*time.Minute)) {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return nil, nil, false
	}
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	stop := make(chan struct{})
	go func() {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		case <-stop:
		}
	}()
	return ctx, func() { close(stop); cancel() }, true
}
func (s *Server) analyze(w http.ResponseWriter, r *http.Request) {
	var base analyzeRequest
	if !decodeTyped(r, &base) {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	ctx, cancel, ok := s.contextFor(r, w, base.commonRequest)
	if !ok {
		return
	}
	defer cancel()
	if base.Model == "" {
		base.Model = s.config.Model
	}
	if _, err := providerURL(s.config.Endpoint, base.Model); err != nil {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	cats, local := s.metadata.Load(ctx)
	input := base.Input
	if input.MaxTextChars == 0 {
		input.MaxTextChars = 100000
	}
	if input.MaxTextChars > 1000000 {
		writeJSON(w, 400, errorEnvelope("resource_limit", 400))
		return
	}
	req := analysis.Request{Version: 1, Title: input.Title, Content: input.Content, URL: input.URL, OriginalContext: input.OriginalContext, Categories: cats, LocalCategory: local, ConfiguredLocalCategory: s.config.LocalCategory, Model: base.Model, Endpoint: s.config.Endpoint, MaxTextChars: input.MaxTextChars}
	value, err := s.breaker.Do(ctx, func() (any, error) {
		return s.retry(ctx, func(attemptCtx context.Context) (any, error) {
			client := *s.client
			client.Timeout = 40 * time.Second
			result, usage, e := analysis.Analyze(attemptCtx, req, analysis.ProviderConfig{APIKey: s.config.APIKey, Model: base.Model, Endpoint: s.config.Endpoint, Client: &client, BeforeAttempt: s.limiter.Acquire})
			return analysisOutput{result, usage}, e
		})
	})
	if err != nil {
		writeError(w, err)
		return
	}
	result := value.(analysisOutput)
	if result.usage != nil {
		s.enqueueUsage(usageRecord{domain: domainFor(input.URL), analysisType: articleAnalysisType, usage: *result.usage})
	}
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": map[string]any{"result": result.result, "usage": result.usage}})
}

type analysisOutput struct {
	result map[string]any
	usage  *analysis.Usage
}

func domainFor(raw string) string {
	scheme := strings.Index(raw, "://")
	if scheme <= 0 {
		return "unknown"
	}
	rest := raw[scheme+3:]
	end := strings.IndexAny(rest, "/?#")
	if end < 0 {
		end = len(rest)
	}
	if end == 0 {
		return "unknown"
	}
	return rest[:end]
}

func (s *Server) generate(w http.ResponseWriter, r *http.Request) {
	var req generateRequest
	if !decodeTyped(r, &req) {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	ctx, cancel, ok := s.contextFor(r, w, req.commonRequest)
	if !ok {
		return
	}
	defer cancel()
	if req.Mode != "text" && req.Mode != "structured" {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	if req.Mode == "structured" && (len(req.Schema) == 0 || !json.Valid(req.Schema)) || req.Mode == "text" && len(req.Schema) > 0 {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	if req.Model == "" {
		req.Model = s.config.Model
	}
	if _, err := providerURL(s.config.Endpoint, req.Model); err != nil || len(req.Prompt) > maxRPCBody/2 || req.AnalysisType == "" || req.Domain == "" || len(req.AnalysisType) > 50 || len(req.Domain) > 8192 {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	if req.Mode == "text" {
		if req.Options.Temperature < 0 || req.Options.Temperature > 2 || req.Options.MaxTokens < 1 || req.Options.MaxTokens > 100000 {
			writeJSON(w, 400, errorEnvelope("invalid_input", 400))
			return
		}
	}
	value, err := s.breaker.Do(ctx, func() (any, error) {
		return s.retry(ctx, func(attemptCtx context.Context) (any, error) { return s.callGenerate(attemptCtx, req) })
	})
	if err != nil {
		writeError(w, err)
		return
	}
	out := value.(generateOutput)
	s.enqueueUsage(usageRecord{domain: req.Domain, analysisType: req.AnalysisType, usage: out.usage, accountingTokens: out.accountingTokens, accountingTokenSet: out.accountingTokenSet})
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": out.result})
}

type generateOutput struct {
	result             map[string]any
	usage              analysis.Usage
	accountingTokens   int
	accountingTokenSet bool
}
type providerFailure struct {
	code       string
	status     *int
	retryAfter *float64
	retryable  bool
}

func (e *providerFailure) Error() string { return e.code }
func (s *Server) callGenerate(ctx context.Context, v generateRequest) (any, error) {
	if err := s.limiter.Acquire(ctx); err != nil {
		return nil, &providerFailure{code: "provider_timeout", retryable: errors.Is(err, context.DeadlineExceeded)}
	}
	urlString, err := providerURL(s.config.Endpoint, v.Model)
	if err != nil {
		return nil, &providerFailure{code: "provider_invalid_configuration"}
	}
	temp, maxTokens := v.Options.Temperature, v.Options.MaxTokens
	generation := map[string]any{"temperature": temp, "maxOutputTokens": maxTokens}
	if v.Mode == "structured" {
		temp, maxTokens = .1, 2000
		generation = map[string]any{"temperature": temp, "maxOutputTokens": maxTokens, "responseMimeType": "application/json"}
		var schema any
		if json.Unmarshal(v.Schema, &schema) != nil {
			return nil, &providerFailure{code: "invalid_input"}
		}
		generation["responseJsonSchema"] = schema
	}
	payload := map[string]any{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]string{"text": v.Prompt}}}}, "generationConfig": generation}
	body, err := json.Marshal(payload)
	if err != nil || len(body) > maxRPCBody {
		return nil, &providerFailure{code: "provider_request_too_large"}
	}
	u, _ := url.Parse(urlString)
	query := u.Query()
	query.Set("key", s.config.APIKey)
	u.RawQuery = query.Encode()
	timeout := 30 * time.Second
	if v.Mode == "structured" {
		timeout = 40 * time.Second
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, &providerFailure{code: "provider_invalid_configuration"}
	}
	req.Header.Set("Content-Type", "application/json")
	client := *s.client
	client.Timeout = timeout
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return nil, &providerFailure{code: "provider_cancelled"}
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
			return nil, &providerFailure{code: "provider_timeout", retryable: true}
		}
		return nil, &providerFailure{code: "provider_transport_error", retryable: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		status := resp.StatusCode
		var after *float64
		if status == 429 {
			after = parseRetryAfter(resp.Header.Get("Retry-After"))
		}
		return nil, &providerFailure{code: "provider_http_error", status: &status, retryAfter: after}
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxRPCBody+1))
	if err != nil {
		return nil, &providerFailure{code: "provider_response_read_error"}
	}
	if len(responseBody) > maxRPCBody {
		return nil, &providerFailure{code: "provider_response_too_large"}
	}
	var raw struct {
		Candidates []struct {
			Content *struct {
				Parts []struct {
					Text *string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		Usage analysis.GeminiUsage `json:"usageMetadata"`
	}
	if json.Unmarshal(responseBody, &raw) != nil {
		return nil, &providerFailure{code: "provider_invalid_response"}
	}
	if len(raw.Candidates) == 0 {
		status := 200
		return nil, &providerFailure{code: "provider_empty_response", status: &status}
	}
	text := ""
	if raw.Candidates[0].Content != nil && len(raw.Candidates[0].Content.Parts) > 0 && raw.Candidates[0].Content.Parts[0].Text != nil {
		text = *raw.Candidates[0].Content.Parts[0].Text
	}
	usage := normalizeUsage(raw.Usage)
	var result map[string]any
	if v.Mode == "text" {
		result = map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": strings.TrimSpace(text)}}}, "usage": usage}
	} else {
		if raw.Candidates[0].Content == nil || len(raw.Candidates[0].Content.Parts) == 0 || raw.Candidates[0].Content.Parts[0].Text == nil {
			status := 200
			return nil, &providerFailure{code: "provider_empty_response", status: &status}
		}
		structured, e := parseLegacyStructured(text)
		if e != nil {
			status := 200
			return nil, &providerFailure{code: "provider_invalid_json", status: &status}
		}
		result = map[string]any{"result": structured, "usage": usage}
	}
	accountingTokens := usage["total_tokens"].(int)
	if accountingTokens == 0 {
		accountingTokens = raw.Usage.PromptTokenCount + raw.Usage.CandidatesTokenCount
	}
	return generateOutput{result: result, usage: analysis.Usage{Model: v.Model, PromptTokens: usage["prompt_tokens"].(int), CompletionTokens: usage["completion_tokens"].(int), CachedTokens: usage["cached_tokens"].(int), TotalTokens: usage["total_tokens"].(int)}, accountingTokens: accountingTokens, accountingTokenSet: true}, nil
}
func normalizeUsage(u analysis.GeminiUsage) map[string]any {
	total := 0
	if u.TotalTokenCount != nil {
		total = *u.TotalTokenCount
	} else {
		total = u.PromptTokenCount + u.CandidatesTokenCount + u.CachedContentTokenCount
	}
	return map[string]any{"prompt_tokens": u.PromptTokenCount, "completion_tokens": u.CandidatesTokenCount, "cached_tokens": u.CachedContentTokenCount, "total_tokens": total}
}

func parseLegacyStructured(response string) (any, error) {
	text := strings.TrimSpace(response)
	decode := func(raw string) (any, error) {
		var value any
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		if dec.Decode(new(any)) != io.EOF {
			return nil, errors.New("trailing_json")
		}
		return value, nil
	}
	if value, err := decode(text); err == nil {
		return value, nil
	}
	if i := strings.Index(text, "```"); i >= 0 {
		start := i + 3
		if strings.HasPrefix(strings.ToLower(text[start:]), "json") {
			start += 4
		}
		if n := strings.Index(text[start:], "```"); n >= 0 {
			text = strings.TrimSpace(text[start : start+n])
		}
	}
	if !strings.HasPrefix(text, "{") {
		start, end := strings.Index(text, "{"), strings.LastIndex(text, "}")
		if start >= 0 && end > start {
			text = text[start : end+1]
		}
	}
	return decode(text)
}

func providerURL(base, model string) (string, error) {
	if model == "" || len(model) > 128 || strings.ContainsAny(model, "/?#") {
		return "", errors.New("invalid")
	}
	u, e := url.Parse(base)
	if e != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("invalid")
	}
	if strings.Contains(strings.ToLower(model), "gemini-3") {
		u.Path = strings.Replace(u.Path, "/v1/models", "/v1beta/models", 1)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/" + url.PathEscape(model) + ":generateContent"
	return u.String(), nil
}
func parseRetryAfter(raw string) *float64 {
	if raw == "" {
		return nil
	}
	if n, e := strconv.ParseFloat(strings.TrimSpace(raw), 64); e == nil && n >= 0 && !math.IsInf(n, 0) && !math.IsNaN(n) {
		return &n
	}
	if d, e := http.ParseTime(raw); e == nil {
		n := time.Until(d).Seconds()
		if n < 0 {
			n = 0
		}
		return &n
	}
	return nil
}
func (s *Server) retry(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return nil, &providerFailure{code: "provider_timeout"}
		}
		value, err := fn(ctx)
		if err == nil {
			return value, nil
		}
		retryable := analysis.RetryableTransport(err)
		var pf *providerFailure
		if errors.As(err, &pf) {
			retryable = pf.retryable
		}
		if !retryable || attempt == 2 {
			return nil, err
		}
		wait := 4 * time.Second
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, &providerFailure{code: "provider_timeout"}
		case <-timer.C:
		}
	}
	return nil, &providerFailure{code: "analysis_failed"}
}
func writeError(w http.ResponseWriter, err error) {
	var detail map[string]any
	var pf *providerFailure
	var ae *analysis.AnalysisError
	switch {
	case errors.As(err, &pf):
		detail = errorDetail(pf.code, pf.status, pf.retryAfter, pf.retryable)
	case errors.As(err, &ae):
		detail = errorDetail(ae.Detail.Code, ae.Detail.StatusCode, ae.Detail.RetryAfterSeconds, ae.Detail.RetryableTransport)
	default:
		if strings.HasPrefix(err.Error(), "breaker_open:") {
			status := 503
			detail = errorDetail("provider_http_error", &status, nil, false)
			break
		}
		status := 503
		detail = errorDetail("analysis_failed", &status, nil, false)
	}
	// Provider status belongs in the envelope. A local status that suppresses
	// bodies (notably 204 and 304) would erase that information on the UDS hop.
	writeJSON(w, http.StatusBadGateway, map[string]any{"version": 1, "ok": false, "error": detail})
}
func errorEnvelope(code string, status int) map[string]any {
	return map[string]any{"version": 1, "ok": false, "error": map[string]any{"code": code, "status_code": status}}
}
func errorDetail(code string, status *int, after *float64, retryable bool) map[string]any {
	m := map[string]any{"code": code}
	if status != nil {
		m["status_code"] = *status
	}
	if after != nil {
		m["retry_after_seconds"] = *after
	}
	if retryable {
		m["retryable_transport"] = true
	}
	return m
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) enqueueUsage(record usageRecord) {
	s.usageMu.RLock()
	defer s.usageMu.RUnlock()
	if s.usageClosed {
		s.usageDropped.Add(1)
		return
	}
	s.usagePending.Add(1)
	select {
	case s.usage <- record:
	default:
		s.usagePending.Add(-1)
		s.usageDropped.Add(1)
	}
}
func (s *Server) StartUsageWorkers(n int) {
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		s.workers.Add(1)
		go func() {
			defer s.workers.Done()
			for rec := range s.usage {
				s.storeUsage(rec)
				s.usagePending.Add(-1)
			}
		}()
	}
}
func (s *Server) storeUsage(rec usageRecord) {
	if s.db == nil {
		return
	}
	values := usageInsert(rec, s.config)
	details, _ := json.Marshal(values["result"])
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := s.db.Exec(ctx, `INSERT INTO ai_usage_tracking (domain, analysis_type, tokens_used, credits_cost, analysis_result, created_at) VALUES ($1,$2,$3,$4,$5,$6)`, rec.domain, rec.analysisType, values["tokens_used"], values["cost"], details, time.Now().UTC()); err != nil {
		s.usageFailed.Add(1)
	}
}

func usageInsert(rec usageRecord, c Config) map[string]any {
	u := rec.usage
	tokens := rec.accountingTokens
	if !rec.accountingTokenSet {
		tokens = u.TotalTokens
		if tokens == 0 {
			tokens = u.PromptTokens + u.CompletionTokens
		}
	}
	cost := (float64(u.PromptTokens)*c.InputRate + float64(u.CompletionTokens)*c.OutputRate + float64(u.CachedTokens)*c.CachedRate) / 1_000_000
	return map[string]any{"domain": rec.domain, "analysis_type": rec.analysisType, "tokens_used": tokens, "cost": cost, "result": map[string]int{"prompt_tokens": u.PromptTokens, "completion_tokens": u.CompletionTokens, "cached_tokens": u.CachedTokens}}
}
func (s *Server) CloseUsage(timeout time.Duration) {
	s.closeOnce.Do(func() {
		s.usageMu.Lock()
		s.usageClosed = true
		close(s.usage)
		s.usageMu.Unlock()
	})
	done := make(chan struct{})
	go func() { s.workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// ServeUnix requires an already-created owner-only ephemeral directory and never
// removes a pre-existing path. The caller owns parent-directory lifecycle.
func ServeUnix(ctx context.Context, s *Server) error {
	dir := filepath.Dir(s.config.Socket)
	st, e := os.Lstat(dir)
	if e != nil {
		if s.db != nil {
			s.db.Close()
		}
		return errors.New("ai_runtime_socket_directory_unavailable")
	}
	stat, ok := st.Sys().(*syscall.Stat_t)
	if !ok || st.Mode()&os.ModeSymlink != 0 || st.Mode().Perm() != 0700 || stat.Uid != uint32(os.Geteuid()) {
		if s.db != nil {
			s.db.Close()
		}
		return errors.New("ai_runtime_socket_directory_permissions")
	}
	if _, e = os.Lstat(s.config.Socket); e == nil {
		if s.db != nil {
			s.db.Close()
		}
		return errors.New("ai_runtime_socket_path_exists")
	} else if !os.IsNotExist(e) {
		if s.db != nil {
			s.db.Close()
		}
		return errors.New("ai_runtime_socket_path_unavailable")
	}
	ln, e := net.Listen("unix", s.config.Socket)
	if e != nil {
		if s.db != nil {
			s.db.Close()
		}
		return errors.New("ai_runtime_socket_listen_failed")
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	socketInfo, e := os.Lstat(s.config.Socket)
	if e != nil {
		_ = ln.Close()
		if s.db != nil {
			s.db.Close()
		}
		return errors.New("ai_runtime_socket_unavailable")
	}
	if e = os.Chmod(s.config.Socket, 0600); e != nil {
		ln.Close()
		removeOwnedSocket(s.config.Socket, socketInfo)
		if s.db != nil {
			s.db.Close()
		}
		return errors.New("ai_runtime_socket_permissions")
	}
	s.StartUsageWorkers(1)
	s.ready.Store(true)
	httpServer := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 15 * time.Second, MaxHeaderBytes: 16 << 10}
	errCh := make(chan error, 1)
	go func() { errCh <- httpServer.Serve(ln) }()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errCh:
	}
	s.draining.Store(true)
	close(s.closing)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	shutdownErr := httpServer.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		_ = httpServer.Close()
	}
	cancel()
	s.CloseUsage(5 * time.Second)
	s.ready.Store(false)
	if s.db != nil {
		s.db.Close()
	}
	removeOwnedSocket(s.config.Socket, socketInfo)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

func removeOwnedSocket(path string, identity os.FileInfo) {
	current, err := os.Lstat(path)
	if err == nil && current.Mode()&os.ModeSocket != 0 && os.SameFile(identity, current) {
		_ = os.Remove(path)
	}
}
func OpenDatabase(ctx context.Context, c Config) (*pgxpool.Pool, error) {
	cfg, e := pgxpool.ParseConfig(c.DatabaseURL)
	if e != nil {
		return nil, errors.New("ai_runtime_database_configuration")
	}
	cfg.MaxConns = c.DBMaxConns
	if cfg.MaxConns < 1 || cfg.MaxConns > 2 {
		return nil, errors.New("ai_runtime_database_pool_configuration")
	}
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, errors.New("ai_runtime_database_unavailable")
	}
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if e = pool.Ping(pingCtx); e != nil {
		pool.Close()
		return nil, errors.New("ai_runtime_database_unavailable")
	}
	return pool, nil
}
