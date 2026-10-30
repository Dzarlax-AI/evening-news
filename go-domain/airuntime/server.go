package airuntime

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
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
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	ai "github.com/Dzarlax-AI/go-ai"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxRPCBody = 5 << 20

type Config struct {
	Prices                                                                                                       map[string]ModelPrice
	Socket, Token, DatabaseURL, APIKey, Endpoint, Model, LocalCategory                                           string
	ActiveProvider, OpenAIKey, OpenAIEndpoint, OpenAIArticleModel, OpenAIDigestModel, OpenAITextModel, TextModel string
	RPS                                                                                                          int
	InputRate, OutputRate, CachedRate                                                                            float64
	DBMaxConns                                                                                                   int32
	TelegramToken, TelegramMain, TelegramService                                                                 string
	TelegramEndpoint                                                                                             string
	DigestModel, SiteBase                                                                                        string
	TelegramRich                                                                                                 bool
}

func ConfigFromEnv() (Config, error) {
	return configFromLookup(true, os.Getenv, os.LookupEnv)
}

// DomainConfigFromEnv loads the same provider and delivery settings without
// requiring the retired Unix RPC socket and its ephemeral token.
func DomainConfigFromEnv() (Config, error) {
	return configFromLookup(false, os.Getenv, os.LookupEnv)
}

// DomainConfigFromLookup loads a managed service-scoped source without environment mutation.
func DomainConfigFromLookup(get func(string) string, lookup func(string) (string, bool)) (Config, error) {
	return configFromLookup(false, get, lookup)
}

func configFromLookup(unixRPC bool, get func(string) string, lookup func(string) (string, bool)) (Config, error) {
	c := Config{Socket: get("NEWS_AI_SOCKET"), Token: get("NEWS_AI_TOKEN"), DatabaseURL: normalizeDSN(get("DATABASE_URL")), APIKey: get("GEMINI_API_KEY"), Endpoint: get("GEMINI_API_ENDPOINT"), Model: get("SUMMARIZATION_MODEL"), LocalCategory: get("LOCAL_CATEGORY"), RPS: 3, DBMaxConns: 2}
	c.ActiveProvider = get("AI_PROVIDER")
	if c.ActiveProvider == "" {
		c.ActiveProvider = "gemini"
	}
	c.OpenAIKey = get("OPENAI_API_KEY")
	c.OpenAIEndpoint = "https://api.openai.com/v1"
	c.OpenAIArticleModel = get("OPENAI_ARTICLE_MODEL")
	c.OpenAIDigestModel = get("OPENAI_DIGEST_MODEL")
	c.OpenAITextModel = get("OPENAI_TEXT_MODEL")
	c.TextModel = get("TEXT_MODEL")
	c.Endpoint = strings.TrimSuffix(c.Endpoint, "/%s:generateContent")
	if !unixRPC && c.Endpoint == "" {
		c.Endpoint = "https://generativelanguage.googleapis.com/v1/models"
	}
	if !unixRPC && c.Model == "" {
		c.Model = "gemini-3.5-flash-lite"
	}
	c.TelegramToken = get("TELEGRAM_TOKEN")
	c.TelegramMain = get("TELEGRAM_CHAT_ID")
	c.TelegramService = get("TELEGRAM_SERVICE_CHAT_ID")
	c.TelegramEndpoint = "https://api.telegram.org"
	if get("NEWS_FIXTURE_MODE") == "1" {
		fixtureEndpoint := get("NEWS_TEST_TELEGRAM_ENDPOINT")
		u, err := url.Parse(fixtureEndpoint)
		if err != nil || u.Scheme != "http" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "fixture-services") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return c, errors.New("ai_runtime_fixture_configuration")
		}
		c.TelegramEndpoint = fixtureEndpoint
	}

	c.DigestModel = get("DIGEST_MODEL")
	c.SiteBase = get("SITE_BASE_URL")
	if !unixRPC && c.DigestModel == "" {
		c.DigestModel = "gemini-3.5-flash-lite"
	}
	if !unixRPC && c.SiteBase == "" {
		c.SiteBase = "https://news.dzarlax.dev"
	}
	c.TelegramRich = true
	if raw, present := lookup("TELEGRAM_RICH_MESSAGES_ENABLED"); present {
		switch strings.ToLower(raw) {
		case "1", "true", "yes", "on", "y", "t":
			c.TelegramRich = true
		case "0", "false", "no", "off", "n", "f":
			c.TelegramRich = false
		default:
			return c, errors.New("ai_runtime_telegram_rich_configuration")
		}
	}
	if (unixRPC && (c.Socket == "" || c.Token == "")) || c.DatabaseURL == "" || c.Endpoint == "" || c.Model == "" {
		return c, errors.New("ai_runtime_missing_configuration")
	}
	if unixRPC && (len(c.Token) < 32 || len(c.Token) > 256) {
		return c, errors.New("ai_runtime_token_configuration")
	}
	if c.ActiveProvider != "gemini" && c.ActiveProvider != "openai" {
		return c, errors.New("ai_runtime_provider_configuration")
	}
	if err := validateGeminiEndpoint(c.Endpoint, get("NEWS_FIXTURE_MODE") == "1"); err != nil {
		return c, errors.New("ai_runtime_provider_configuration")
	}
	if _, err := providerURL(c.Endpoint, c.Model); err != nil {
		return c, errors.New("ai_runtime_provider_configuration")
	}
	if raw := get("RPS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 {
			return c, errors.New("ai_runtime_rps_configuration")
		}
		c.RPS = n
	}
	if raw := get("NEWS_AI_DB_MAX_CONNS"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 2 {
			return c, errors.New("ai_runtime_database_pool_configuration")
		}
		c.DBMaxConns = int32(n)
	}
	if raw := get("AI_MODEL_PRICES"); raw != "" {
		if json.Unmarshal([]byte(raw), &c.Prices) != nil {
			return c, errors.New("ai_runtime_price_configuration")
		}
		for key, p := range c.Prices {
			if key == "" || !p.valid() {
				return c, errors.New("ai_runtime_price_configuration")
			}
		}
	}
	c.InputRate = parseRate(get("AI_INPUT_COST_PER_1M"))
	c.OutputRate = parseRate(get("AI_OUTPUT_COST_PER_1M"))
	c.CachedRate = parseRate(get("AI_CACHED_INPUT_COST_PER_1M"))
	return c, nil
}

// Only explicit fixture mode permits a local test upstream. Management settings
// never carry endpoints, so a saved profile cannot change this boundary.
func validateGeminiEndpoint(raw string, fixture bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "/v1/models" && u.Path != "/v1beta/models") {
		return errors.New("invalid_endpoint")
	}
	if u.Scheme == "https" && u.Host == "generativelanguage.googleapis.com" {
		return nil
	}
	if fixture && u.Scheme == "http" && (u.Hostname() == "127.0.0.1" || u.Hostname() == "::1" || u.Hostname() == "localhost" || u.Hostname() == "fixture-services") {
		return nil
	}
	return errors.New("invalid_endpoint")
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
	credentials   *credentialstore.Store
	config        Config
	db            *pgxpool.Pool
	limiter       *SlidingWindow
	breaker       *Breaker
	openAIBreaker *Breaker
	metadata      *MetadataStore
	client        *http.Client
	usage         chan usageRecord
	workers       sync.WaitGroup
	closing       chan struct{}
	drainOnce     sync.Once
	closeOnce     sync.Once
	mux           *http.ServeMux
	digest        *digestRegistry
	usageMu       sync.RWMutex
	usageClosed   bool
	usageFailed   atomic.Uint64
	usageDropped  atomic.Uint64
	usagePending  atomic.Int64
	ready         atomic.Bool
	draining      atomic.Bool
}

const articleAnalysisType = "combined_analysis"

type usageRecord struct {
	domain, analysisType string
	usage                analysis.Usage
	accountingTokens     int
	accountingTokenSet   bool
}

func NewServer(c Config, db *pgxpool.Pool) *Server {
	s := &Server{config: c, db: db, limiter: NewSlidingWindow(c.RPS), breaker: NewBreaker(), openAIBreaker: NewBreaker(), metadata: NewMetadataStore(db), client: &http.Client{}, usage: make(chan usageRecord, 64), closing: make(chan struct{}), mux: http.NewServeMux()}
	s.digest = &digestRegistry{entries: make(map[string]*digestReceipt)}
	s.mux.HandleFunc("POST /v2/digest/operations", s.digestAccept)
	s.mux.HandleFunc("GET /v2/digest/operations", s.digestStatus)
	s.mux.HandleFunc("GET /v1/status", s.status)
	s.mux.HandleFunc("POST /v1/analyze", s.analyze)
	s.mux.HandleFunc("POST /v1/generate", s.generate)
	s.mux.HandleFunc("POST /v1/permit", s.permit)
	s.mux.HandleFunc("POST /v1/telegram/send", s.telegramSend)
	s.mux.HandleFunc("POST /v1/categories/invalidate", s.invalidate)
	return s
}

// SetCredentialStore enables managed credential resolution before serving requests.
func (s *Server) SetCredentialStore(store *credentialstore.Store) { s.credentials = store }

func (s *Server) Handler() http.Handler { return s.auth(s.mux) }

// BeginDrain revokes direct operations. The owning server must then wait for
// in-flight calls before closing usage workers and the shared database pool.
// DirectReady reports whether the direct runtime is still accepting work.
func (s *Server) DirectReady() bool { return !s.draining.Load() }

func (s *Server) BeginDrain() {
	s.draining.Store(true)
	s.drainOnce.Do(func() { close(s.closing) })
}

// InvalidateCategories refreshes metadata after a web-side category change.
func (s *Server) InvalidateCategories() { s.metadata.Invalidate() }
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
	Title           string   `json:"title"`
	Content         string   `json:"content"`
	URL             string   `json:"url"`
	OriginalContext *string  `json:"original_context"`
	MaxTextChars    int      `json:"max_text_chars"`
	Locales         []string `json:"locales,omitempty"`
}

// ArticleInput is the direct in-process input for article analysis.
type ArticleInput struct {
	Title, Content, URL string
	OriginalContext     *string
	MaxTextChars        int
	Locales             []string
	Model               string
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
	if base.Input.MaxTextChars > 1000000 {
		writeJSON(w, 400, errorEnvelope("resource_limit", 400))
		return
	}
	result, usage, err := s.analyzeArticle(ctx, ArticleInput{Title: base.Input.Title, Content: base.Input.Content, URL: base.Input.URL, OriginalContext: base.Input.OriginalContext, MaxTextChars: base.Input.MaxTextChars, Locales: base.Input.Locales, Model: base.Model})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": map[string]any{"result": result, "usage": usage}})
}

// AnalyzeArticle shares the runtime's limiter, circuit breaker, metadata and
// usage accounting with all other operations in this process.
func (s *Server) AnalyzeArticle(ctx context.Context, input ArticleInput) (map[string]any, *analysis.Usage, error) {
	if s.draining.Load() {
		return nil, nil, &providerFailure{code: "runtime_draining"}
	}
	deadline, ok := ctx.Deadline()
	if !ok || !deadline.After(time.Now()) {
		return nil, nil, &providerFailure{code: "invalid_input"}
	}
	// Match the former bridge's maximum per-call deadline and cancel direct
	// calls as soon as the runtime begins draining.
	ctx, cancel := context.WithTimeout(ctx, 240*time.Second)
	stop := make(chan struct{})
	go func() {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		case <-stop:
		}
	}()
	defer func() { close(stop); cancel() }()
	return s.analyzeArticle(ctx, input)
}

func (s *Server) analyzeArticle(ctx context.Context, input ArticleInput) (map[string]any, *analysis.Usage, error) {
	snap, err := s.snapshot(ctx, "article")
	if err != nil {
		return nil, nil, err
	}
	input.Model = snap.model
	cats, local := s.metadata.Load(ctx)
	if input.MaxTextChars == 0 {
		input.MaxTextChars = 100000
	}
	if input.MaxTextChars > 1000000 {
		return nil, nil, &providerFailure{code: "resource_limit"}
	}
	version := 1
	if len(input.Locales) > 0 {
		version = 2
	}
	req := analysis.Request{Version: version, Locales: input.Locales, Title: input.Title, Content: input.Content, URL: input.URL, OriginalContext: input.OriginalContext, Categories: cats, LocalCategory: local, ConfiguredLocalCategory: s.config.LocalCategory, Model: input.Model, Endpoint: s.config.Endpoint, MaxTextChars: input.MaxTextChars}
	resultMap, usage, err := analysis.Analyze(ctx, req, analysis.ProviderConfig{Provider: snap.provider, Model: snap.model, Revision: snap.revision})
	if err != nil {
		return nil, nil, err
	}
	value := analysisOutput{resultMap, usage}
	result := value
	if result.usage != nil {
		s.enqueueUsage(usageRecord{domain: domainFor(input.URL), analysisType: articleAnalysisType, usage: *result.usage})
	}
	return result.result, result.usage, nil
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
	if len(req.Prompt) > maxRPCBody/2 || req.AnalysisType == "" || req.Domain == "" || len(req.AnalysisType) > 50 || len(req.Domain) > 8192 {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	snap, err := s.snapshot(ctx, "text")
	if err != nil {
		writeError(w, err)
		return
	}
	if req.Mode == "text" {
		if req.Options.Temperature < 0 || req.Options.Temperature > 2 || req.Options.MaxTokens < 1 || req.Options.MaxTokens > 100000 {
			writeJSON(w, 400, errorEnvelope("invalid_input", 400))
			return
		}
	}
	out, err := s.generateWithSnapshot(ctx, snap, req)
	if err != nil {
		writeError(w, err)
		return
	}
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
	var aie *ai.Error
	switch {
	case errors.As(err, &aie):
		code := "provider_" + aie.Kind
		if aie.Kind == "http" {
			code = "provider_http_error"
		}
		var status *int
		if aie.StatusCode != 0 {
			status = &aie.StatusCode
		}
		var after *float64
		if aie.RetryAfter > 0 {
			seconds := aie.RetryAfter.Seconds()
			after = &seconds
		}
		detail = errorDetail(code, status, after, aie.Retryable)
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

type ModelPrice struct {
	Input, Output, CachedInput *float64 `json:"-"`
}

func (p *ModelPrice) UnmarshalJSON(data []byte) error {
	var v struct {
		Input       *float64 `json:"input"`
		Output      *float64 `json:"output"`
		CachedInput *float64 `json:"cached_input"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	p.Input, p.Output, p.CachedInput = v.Input, v.Output, v.CachedInput
	return nil
}
func (p ModelPrice) valid() bool {
	for _, v := range []*float64{p.Input, p.Output, p.CachedInput} {
		if v == nil || *v < 0 || math.IsNaN(*v) || math.IsInf(*v, 0) {
			return false
		}
	}
	return true
}
func usageInsert(rec usageRecord, c Config) map[string]any {
	u := rec.usage
	var tokens any
	var cost any
	if u.Normalized == nil {
		n := rec.accountingTokens
		if !rec.accountingTokenSet {
			n = u.TotalTokens
			if n == 0 {
				n = u.PromptTokens + u.CompletionTokens
			}
		}
		tokens = n
		cost = (float64(u.PromptTokens)*c.InputRate + float64(u.CompletionTokens)*c.OutputRate + float64(u.CachedTokens)*c.CachedRate) / 1_000_000
	} else {
		n := u.Normalized
		if n.TotalTokens != nil {
			tokens = *n.TotalTokens
		} else if n.InputTokens != nil && n.OutputTokens != nil {
			tokens = *n.InputTokens + *n.OutputTokens
		}
		price, ok := c.Prices[u.Provider+"/"+u.Model]
		if !ok && u.RequestedModel != "" {
			price, ok = c.Prices[u.Provider+"/"+u.RequestedModel]
		}
		if ok && price.valid() && n.InputTokens != nil && n.OutputTokens != nil && n.CachedInputTokens != nil && *n.CachedInputTokens <= *n.InputTokens {
			cost = (float64(*n.InputTokens-*n.CachedInputTokens)**price.Input + float64(*n.CachedInputTokens)**price.CachedInput + float64(*n.OutputTokens)**price.Output) / 1_000_000
		}
	}
	var result any = map[string]int{"prompt_tokens": u.PromptTokens, "completion_tokens": u.CompletionTokens, "cached_tokens": u.CachedTokens}
	if u.Normalized != nil {
		result = map[string]any{"provider": u.Provider, "requested_model": u.RequestedModel, "model": u.Model, "profile_revision": u.ProfileRevision, "normalized_usage": u.Normalized, "cost_estimate": cost, "prompt_tokens": u.PromptTokens, "completion_tokens": u.CompletionTokens, "cached_tokens": u.CachedTokens}
	}
	return map[string]any{"domain": rec.domain, "analysis_type": rec.analysisType, "tokens_used": tokens, "cost": cost, "result": result}
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
	attention, err := s.StartupAISettings(ctx)
	if err != nil {
		if s.db != nil {
			s.db.Close()
		}
		return errors.New("ai_runtime_provider_configuration")
	}
	if attention {
		log.Print("AI settings require attention; generation unavailable until repaired")
	}
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
	s.BeginDrain()
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
