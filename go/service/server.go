package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/internal/jsonsingle"
)

type Config struct {
	Token                         string
	BuildVersion                  string
	MaxConcurrent                 int
	BrowserAvailable              func() bool
	Ready                         func() bool
	NativeBrowser                 bool
	NativeReadySelectorConfigured bool
	NativeReadySelectorHash       string
}
type Server struct {
	cfg       Config
	runner    Runner
	slots     chan struct{}
	mu        sync.Mutex
	draining  bool
	active    map[uint64]context.CancelFunc
	next      uint64
	completed uint64
	rejected  map[int]uint64
	outcomes  map[string]uint64
	methods   map[string]uint64
	latency   time.Duration
}

func New(cfg Config, runner Runner) (*Server, error) {
	if len(cfg.Token) < 16 {
		return nil, errors.New("service token must contain at least 16 bytes")
	}
	if runner == nil {
		return nil, errors.New("runner is required")
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 5
	}
	if cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 100 {
		return nil, errors.New("invalid concurrency")
	}
	if cfg.BuildVersion == "" {
		cfg.BuildVersion = "dev"
	}
	return &Server{cfg: cfg, runner: runner, slots: make(chan struct{}, cfg.MaxConcurrent), active: make(map[uint64]context.CancelFunc), rejected: make(map[int]uint64), outcomes: make(map[string]uint64), methods: make(map[string]uint64)}, nil
}
func (s *Server) ready() bool {
	s.mu.Lock()
	d := s.draining
	s.mu.Unlock()
	return !d && (s.cfg.Ready == nil || s.cfg.Ready())
}
func (s *Server) browser() bool { return s.cfg.BrowserAvailable != nil && s.cfg.BrowserAvailable() }
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health/live":
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		s.write(w, 200, map[string]any{"live": true})
	case "/health/ready":
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		status := 200
		if !s.ready() {
			status = 503
		}
		s.write(w, status, map[string]any{"ready": status == 200, "capabilities": map[string]bool{"http": true, "browser": s.browser(), "native_browser": s.cfg.NativeBrowser, "native_ready_selector": s.cfg.NativeReadySelectorConfigured}, "native_profile": map[string]string{"ready_selector_sha256": s.cfg.NativeReadySelectorHash}})
	case "/version":
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		s.write(w, 200, map[string]string{"contract_version": ContractVersion, "build_version": s.cfg.BuildVersion})
	case "/stats":
		if r.Method != "GET" {
			w.WriteHeader(405)
			return
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		s.mu.Lock()
		// Serialize under the lock to avoid exposing mutable counter maps.
		b, _ := json.Marshal(map[string]any{"completed": s.completed, "inflight": len(s.active), "admission_inflight": len(s.slots), "rejected": s.rejected, "outcomes": s.outcomes, "methods": s.methods, "latency_seconds_sum": s.latency.Seconds(), "heap_bytes": mem.HeapAlloc, "goroutines": runtime.NumGoroutine()})
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(b)
	case "/v1/extract":
		s.extract(w, r)
	case "/v1/telegram/fetch":
		s.fetchTelegram(w, r)
	case "/v1/page-monitor/extract":
		s.extractPageMonitor(w, r)
	case "/v1/sources/collect":
		s.collectSources(w, r)
	default:
		http.NotFound(w, r)
	}
}

var telegramUsername = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

func normalizeTelegramChannel(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || u.RawFragment != "" || u.ForceQuery || strings.ContainsAny(raw, "?#") {
		return "", errors.New("invalid channel url")
	}
	host := strings.ToLower(strings.TrimSuffix(u.Hostname(), "."))
	if host != "t.me" && host != "telegram.me" || (u.Port() != "" && u.Port() != "443") || u.Path == "" || u.EscapedPath() != u.Path {
		return "", errors.New("invalid channel url")
	}
	if !strings.HasPrefix(u.Path, "/") || strings.HasSuffix(u.Path, "/") {
		return "", errors.New("invalid channel url")
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	username := parts[0]
	if len(parts) == 2 && parts[0] == "s" {
		username = parts[1]
	} else if len(parts) != 1 {
		return "", errors.New("invalid channel url")
	}
	if !telegramUsername.MatchString(username) {
		return "", errors.New("invalid channel url")
	}
	return "https://t.me/s/" + strings.ToLower(username), nil
}

func (s *Server) fetchTelegram(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.authorized(r) {
		s.telegramHTTPFailure(w, http.StatusUnauthorized, "unauthorized", "", "")
		return
	}
	if !s.ready() {
		s.telegramHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", "", "")
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		s.telegramHTTPFailure(w, http.StatusTooManyRequests, "busy", "", "")
		return
	}
	var req TelegramRequest
	tooLarge, err := jsonsingle.Decode(http.MaxBytesReader(w, r.Body, 16*1024), 16*1024, &req)
	if tooLarge {
		s.telegramHTTPFailure(w, http.StatusRequestEntityTooLarge, "request_too_large", "", "")
		return
	}
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			s.telegramHTTPFailure(w, http.StatusRequestEntityTooLarge, "request_too_large", "", "")
		} else {
			s.telegramHTTPFailure(w, http.StatusBadRequest, "invalid_request", "", "")
		}
		return
	}
	originalURL := req.ChannelURL
	canonical, err := normalizeTelegramChannel(req.ChannelURL)
	if err != nil || req.ContractVersion != ContractVersion || req.RequestID == "" || len(req.RequestID) > 128 || req.SourceName == "" || len(req.SourceName) > 256 || req.Limit < 1 || req.Limit > 100 || req.BudgetMS < 1 || req.BudgetMS > 30000 {
		s.telegramHTTPFailure(w, http.StatusBadRequest, "invalid_request", req.RequestID, "")
		return
	}
	runner, ok := s.runner.(TelegramRunner)
	if !ok {
		s.telegramHTTPFailure(w, http.StatusServiceUnavailable, "telegram_unavailable", req.RequestID, canonical)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(req.BudgetMS)*time.Millisecond)
	defer cancel()
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		s.telegramHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", req.RequestID, canonical)
		return
	}
	s.next++
	id := s.next
	s.active[id] = cancel
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.active, id); s.mu.Unlock() }()
	started := time.Now()
	req.ChannelURL = canonical
	resp := runner.RunTelegram(ctx, req)
	if ctx.Err() != nil {
		code, retryable := "cancelled", false
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code, retryable = "deadline_exceeded", true
		}
		resp = TelegramResponse{Outcome: "failed", Articles: []extractor.TelegramArticle{}, Error: &Failure{Code: code, Retryable: retryable}}
	}
	resp.ContractVersion = ContractVersion
	resp.BuildVersion = s.cfg.BuildVersion
	resp.RequestID = req.RequestID
	resp.OriginalURL = originalURL
	if resp.FinalURL == "" {
		resp.FinalURL = req.ChannelURL
	}
	if resp.Articles == nil {
		resp.Articles = []extractor.TelegramArticle{}
	}
	if resp.Outcome != "success" && resp.Outcome != "partial" && resp.Outcome != "failed" && resp.Outcome != "rejected" {
		resp.Outcome = "failed"
		resp.Error = &Failure{Code: "invalid_runner_response", Retryable: false}
	}
	s.mu.Lock()
	s.completed++
	s.latency += time.Since(started)
	s.outcomes[boundedOutcome(resp.Outcome)]++
	s.methods["telegram"]++
	s.mu.Unlock()
	s.writeTelegram(w, http.StatusOK, resp)
}

func (s *Server) authorized(r *http.Request) bool {
	expected := sha256.Sum256([]byte("Bearer " + s.cfg.Token))
	actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	return subtle.ConstantTimeCompare(expected[:], actual[:]) == 1
}

func (s *Server) telegramHTTPFailure(w http.ResponseWriter, status int, code, requestID, originalURL string) {
	s.mu.Lock()
	s.rejected[status]++
	s.mu.Unlock()
	s.writeTelegram(w, status, TelegramResponse{ContractVersion: ContractVersion, BuildVersion: s.cfg.BuildVersion, RequestID: requestID, OriginalURL: originalURL, FinalURL: originalURL, Outcome: "rejected", Articles: []extractor.TelegramArticle{}, Error: &Failure{Code: code, Retryable: status == 429 || status == 503}})
}

func (s *Server) writeTelegram(w http.ResponseWriter, status int, v TelegramResponse) {
	b, err := json.Marshal(v)
	if err != nil || len(b) > MaxTelegramJSONBytes {
		status = http.StatusInternalServerError
		fallback := TelegramResponse{ContractVersion: ContractVersion, BuildVersion: v.BuildVersion, RequestID: v.RequestID, OriginalURL: v.OriginalURL, FinalURL: v.FinalURL, Outcome: "failed", Articles: []extractor.TelegramArticle{}, Error: &Failure{Code: "response_too_large", Retryable: false}}
		b, _ = json.Marshal(fallback)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
func (s *Server) fail(w http.ResponseWriter, status int, code string) {
	s.mu.Lock()
	s.rejected[status]++
	s.mu.Unlock()
	s.write(w, status, map[string]any{"contract_version": ContractVersion, "build_version": s.cfg.BuildVersion, "error": Failure{Code: code, Retryable: status == 429 || status == 503}})
}
func (s *Server) write(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil || len(b) > MaxJSONBytes {
		status = 500
		b = []byte(`{"contract_version":"1","error":{"code":"response_too_large","retryable":false}}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
func (s *Server) extract(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(405)
		return
	}
	expected := sha256.Sum256([]byte("Bearer " + s.cfg.Token))
	actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	if subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
		s.fail(w, 401, "unauthorized")
		return
	}
	if !s.ready() {
		s.fail(w, 503, "unavailable")
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		s.fail(w, 429, "busy")
		return
	}
	var req Request
	tooLarge, err := jsonsingle.Decode(http.MaxBytesReader(w, r.Body, MaxJSONBytes), MaxJSONBytes, &req)
	if tooLarge {
		s.fail(w, 413, "request_too_large")
		return
	}
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			s.fail(w, 413, "request_too_large")
		} else {
			s.fail(w, 400, "invalid_request")
		}
		return
	}
	if err = validate(&req); err != nil {
		status := 400
		if errors.Is(err, errSize) {
			status = 413
		}
		s.fail(w, status, map[bool]string{true: "request_too_large", false: "invalid_request"}[status == 413])
		return
	}
	if requiresBrowser(req) && !s.browser() {
		s.fail(w, 503, "browser_unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(req.BudgetMS)*time.Millisecond)
	defer cancel()
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		s.fail(w, 503, "unavailable")
		return
	}
	s.next++
	id := s.next
	s.active[id] = cancel
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.active, id); s.mu.Unlock() }()
	started := time.Now()
	resp := s.runner.Run(ctx, req)
	if ctx.Err() != nil {
		code := "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = "deadline_exceeded"
			if s.cfg.NativeBrowser && req.UseBrowser != nil && *req.UseBrowser {
				code = "native_browser_timeout"
			}
		}
		resp = Response{Outcome: "failed", Error: &Failure{Code: code, Retryable: code == "deadline_exceeded" || code == "native_browser_timeout"}}
	}
	resp.ContractVersion = ContractVersion
	resp.BuildVersion = s.cfg.BuildVersion
	resp.RequestID = req.RequestID
	resp.OriginalURL = req.URL
	if resp.FinalURL == "" {
		resp.FinalURL = resp.Result.URL
	}
	if resp.FinalURL == "" {
		resp.FinalURL = req.URL
	}
	if resp.Observations == nil {
		resp.Observations = []Observation{}
	}
	s.mu.Lock()
	s.completed++
	s.latency += time.Since(started)
	s.outcomes[boundedOutcome(resp.Outcome)]++
	s.methods[boundedMethod(resp.Result.MethodUsed)]++
	s.mu.Unlock()
	if len(resp.Result.Trace) > 64 {
		resp.Result.Trace = resp.Result.Trace[:64]
	}
	for i, v := range resp.Result.Trace {
		if len(v) > 512 {
			resp.Result.Trace[i] = v[:512]
		}
	}
	s.write(w, 200, resp)
}

func requiresBrowser(r Request) bool {
	return r.Strategy == "browser_rendering" || r.Strategy == "browser_heuristics" || r.Strategy == "telegram_browser" || r.Strategy == "custom_browser" || r.Strategy == "learned_pattern_browser_rendering"
}
func boundedOutcome(v string) string {
	switch v {
	case "success", "rejected", "failed", "needs_selector_study":
		return v
	}
	return "other"
}
func boundedMethod(v string) string {
	for _, method := range []string{"source_content", "enhanced_selectors", "readability", "browser_rendering", "browser_heuristics", "fallback_extraction", "json_ld", "open_graph", "meta_description", "enhanced_heuristics", "reddit_json", "reddit_linked_article"} {
		if v == method || v == "learned_pattern_"+method {
			return v
		}
	}
	return "other"
}

var errSize = errors.New("field too large")

func validate(r *Request) error {
	if r.SourceType != "" && r.SourceType != "article" && r.SourceType != "web" {
		return errors.New("unsupported source type")
	}
	switch r.Strategy {
	case "", "auto", "pipeline", "source_content", "learned_pattern", "learned_pattern_browser_rendering", "enhanced_selectors", "readability", "browser_rendering", "browser_heuristics", "json_ld", "open_graph", "meta_description", "enhanced_heuristics", "fallback_extraction", "reddit_json", "reddit_linked_article":
	default:
		return errors.New("unsupported strategy")
	}
	u, e := url.Parse(r.URL)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return errors.New("invalid url")
	}
	if r.RequestID == "" || len(r.RequestID) > 128 || len(r.URL) > 8192 || len(r.Title) > 8192 || len(r.SourceType) > 64 || len(r.Strategy) > 64 || len(r.LearnedMethod) > 128 || len(r.LearnedSelector) > 2048 {
		return errors.New("invalid field")
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = MaxBytes
	}
	if r.MaxChars == 0 {
		r.MaxChars = 100000
	}
	if r.BudgetMS == 0 {
		r.BudgetMS = 30000
	}
	if r.MaxBytes < 1 || r.MaxBytes > MaxBytes || r.MaxChars < 1 || r.MaxChars > 100000 || r.BudgetMS < 1 || r.BudgetMS > 30000 {
		return errors.New("invalid limits")
	}
	if len(r.Content)+len(r.HTML) > r.MaxBytes {
		return errSize
	}
	if len(r.Selectors) > 32 {
		return errors.New("too many selectors")
	}
	for k, v := range r.Selectors {
		if len(k) > 64 || len(v) > 16 {
			return errors.New("invalid selector")
		}
		for _, s := range v {
			if len(s) > 2048 || strings.TrimSpace(s) == "" {
				return fmt.Errorf("invalid selector")
			}
		}
	}
	return nil
}

// Drain prevents admission immediately; deadline expiration cancels all owned work.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		s.mu.Lock()
		n := len(s.active)
		s.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			s.mu.Lock()
			for _, cancel := range s.active {
				cancel()
			}
			s.mu.Unlock()
			return ctx.Err()
		case <-tick.C:
		}
	}
}
