package processing

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

type AIClient struct {
	URL, Token string
	HTTP       *http.Client
}
type ExtractorClient struct {
	URL, Token         string
	HTTP               *http.Client
	MaxBytes, MaxChars int
	UseBrowser         bool
	BrowserMaxBytes    int
	AttemptTimeout     time.Duration
	TotalBudget        time.Duration
	admission          chan struct{}
	waiterMu           sync.Mutex
	waiters            int
}
type AIResult struct {
	Version          int            `json:"version"`
	RunID            string         `json:"run_id"`
	ArticleID        int64          `json:"article_id"`
	ArticleAttempt   int            `json:"article_attempt"`
	InputFingerprint string         `json:"input_fingerprint"`
	Operation        string         `json:"operation"`
	Result           map[string]any `json:"result"`
}

func NewAIClient(url, token string) *AIClient {
	return &AIClient{URL: strings.TrimRight(url, "/"), Token: token, HTTP: &http.Client{Timeout: 0, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}
func NewExtractorClient(url, token string) *ExtractorClient {
	return &ExtractorClient{URL: strings.TrimRight(url, "/"), Token: token, HTTP: &http.Client{Timeout: 0, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, admission: make(chan struct{}, 1)}
}
func InputFingerprint(input map[string]any) string {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	_ = e.Encode(input)
	raw := bytes.TrimSuffix(b.Bytes(), []byte("\n"))
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
func (c *AIClient) Call(ctx context.Context, f store.Fence, a store.Article, attempt int, fp, op string, input map[string]any) (map[string]any, error) {
	body := map[string]any{"version": 1, "run_id": f.RunID, "envelope": f.Envelope, "article_id": a.ID, "article_attempt": attempt, "input_fingerprint": fp, "operation": op, "input": input}
	wire, err := json.Marshal(body)
	if err != nil {
		return nil, errors.New("article ai request encode failed")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/internal/v1/article-ai", bytes.NewReader(wire))
	if err != nil {
		return nil, errors.New("article ai request failed")
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, errors.New("article ai unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return map[string]any{"_processing_error": "busy", "_retry_after_seconds": retrySeconds(resp.Header.Get("Retry-After"))}, nil
	}
	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		return nil, ErrResourceLimit
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity {
		return nil, fatal("article_ai_rejected")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.New("article ai rejected request")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024+1))
	if err != nil || len(data) > 5*1024*1024 {
		return nil, errors.New("article ai response invalid")
	}
	var out AIResult
	if json.Unmarshal(data, &out) != nil || out.Version != 1 || out.RunID != f.RunID || out.ArticleID != a.ID || out.ArticleAttempt != attempt || out.InputFingerprint != fp || out.Operation != op {
		return nil, fatal("article_ai_protocol")
	}
	return out.Result, nil
}
func retrySeconds(raw string) any {
	if raw == "" {
		return nil
	}
	return raw
}

type ExtractorObservation struct {
	Selector string `json:"selector"`
	Method   string `json:"method"`
	Success  bool   `json:"success"`
}
type Extracted struct {
	Content           string                 `json:"content"`
	Title             string                 `json:"title"`
	PublicationDate   string                 `json:"publication_date"`
	ImageURL          string                 `json:"image_url"`
	MediaFiles        []map[string]any       `json:"media_files"`
	Method            string                 `json:"method"`
	MethodUsed        string                 `json:"method_used"`
	SelectorUsed      string                 `json:"selector_used"`
	State             string                 `json:"state"`
	Error             string                 `json:"error"`
	Outcome           string                 `json:"outcome"`
	RetryAfterSeconds any                    `json:"retry_after_seconds"`
	Observations      []ExtractorObservation `json:"-"`
	Retryable         bool                   `json:"-"`
	Terminal          bool                   `json:"-"`
}

func (c *ExtractorClient) Extract(ctx context.Context, rawURL, title, sourceType, selector, method string, maxChars int) (Extracted, error) {
	maxBytes := c.MaxBytes
	if maxBytes < 1 || maxBytes > 5*1024*1024 {
		maxBytes = 5 * 1024 * 1024
	}
	totalBudget := c.TotalBudget
	if totalBudget <= 0 || totalBudget > 10*time.Minute {
		totalBudget = 120 * time.Second
	}
	totalCtx, cancel := context.WithTimeout(ctx, totalBudget)
	defer cancel()
	firstSelector, firstMethod := selector, method
	if browserLearnedMethod(method) {
		firstSelector, firstMethod = "", ""
	}
	result, err := c.admittedAttempt(totalCtx, rawURL, title, sourceType, firstSelector, firstMethod, maxChars, false, maxBytes)
	if err != nil {
		return result, err
	}
	for busyRetries := 0; result.Error == "busy" && busyRetries < 8; busyRetries++ {
		delay, ok := retryAfterDuration(result.RetryAfterSeconds)
		deadline, hasDeadline := totalCtx.Deadline()
		if !ok || (hasDeadline && time.Until(deadline) < delay) {
			return result, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-totalCtx.Done():
			timer.Stop()
			return Extracted{Error: "busy", Retryable: true}, nil
		case <-timer.C:
		}
		result, err = c.admittedAttempt(totalCtx, rawURL, title, sourceType, firstSelector, firstMethod, maxChars, false, maxBytes)
		if err != nil {
			return result, err
		}
	}
	if result.Error == "busy" || !c.UseBrowser || !browserEligible(rawURL) || !nativeBrowserRetryAllowed(result.Error) {
		return result, nil
	}
	browserBytes := c.BrowserMaxBytes
	if browserBytes <= 0 || browserBytes > 5*1024*1024 {
		browserBytes = maxBytes
	}
	return c.admittedAttempt(totalCtx, rawURL, title, sourceType, selector, method, maxChars, true, browserBytes)
}

func (c *ExtractorClient) admittedAttempt(ctx context.Context, rawURL, title, sourceType, selector, method string, maxChars int, useBrowser bool, maxBytes int) (Extracted, error) {
	if c.admission == nil {
		c.admission = make(chan struct{}, 1)
	}
	select {
	case c.admission <- struct{}{}:
	default:
		c.waiterMu.Lock()
		if c.waiters >= 8 {
			c.waiterMu.Unlock()
			return Extracted{Error: "busy", Retryable: true}, nil
		}
		c.waiters++
		c.waiterMu.Unlock()
		select {
		case c.admission <- struct{}{}:
		case <-ctx.Done():
			c.waiterMu.Lock()
			c.waiters--
			c.waiterMu.Unlock()
			return Extracted{Error: "busy", Retryable: true}, nil
		}
		c.waiterMu.Lock()
		c.waiters--
		c.waiterMu.Unlock()
	}
	defer func() { <-c.admission }()
	attempt := c.AttemptTimeout
	if attempt <= 0 || attempt > 30*time.Second {
		attempt = 30 * time.Second
	}
	requestCtx, cancel := context.WithTimeout(ctx, attempt)
	defer cancel()
	return c.extractOnce(requestCtx, rawURL, title, sourceType, selector, method, maxChars, useBrowser, maxBytes)
}

func (c *ExtractorClient) extractOnce(ctx context.Context, rawURL, title, sourceType, selector, method string, maxChars int, useBrowser bool, maxBytes int) (Extracted, error) {
	if maxChars < 1 || maxChars > 100000 {
		maxChars = 100000
	}
	budget := 30 * time.Second
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < budget {
		budget = time.Until(deadline)
	}
	if budget <= 0 {
		return Extracted{}, errors.New("extractor deadline exceeded")
	}
	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return Extracted{}, errors.New("extractor request id unavailable")
	}
	requestID := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", idBytes[0:4], idBytes[4:6], idBytes[6:8], idBytes[8:10], idBytes[10:16])
	body := map[string]any{"request_id": requestID, "url": rawURL, "strategy": "auto", "source_type": "article", "budget_ms": max(1, min(30000, int(budget.Milliseconds()))), "max_bytes": maxBytes, "max_chars": maxChars, "title": title, "use_browser": useBrowser}
	if selector != "" || method != "" {
		body["learned_selector"] = selector
		body["learned_method"] = method
	}
	wire, err := json.Marshal(body)
	if err != nil || len(wire) > maxBytes {
		return Extracted{}, errors.New("extractor request too large")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/v1/extract", bytes.NewReader(wire))
	if err != nil {
		return Extracted{}, errors.New("extractor request failed")
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return Extracted{}, errors.New("extractor unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == 429 {
			return Extracted{Error: "busy", Retryable: true, RetryAfterSeconds: retrySeconds(resp.Header.Get("Retry-After"))}, nil
		}
		if resp.StatusCode == http.StatusRequestEntityTooLarge {
			return Extracted{}, ErrResourceLimit
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnprocessableEntity {
			return Extracted{}, fatal("extractor_rejected")
		}
		retryable := resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusGatewayTimeout
		return Extracted{Error: "http_error", Retryable: retryable}, nil
	}
	if enc := strings.ToLower(resp.Header.Get("Content-Encoding")); enc != "" && enc != "identity" {
		return Extracted{}, fatal("extractor_protocol")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(maxBytes)+1))
	if err != nil || len(data) > maxBytes {
		return Extracted{}, ErrResourceLimit
	}
	var envelope struct {
		ContractVersion string `json:"contract_version"`
		RequestID       string `json:"request_id"`
		OriginalURL     string `json:"original_url"`
		FinalURL        string `json:"final_url"`
		Outcome         string `json:"outcome"`
		Result          struct {
			Content         string           `json:"content"`
			Title           string           `json:"title"`
			PublicationDate string           `json:"publication_date"`
			ImageURL        string           `json:"image_url"`
			Media           []map[string]any `json:"media"`
			Method          string           `json:"method"`
			MethodUsed      string           `json:"method_used"`
			SelectorUsed    string           `json:"selector_used"`
		} `json:"result"`
		Error *struct {
			Code              string `json:"code"`
			Retryable         *bool  `json:"retryable"`
			RetryAfterSeconds *int   `json:"retry_after_seconds"`
		} `json:"error"`
		Observations []ExtractorObservation `json:"observations"`
	}
	if json.Unmarshal(data, &envelope) != nil || envelope.ContractVersion != "1" || envelope.RequestID != requestID || envelope.OriginalURL != rawURL || !validOutcome(envelope.Outcome) || len(envelope.Observations) > 64 {
		return Extracted{}, fatal("extractor_protocol")
	}
	out := Extracted{Content: envelope.Result.Content, Title: envelope.Result.Title, PublicationDate: envelope.Result.PublicationDate, ImageURL: envelope.Result.ImageURL, MediaFiles: envelope.Result.Media, Method: envelope.Result.Method, MethodUsed: envelope.Result.MethodUsed, SelectorUsed: envelope.Result.SelectorUsed, Outcome: envelope.Outcome, Observations: envelope.Observations}
	if out.MethodUsed == "" {
		out.MethodUsed = out.Method
	}
	if envelope.Error != nil {
		if envelope.Error.Code == "" || envelope.Error.Retryable == nil {
			return Extracted{}, fatal("extractor_protocol")
		}
		out.Error = envelope.Error.Code
		out.Retryable = *envelope.Error.Retryable
		if envelope.Error.RetryAfterSeconds != nil {
			if *envelope.Error.RetryAfterSeconds < 0 || *envelope.Error.RetryAfterSeconds > 86400 {
				return Extracted{}, fatal("extractor_protocol")
			}
			out.RetryAfterSeconds = *envelope.Error.RetryAfterSeconds
		}
	}
	if utf8.RuneCountInString(out.Content) > maxChars {
		return Extracted{}, ErrResourceLimit
	}
	if out.Outcome == "success" && strings.TrimSpace(out.Content) == "" {
		return Extracted{}, fatal("extractor_protocol")
	}
	if out.Error == "" && out.Outcome != "success" {
		switch out.Outcome {
		case "rejected":
			out.Error = "no_content"
		case "needs_selector_study":
			out.Error = "needs_selector_study"
		default:
			out.Error = "extractor_failed"
		}
	}
	out.Terminal = terminalExtractorError(out.Error)
	for _, m := range out.MediaFiles {
		if _, ok := m["type"].(string); !ok {
			return Extracted{}, fatal("extractor_protocol")
		}
		if _, ok := m["url"].(string); !ok {
			return Extracted{}, fatal("extractor_protocol")
		}
	}
	return out, nil
}
func validOutcome(v string) bool {
	return v == "success" || v == "rejected" || v == "failed" || v == "needs_selector_study"
}

func retryAfterDuration(v any) (time.Duration, bool) {
	var seconds float64
	switch n := v.(type) {
	case string:
		parsed, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, false
		}
		seconds = parsed
	case int:
		seconds = float64(n)
	case float64:
		seconds = n
	default:
		return 0, false
	}
	if seconds < 0 || seconds > 86400 {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)), true
}

func browserEligible(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(u.Scheme, "https") || u.Hostname() == "" {
		return false
	}
	return u.Port() == "" || u.Port() == "443"
}

func browserLearnedMethod(method string) bool {
	switch method {
	case "browser_rendering", "browser_heuristics", "learned_pattern_browser_rendering", "learned_pattern_browser_heuristics":
		return true
	default:
		return false
	}
}

func nativeBrowserRetryAllowed(code string) bool {
	switch code {
	case "browser_required", "no_content", "needs_selector_study", "upstream_forbidden", "challenge", "invalid_content":
		return true
	default:
		return false
	}
}

func terminalExtractorError(code string) bool {
	switch code {
	case "resource_limit", "invalid_input", "unsupported_input", "request_too_large", "invalid_request", "public_url_denied", "upstream_auth_required", "native_browser_policy_denied", "native_browser_unsupported", "native_browser_challenge", "native_browser_no_content", "native_browser_http_rejected":
		return true
	default:
		return false
	}
}
