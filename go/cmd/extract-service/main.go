// extract-service runs the private, stateless v1 extraction API.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/pipeline"
	"github.com/Dzarlax-AI/evening-news/go/service"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

var buildVersion = "dev"

func env(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
func envInt(key string, fallback int) (int, error) {
	s := os.Getenv(key)
	if s == "" {
		return fallback, nil
	}
	n, e := strconv.Atoi(s)
	if e != nil || n < 1 || n > 100 {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return n, nil
}
func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() error {
	healthcheck := flag.Bool("healthcheck", false, "check the local readiness endpoint and exit")
	flag.Parse()
	addr := env("LISTEN_ADDR", ":8080")
	if *healthcheck {
		return checkHealth(addr)
	}
	workers, err := envInt("EXTRACTOR_MAX_CONCURRENT", 5)
	if err != nil {
		return err
	}
	browserWorkers, err := envInt("EXTRACTOR_BROWSER_CONCURRENT", 1)
	if err != nil {
		return err
	}
	endpoint := os.Getenv("CHROME_ENDPOINT")
	httpOnly, err := httpOnlyConfig(os.Getenv("EXTRACTOR_HTTP_ONLY"), endpoint)
	if err != nil {
		return err
	}
	if endpoint != "" {
		u, e := url.Parse(endpoint)
		if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("invalid CHROME_ENDPOINT")
		}
	}
	native, err := configuredNativeBrowser(httpOnly, endpoint, browserWorkers)
	if err != nil {
		return err
	}
	nativeSelector := os.Getenv("EXTRACTOR_NATIVE_READY_SELECTOR")
	if native != nil {
		probe, done := context.WithTimeout(context.Background(), 10*time.Second)
		err = native.Probe(probe)
		done()
		if err != nil {
			return errors.New("native browser capability probe failed")
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			_ = native.Close(cleanup)
		}()
	}
	browserSlots := make(chan struct{}, browserWorkers)
	engine := pipeline.Runner{HTTPOnly: httpOnly, Render: func(ctx context.Context, endpoint, rawURL string, opts transport.BrowserOptions) (transport.RenderedDocument, error) {
		select {
		case browserSlots <- struct{}{}:
			defer func() { <-browserSlots }()
		default:
			return transport.RenderedDocument{}, errors.New("browser_busy")
		}
		return transport.BrowserFetchPublic(ctx, endpoint, rawURL, opts)
	}}
	nativeReady, nativeAvailable := nativeBrowserReadiness(native)
	browserAvailable := func() bool {
		if nativeAvailable != nil {
			return nativeAvailable()
		}
		return !httpOnly && chromeReady(endpoint)
	}
	articleRunner := service.RunnerFunc(func(ctx context.Context, r service.Request) service.Response {
		if native != nil && r.UseBrowser != nil && *r.UseBrowser {
			return nativeArticleResponse(ctx, r, func(ctx context.Context, raw string, opts transport.BrowserOptions, selector string) (transport.RenderedDocument, error) {
				return native.Fetch(ctx, raw, transport.NativeBrowserOptions{BrowserOptions: opts, ReadySelector: nativeSelector})
			})
		}
		in := extractor.Input{URL: r.URL, Content: r.Content, HTML: r.HTML, Title: r.Title, SourceType: r.SourceType, Strategy: r.Strategy, Selectors: r.Selectors, LearnedSelector: r.LearnedSelector, LearnedMethod: r.LearnedMethod, UseBrowser: r.UseBrowser, MaxBytes: r.MaxBytes, MaxChars: r.MaxChars}
		requestEngine := engine
		if native != nil {
			requestEngine.HTTPOnly = true
		}
		out := requestEngine.Extract(ctx, pipeline.Request{Input: in, ChromeEndpoint: endpoint})
		return toServiceResponse(out)
	})
	telegramSource := pipeline.TelegramSource{Fetch: transport.FetchPublic}
	pageMonitorSource := pipeline.PageMonitorSource{Fetch: transport.FetchPublic, Admit: transport.AdmitPublicURL}
	pageMonitorSource.Render = func(ctx context.Context, raw string, opts transport.BrowserOptions, selector string, settle time.Duration) (transport.RenderedDocument, error) {
		select {
		case browserSlots <- struct{}{}:
			defer func() { <-browserSlots }()
		default:
			return transport.RenderedDocument{}, errors.New("browser_busy")
		}
		if native != nil {
			return native.Fetch(ctx, raw, transport.NativeBrowserOptions{BrowserOptions: opts, ReadySelector: selector, MinSettle: settle})
		}
		return transport.BrowserFetchPublic(ctx, endpoint, raw, opts)
	}
	if native != nil {
		telegramSource.Render = func(ctx context.Context, raw string, opts transport.BrowserOptions, selector string) (transport.RenderedDocument, error) {
			return native.Fetch(ctx, raw, transport.NativeBrowserOptions{BrowserOptions: opts, ReadySelector: selector})
		}
	}
	runner := runtimeRunner{article: articleRunner, telegram: telegramSource, pageMonitor: pageMonitorSource}
	var ready func() bool
	if native != nil {
		ready = nativeReady
	}
	selectorHash := ""
	if native != nil && nativeSelector != "" {
		selectorHash = fmt.Sprintf("%x", sha256.Sum256([]byte(nativeSelector)))
	}
	handler, err := service.New(service.Config{Token: os.Getenv("EXTRACTOR_SERVICE_TOKEN"), BuildVersion: buildVersion, MaxConcurrent: workers, BrowserAvailable: browserAvailable, Ready: ready, NativeBrowser: native != nil, NativeReadySelectorConfigured: native != nil && nativeSelector != "", NativeReadySelectorHash: selectorHash}, runner)
	if err != nil {
		return err
	}
	httpServer := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 36 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	finished := make(chan error, 1)
	go func() { finished <- httpServer.Serve(listener) }()
	log.Printf("extraction service listening on %s build=%s", addr, buildVersion)
	var fatal <-chan error
	if native != nil {
		fatal = native.Fatal()
	}
	select {
	case err := <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-fatal:
		_ = shutdownExtractionServer(httpServer, handler.Drain)
		return transport.ErrNativeWorkerFatal
	case <-signals.Done():
		if err = shutdownExtractionServer(httpServer, handler.Drain); err != nil {
			return err
		}
		return nil
	}
}

type runtimeRunner struct {
	article     service.Runner
	telegram    pipeline.TelegramSource
	pageMonitor pipeline.PageMonitorSource
}

func (r runtimeRunner) Run(ctx context.Context, req service.Request) service.Response {
	return r.article.Run(ctx, req)
}

func (r runtimeRunner) RunTelegram(ctx context.Context, req service.TelegramRequest) service.TelegramResponse {
	out := r.telegram.FetchChannel(ctx, req.ChannelURL, req.SourceName, req.Limit)
	resp := service.TelegramResponse{Outcome: out.Outcome, FinalURL: out.FinalURL, FetchMethod: out.AccessMethod, Articles: out.Articles}
	if out.Error != "" {
		resp.Error = &service.Failure{Code: out.Error, Retryable: out.Retryable}
	}
	return resp
}

func (r runtimeRunner) RunPageMonitor(ctx context.Context, req service.PageMonitorRequest) service.PageMonitorResponse {
	useBrowser := true
	waitForJS := true
	if req.UseBrowser != nil {
		useBrowser = *req.UseBrowser
	}
	if req.WaitForJS != nil {
		waitForJS = *req.WaitForJS
	}
	input := pipeline.PageMonitorRequest{Operation: req.Operation, URL: req.URL, SourceName: req.SourceName, Budget: time.Duration(req.BudgetMS) * time.Millisecond, Limit: req.Limit, MinTitleLength: req.MinTitleLength, UseBrowser: useBrowser, WaitForJS: waitForJS, Selectors: extractor.PageMonitorSelectors{Article: req.ArticleSelectors, Title: req.TitleSelectors, Date: req.DateSelectors, Link: req.LinkSelectors}, LearnedContainers: req.LearnedContainerSelectors, LearnedTitles: req.LearnedTitleSelectors, LearnedLinks: req.LearnedLinkSelectors, LearnedDates: req.LearnedDateSelectors, LearnedGeneral: req.LearnedGeneralSelectors}
	if req.ProvidedHTML != nil {
		input.HTML = *req.ProvidedHTML
	}
	if req.SnapshotAt != nil {
		input.SnapshotAt = *req.SnapshotAt
	}
	out := r.pageMonitor.Extract(ctx, input)
	resp := service.PageMonitorResponse{Outcome: out.Outcome, OriginalURL: out.OriginalURL, FinalURL: out.FinalURL, FetchMethod: out.FetchMethod, ContentHash: out.ContentHash, SelectorsUsed: out.SelectorsUsed, Items: out.Items, NeedsSelectorStudy: out.NeedsSelectorStudy, StudyHTML: out.StudyHTML}
	if !out.SnapshotAt.IsZero() {
		at := out.SnapshotAt
		resp.SnapshotAt = &at
	}
	if out.Error != "" {
		resp.Error = &service.Failure{Code: out.Error, Retryable: out.Retryable}
	}
	return resp
}

func shutdownExtractionServer(httpServer *http.Server, drain func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 36*time.Second)
	defer cancel()
	_ = drain(ctx)
	// Owned browser cleanup may outlive cancellation by up to five seconds.
	cleanup, stop := context.WithTimeout(context.Background(), 6*time.Second)
	defer stop()
	if err := httpServer.Shutdown(cleanup); err != nil {
		_ = httpServer.Close()
		return err
	}
	return nil
}

// Browser execution remains an explicit experimental mode. HTTP-only is the
// production default and rejects contradictory Chrome configuration at startup.
func httpOnlyConfig(raw, endpoint string) (bool, error) {
	httpOnly := true
	if raw != "" {
		if raw != "true" && raw != "false" {
			return false, errors.New("invalid EXTRACTOR_HTTP_ONLY")
		}
		httpOnly = raw == "true"
	}
	if httpOnly && endpoint != "" {
		return false, errors.New("CHROME_ENDPOINT is forbidden in HTTP-only mode")
	}
	return httpOnly, nil
}

func toServiceResponse(out pipeline.Outcome) service.Response {
	// StudyHTML is retained inside the Go pipeline for bounded alternative-page
	// recovery, but it is unused by the Python caller. Omitting it keeps a
	// near-limit captured page from expanding beyond the private response cap.
	resp := service.Response{Result: out.Result, NeedsSelectorStudy: out.NeedsSelectorStudy, Outcome: "failed"}
	for _, v := range out.Observations {
		resp.Observations = append(resp.Observations, service.Observation{Selector: v.Selector, Method: v.Method, Success: v.Success, Reason: v.Reason})
	}
	if out.Result.ExtractedFrom != "" {
		resp.FinalURL = out.Result.ExtractedFrom
	}
	code := out.Result.Error
	switch {
	case out.Result.State == "extracted":
		resp.Outcome = "success"
		return resp
	case out.NeedsSelectorStudy:
		resp.Outcome = "needs_selector_study"
		return resp
	case out.Result.State == "unsupported":
		code = "unsupported_input"
	case out.Result.State == "resource_limit" || code == "resource_limit":
		code = "resource_limit"
	case code == "invalid_url" || code == "invalid_limits" || code == "invalid_now":
		code = "invalid_input"
		resp.Outcome = "rejected"
	case code == "public_url_denied" || code == "upstream_http_rejected" || code == "upstream_forbidden" || code == "upstream_auth_required" || code == "browser_required":
		resp.Outcome = "rejected"
	case code == "transport_failed" || code == "admission_unavailable" || code == "upstream_rate_limited" || code == "upstream_unavailable" || code == "deadline_exceeded" || code == "cancelled":
	default:
		code = "no_content"
		resp.Outcome = "rejected"
	}
	retryable := code == "transport_failed" || code == "admission_unavailable" || code == "upstream_rate_limited" || code == "upstream_unavailable" || code == "deadline_exceeded"
	resp.Error = &service.Failure{Code: code, Retryable: retryable, UpstreamStatus: out.UpstreamStatus, RetryAfterSeconds: out.RetryAfterSeconds, Reason: out.Reason}
	return resp
}
func checkHealth(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/health/ready")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("readiness status %d", resp.StatusCode)
	}
	return nil
}
func chromeReady(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return transport.ProbeConfiguredBrowser(ctx, endpoint) == nil
}
