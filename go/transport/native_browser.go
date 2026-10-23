package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/chromedp/cdproto"
)

var (
	ErrNativeNotReady                 = errors.New("native_browser_not_ready")
	ErrNativeBusy                     = errors.New("native_browser_busy")
	ErrNativeAdmissionDeadline  error = nativeAdmissionDeadlineError{}
	ErrNativePolicy                   = errors.New("native_browser_policy_denied")
	ErrNativeUnsupported              = errors.New("native_browser_unsupported")
	ErrNativeTargetCrashed            = errors.New("native_browser_target_crashed")
	ErrNativeWorkerFatal              = errors.New("native_browser_worker_fatal")
	errNativeClientDisconnected       = errors.New("native_browser_client_disconnected")
	errNativeMalformedDetach          = errors.New("native_browser_malformed_detach")
	errNativeOwnedDetach              = errors.New("native_browser_owned_detach")
)

// nativeAdmissionDeadlineError reports that a request's budget expired while
// waiting for render admission. It matches ErrNativeBusy but deliberately does
// not match context.DeadlineExceeded: navigation never started in this case.
type nativeAdmissionDeadlineError struct{}

func (nativeAdmissionDeadlineError) Error() string        { return ErrNativeBusy.Error() }
func (nativeAdmissionDeadlineError) Is(target error) bool { return target == ErrNativeBusy }

func nativeAdmissionWaitError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrNativeAdmissionDeadline
	}
	return err
}

// NativeBrowserConfig is server configuration, never part of an article payload.
// NetworkIsolationVerified attests an independently verified network namespace:
// this library cannot establish firewall/UDP/DNS isolation using CDP alone.
type NativeBrowserConfig struct {
	Endpoint                 string
	ProxyURL                 string
	ProxyToken               string
	MaxConcurrent            int
	MaxWaiters               int
	NetworkIsolationVerified bool
	Diagnostic               func(stage, reason string)
}

type NativeBrowserOptions struct {
	BrowserOptions
	ReadySelector string
}

const nativeEventRPCMaxInFlight = 32

type nativeEventRPCResult struct {
	err            error
	terminal       error
	ignoreRPCError bool
}

// nativeEventRPCs bounds continuation commands while keeping policy and
// resource accounting in the single event-consumer goroutine.
type nativeEventRPCs struct {
	limit   int64
	pending atomic.Int64
	results chan nativeEventRPCResult
	wg      sync.WaitGroup
}

func newNativeEventRPCs(limit int) *nativeEventRPCs {
	return &nativeEventRPCs{limit: int64(limit), results: make(chan nativeEventRPCResult, limit)}
}

func (r *nativeEventRPCs) submit(call func() error, terminal error, ignoreRPCError bool) bool {
	for {
		current := r.pending.Load()
		if current >= r.limit {
			return false
		}
		if r.pending.CompareAndSwap(current, current+1) {
			break
		}
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		result := nativeEventRPCResult{err: call(), terminal: terminal, ignoreRPCError: ignoreRPCError}
		// The result buffer is the same size as the admission cap, so every
		// admitted call can publish exactly one completion during cancellation.
		r.results <- result
	}()
	return true
}

func (r *nativeEventRPCs) complete() { r.pending.Add(-1) }

func (r *nativeEventRPCs) join() []nativeEventRPCResult {
	r.wg.Wait()
	completed := make([]nativeEventRPCResult, 0, r.pending.Load())
	for {
		select {
		case result := <-r.results:
			r.complete()
			completed = append(completed, result)
		default:
			return completed
		}
	}
}

// NativeBrowser uses a shared externally managed browser and owns only its
// disposable contexts and targets.
// Readiness starts false and ownership loss permanently taints this generation.
// It never launches, restarts or globally closes the browser process.
type NativeBrowser struct {
	cfg                    NativeBrowserConfig
	mu                     sync.Mutex
	ready, tainted, closed bool
	operations             map[uint64]context.CancelCauseFunc
	nextOperation          uint64
	waiters                chan struct{}
	slots                  chan struct{}
	stopped                chan struct{}
	stoppedClosed          bool
	probing                bool
	idle                   chan struct{}
	generation             string
	fatal                  chan error
}

func NewNativeBrowser(cfg NativeBrowserConfig) (*NativeBrowser, error) {
	if !cfg.NetworkIsolationVerified {
		return nil, errors.New("native_browser_network_isolation_required")
	}
	endpoint, e := url.Parse(cfg.Endpoint)
	if e != nil || endpoint.Host == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Opaque != "" {
		return nil, errors.New("native_browser_invalid_endpoint")
	}
	proxy, e := url.Parse(cfg.ProxyURL)
	if e != nil || proxy.Scheme != "http" || proxy.Hostname() == "" || proxy.User != nil || (proxy.Path != "" && proxy.Path != "/") || proxy.RawQuery != "" || proxy.Fragment != "" || proxy.Opaque != "" || len(cfg.ProxyToken) < 16 || len(cfg.ProxyToken) > 4096 {
		return nil, errors.New("native_browser_invalid_proxy_configuration")
	}
	cfg.ProxyURL = strings.TrimRight(cfg.ProxyURL, "/")
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 1
	}
	if cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 2 {
		return nil, errors.New("native_browser_invalid_concurrency")
	}
	if cfg.MaxWaiters == 0 {
		cfg.MaxWaiters = 8
	}
	if cfg.MaxWaiters < 1 || cfg.MaxWaiters > 8 {
		return nil, errors.New("native_browser_invalid_waiters")
	}
	idle := make(chan struct{})
	close(idle)
	return &NativeBrowser{cfg: cfg, idle: idle, fatal: make(chan error, 1),
		operations: make(map[uint64]context.CancelCauseFunc), slots: make(chan struct{}, cfg.MaxConcurrent),
		waiters: make(chan struct{}, cfg.MaxWaiters), stopped: make(chan struct{})}, nil
}

// Fatal reports the one-way transition to a permanently tainted browser worker.
// The channel is nil only for a nil NativeBrowser receiver.
func (w *NativeBrowser) Fatal() <-chan error {
	if w == nil {
		return nil
	}
	return w.fatal
}

func (w *NativeBrowser) Ready() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ready && !w.tainted && !w.closed
}

func nativeDiagnosticReason(err error) string {
	switch {
	case errors.Is(err, errNativeClientDisconnected):
		return "client_disconnected"
	case errors.Is(err, errNativeMalformedDetach):
		return "malformed_detach"
	case errors.Is(err, errNativeOwnedDetach):
		return "owned_detach"
	case errors.Is(err, ErrNativeTargetCrashed):
		return "target_crashed"
	case errors.Is(err, context.DeadlineExceeded):
		return "context_timeout"
	case errors.Is(err, context.Canceled):
		return "context_cancelled"
	case errors.Is(err, ErrBrowserEventOverflow):
		return "event_overflow"
	case errors.Is(err, ErrNativeOwnershipLost):
		return "ownership_loss"
	case errors.Is(err, ErrNativeUnsupported):
		return "unsupported"
	case errors.Is(err, io.EOF):
		return "transport_error"
	default:
		var protocolErr *cdproto.Error
		if errors.As(err, &protocolErr) {
			if protocolErr.Code >= -999999 && protocolErr.Code <= 999999 {
				return fmt.Sprintf("protocol_error_%d", protocolErr.Code)
			}
			return "protocol_error"
		}
		var networkErr net.Error
		if errors.As(err, &networkErr) {
			return "transport_error"
		}
		return "operation_failed"
	}
}

func nativeCrashRecoveryAllowed(crashed bool, err error, connected bool, cleanupErr error) bool {
	return crashed && err == ErrNativeTargetCrashed && connected && cleanupErr == nil
}

func nativeCrashCancellation(err, cause error, crashed bool) bool {
	return crashed && errors.Is(cause, ErrNativeTargetCrashed) && err == context.Canceled
}

func (w *NativeBrowser) reportDiagnostic(stage string, err error) {
	if w.cfg.Diagnostic != nil && err != nil {
		w.cfg.Diagnostic(stage, nativeDiagnosticReason(err))
	}
}

const nativeDiscoveryAttempts = 3

func nativeBrowserEndpoint(ctx context.Context, endpoint string) (string, error) {
	return nativeBrowserEndpointWithResolver(ctx, endpoint, net.DefaultResolver)
}

func nativeBrowserEndpointWithResolver(ctx context.Context, endpoint string, resolver publicResolver) (string, error) {
	return retryBrowserDiscovery(ctx, func(ctx context.Context) (string, error) {
		return configuredBrowserEndpointWithResolver(ctx, endpoint, resolver)
	})
}

// retryBrowserDiscovery retries only transient TCP resets during the bounded
// read-only discovery GET. Every attempt and the backoff share the original
// context; callers retain control of the overall readiness or render budget.
func retryBrowserDiscovery(ctx context.Context, discover func(context.Context) (string, error)) (string, error) {
	for attempt := 1; attempt <= nativeDiscoveryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		endpoint, err := discover(ctx)
		if err == nil || !transientBrowserDiscoveryError(err) || attempt == nativeDiscoveryAttempts {
			return endpoint, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return "", ctx.Err()
		case <-timer.C:
		}
	}
	return "", errors.New("browser_discovery_attempts_exhausted")
}

func transientBrowserDiscoveryError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrNativeOwnershipLost) || errors.Is(err, ErrNativePolicy) || errors.Is(err, ErrNativeUnsupported) {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return false
	}
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

// CheckReady revalidates discovery without allocating targets or probing an
// active job. A previously verified generation may recover after a transient
// discovery failure, but only while that exact generation remains current.
func (w *NativeBrowser) CheckReady(ctx context.Context) bool {
	w.mu.Lock()
	if w.generation == "" || w.tainted || w.closed {
		w.mu.Unlock()
		return false
	}
	if len(w.operations) > 0 {
		ready := w.ready
		w.mu.Unlock()
		return ready
	}
	generation := w.generation
	w.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	ws, err := nativeBrowserEndpoint(ctx, w.cfg.Endpoint)
	if err != nil {
		w.unavailable()
		w.reportDiagnostic("discovery", err)
		return false
	}
	w.mu.Lock()
	if generation == "" || ws != generation {
		w.taintLocked()
		w.mu.Unlock()
		w.reportDiagnostic("discovery", ErrNativeOwnershipLost)
		return false
	}
	ready := !w.tainted && !w.closed && w.generation == generation &&
		(len(w.operations) == 0 || w.ready)
	if ready {
		w.ready = true
	}
	w.mu.Unlock()
	return ready
}

// begin waits for one bounded render slot using the caller's original context.
// The caller applies any navigation timeout only after this returns.
func (w *NativeBrowser) begin(ctx context.Context, probe bool) (context.Context, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nativeAdmissionWaitError(err)
	}
	w.mu.Lock()
	if probe {
		if w.ready || w.probing || len(w.operations) != 0 {
			w.mu.Unlock()
			return nil, nil, ErrNativeBusy
		}
		w.probing = true
	}
	if err := w.admissionErrorLocked(probe); err != nil {
		if probe {
			w.probing = false
		}
		w.mu.Unlock()
		return nil, nil, err
	}
	select {
	case w.slots <- struct{}{}:
		w.mu.Unlock()
	default:
		select {
		case w.waiters <- struct{}{}:
			w.mu.Unlock()
		case <-ctx.Done():
			if probe {
				w.probing = false
			}
			w.mu.Unlock()
			return nil, nil, nativeAdmissionWaitError(ctx.Err())
		default:
			if probe {
				w.probing = false
			}
			w.mu.Unlock()
			return nil, nil, ErrNativeBusy
		}
		select {
		case w.slots <- struct{}{}:
			<-w.waiters
		case <-ctx.Done():
			<-w.waiters
			if probe {
				w.mu.Lock()
				w.probing = false
				w.mu.Unlock()
			}
			return nil, nil, nativeAdmissionWaitError(ctx.Err())
		case <-w.stopped:
			<-w.waiters
			w.mu.Lock()
			err := w.admissionErrorLocked(probe)
			if probe {
				w.probing = false
			}
			w.mu.Unlock()
			if err == nil {
				err = ErrNativeNotReady
			}
			return nil, nil, err
		}
		if err := ctx.Err(); err != nil {
			if probe {
				w.mu.Lock()
				w.probing = false
				w.mu.Unlock()
			}
			<-w.slots
			return nil, nil, nativeAdmissionWaitError(err)
		}
		w.mu.Lock()
		if err := w.admissionErrorLocked(probe); err != nil {
			if probe {
				w.probing = false
			}
			w.mu.Unlock()
			<-w.slots
			return nil, nil, err
		}
		w.mu.Unlock()
	}
	ctx, cancel := context.WithCancelCause(ctx)
	w.mu.Lock()
	if err := w.admissionErrorLocked(probe); err != nil {
		if probe {
			w.probing = false
		}
		w.mu.Unlock()
		cancel(context.Canceled)
		<-w.slots
		return nil, nil, err
	}
	w.nextOperation++
	op := w.nextOperation
	if len(w.operations) == 0 {
		w.idle = make(chan struct{})
	}
	w.operations[op] = cancel
	w.mu.Unlock()
	return ctx, func() {
		cancel(context.Canceled)
		w.mu.Lock()
		delete(w.operations, op)
		<-w.slots
		if probe {
			w.probing = false
		}
		if len(w.operations) == 0 {
			close(w.idle)
		}
		w.mu.Unlock()
	}, nil
}

func (w *NativeBrowser) admissionErrorLocked(probe bool) error {
	if w.tainted {
		return ErrNativeOwnershipLost
	}
	if w.closed || (!probe && !w.ready) {
		return ErrNativeNotReady
	}
	return nil
}

func (w *NativeBrowser) taintLocked() {
	w.ready = false
	if w.closed || w.tainted {
		return
	}
	w.tainted = true
	w.stopAdmissionLocked()
	for _, cancel := range w.operations {
		cancel(ErrNativeOwnershipLost)
	}
	w.fatal <- ErrNativeWorkerFatal
	close(w.fatal)
}

func (w *NativeBrowser) taint() {
	w.mu.Lock()
	w.taintLocked()
	w.mu.Unlock()
}

func (w *NativeBrowser) unavailable() { w.mu.Lock(); w.ready = false; w.mu.Unlock() }

// Probe checks the actual configured CDP context/target/session/disposal methods.
// It performs no origin navigation and does not prove network firewall policy.
func (w *NativeBrowser) Probe(ctx context.Context) error {
	ctx, done, err := w.begin(ctx, true)
	if err != nil {
		return err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resources, err := w.open(ctx)
	if err != nil {
		return err
	}
	capabilityErr := resources.configure(ctx)
	if err = resources.cleanup(); err != nil {
		w.taint()
		return errors.Join(ErrNativeOwnershipLost, err)
	}
	if capabilityErr != nil {
		w.taint()
		return errors.Join(ErrNativeNotReady, capabilityErr)
	}
	w.mu.Lock()
	if !w.closed && !w.tainted {
		w.ready = true
		w.generation = resources.generation
	}
	w.mu.Unlock()
	return nil
}

func (w *NativeBrowser) Close(ctx context.Context) error {
	w.mu.Lock()
	w.closed = true
	w.ready = false
	w.stopAdmissionLocked()
	cancels := make([]context.CancelCauseFunc, 0, len(w.operations))
	for _, cancel := range w.operations {
		cancels = append(cancels, cancel)
	}
	idle := w.idle
	w.mu.Unlock()
	for _, cancel := range cancels {
		cancel(context.Canceled)
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *NativeBrowser) stopAdmissionLocked() {
	if !w.stoppedClosed {
		close(w.stopped)
		w.stoppedClosed = true
	}
}

type nativeResources struct {
	client                         *browserCDP
	diagnostic                     func(string, error)
	contextID, targetID, sessionID string
	mainFrameID                    string
	generation                     string
}

func (w *NativeBrowser) open(ctx context.Context) (res *nativeResources, err error) {
	ws, err := nativeBrowserEndpoint(ctx, w.cfg.Endpoint)
	if err != nil {
		w.unavailable()
		w.reportDiagnostic("discovery", err)
		return nil, errors.Join(ErrNativeNotReady, err)
	}
	w.mu.Lock()
	changed := w.generation != "" && w.generation != ws
	w.mu.Unlock()
	if changed {
		w.taint()
		w.reportDiagnostic("discovery", ErrNativeOwnershipLost)
		return nil, ErrNativeOwnershipLost
	}
	client, err := dialBrowser(ctx, ws)
	if err != nil {
		w.unavailable()
		w.reportDiagnostic("websocket", err)
		return nil, errors.Join(ErrNativeNotReady, err)
	}
	res = &nativeResources{client: client, generation: ws, diagnostic: w.reportDiagnostic}
	owned := res
	defer func() {
		if err != nil {
			w.taint()
			err = errors.Join(err, owned.cleanup())
		}
	}()
	// Capture receipts even when the caller cancels midway through allocation.
	setup, done := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer done()
	var created struct {
		ID string `json:"browserContextId"`
	}
	if err = client.callAllocation(setup, "", "Target.createBrowserContext", map[string]any{"disposeOnDetach": true, "proxyServer": w.cfg.ProxyURL, "proxyBypassList": "<-loopback>"}, &created); err != nil {
		w.reportDiagnostic("create_context", err)
		return nil, errors.Join(ErrNativeOwnershipLost, err)
	}
	res.contextID = created.ID
	if created.ID == "" {
		w.reportDiagnostic("create_context", ErrNativeOwnershipLost)
		return nil, ErrNativeOwnershipLost
	}
	var target struct {
		ID string `json:"targetId"`
	}
	if err = client.callAllocation(setup, "", "Target.createTarget", map[string]any{"url": "about:blank", "browserContextId": res.contextID, "background": true}, &target); err != nil {
		w.reportDiagnostic("create_target", err)
		return nil, errors.Join(ErrNativeOwnershipLost, err)
	}
	res.targetID = target.ID
	if target.ID == "" {
		w.reportDiagnostic("create_target", ErrNativeOwnershipLost)
		return nil, ErrNativeOwnershipLost
	}
	var attached struct {
		ID string `json:"sessionId"`
	}
	if err = client.callAllocation(setup, "", "Target.attachToTarget", map[string]any{"targetId": res.targetID, "flatten": true}, &attached); err != nil {
		w.reportDiagnostic("attach_target", err)
		return nil, errors.Join(ErrNativeOwnershipLost, err)
	}
	res.sessionID = attached.ID
	if attached.ID == "" {
		w.reportDiagnostic("attach_target", ErrNativeOwnershipLost)
		return nil, ErrNativeOwnershipLost
	}
	return res, nil
}

func (r *nativeResources) cleanup() error {
	_, err := r.cleanupWithConnectionState(false)
	return err
}

// cleanupWithConnectionState reports whether the browser connection remained
// live through disposal and both absence receipts, before its intentional close.
func (r *nativeResources) cleanupWithConnectionState(skipStopLoading bool) (bool, error) {
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	var errs []error
	if skipStopLoading {
		// The crash notification can race a following detach notification in the
		// reader goroutine. A root-session reply on this same connection orders
		// all earlier websocket frames before the drain below.
		barrierCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		var version struct {
			Product string `json:"product"`
		}
		barrierErr := r.client.callCleanup(barrierCtx, "", "Browser.getVersion", map[string]any{}, &version)
		cancel()
		if barrierErr != nil || version.Product == "" {
			errs = append(errs, ErrNativeOwnershipLost, barrierErr)
		}
		if err := r.crashIntegrityError(); err != nil {
			errs = append(errs, err)
		}
	}
	if r.sessionID != "" && !skipStopLoading {
		// Navigation may already have stopped after a denied redirect. Disposal
		// and the two absence receipts below establish ownership cleanup.
		_ = r.client.callCleanup(ctx, r.sessionID, "Page.stopLoading", map[string]any{}, nil)
	}
	if r.contextID != "" {
		// Dispose the owned context atomically. Closing its target first races
		// disposal on the pinned Cloak build; disposal closes all child targets.
		errs = append(errs, r.client.callCleanup(ctx, "", "Target.disposeBrowserContext", map[string]any{"browserContextId": r.contextID}, nil))
	} else if r.targetID != "" {
		errs = append(errs, r.client.callCleanup(ctx, "", "Target.closeTarget", map[string]any{"targetId": r.targetID}, nil))
	}
	var contexts struct {
		IDs []string `json:"browserContextIds"`
	}
	if err := r.client.callCleanup(ctx, "", "Target.getBrowserContexts", map[string]any{}, &contexts); err != nil {
		errs = append(errs, err)
	} else {
		for _, id := range contexts.IDs {
			if id == r.contextID {
				errs = append(errs, ErrNativeOwnershipLost)
			}
		}
	}
	var targets struct {
		Infos []struct {
			ID        string `json:"targetId"`
			ContextID string `json:"browserContextId"`
		} `json:"targetInfos"`
	}
	if err := r.client.callCleanup(ctx, "", "Target.getTargets", map[string]any{}, &targets); err != nil {
		errs = append(errs, err)
	} else {
		for _, t := range targets.Infos {
			if t.ID == r.targetID || (r.contextID != "" && t.ContextID == r.contextID) {
				errs = append(errs, ErrNativeOwnershipLost)
			}
		}
	}
	connectionHealthy := r.client.connected() && r.client.eventError() == nil
	if !connectionHealthy {
		errs = append(errs, ErrNativeOwnershipLost)
	}
	r.client.close()
	if err := r.client.eventError(); err != nil {
		errs = append(errs, err)
	}
	joined := errors.Join(errs...)
	if joined != nil && r.diagnostic != nil {
		r.diagnostic("cleanup", joined)
	}
	return connectionHealthy, joined
}

// crashIntegrityError drains events already received before cleanup begins so
// an owned detach or event overflow cannot be mistaken for a recoverable crash.
func (r *nativeResources) crashIntegrityError() error {
	var errs []error
	if !r.client.connected() {
		errs = append(errs, ErrNativeOwnershipLost)
	}
	if err := r.client.eventError(); err != nil {
		errs = append(errs, err, ErrNativeOwnershipLost)
	}
	for drained := 0; drained < cap(r.client.events); drained++ {
		select {
		case message := <-r.client.events:
			if detached, malformed := detachedOwnedSession(message, r.sessionID); malformed {
				errs = append(errs, ErrNativeOwnershipLost, ErrNativePolicy, errNativeMalformedDetach)
			} else if detached {
				errs = append(errs, ErrNativeOwnershipLost, errNativeOwnedDetach)
			}
		default:
			return errors.Join(errs...)
		}
	}
	return errors.Join(append(errs, ErrNativeOwnershipLost)...)
}

// verifyCrashRecovery performs a non-allocating CDP health check against the
// already verified browser generation. It is called only after owned cleanup.
func (w *NativeBrowser) verifyCrashRecovery(generation string) error {
	w.mu.Lock()
	eligible := !w.closed && !w.tainted && len(w.operations) > 0 && w.generation == generation
	w.mu.Unlock()
	if !eligible {
		return ErrNativeOwnershipLost
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	current, err := nativeBrowserEndpoint(ctx, w.cfg.Endpoint)
	if err != nil || current != generation {
		return errors.Join(ErrNativeOwnershipLost, err)
	}
	client, err := dialBrowser(ctx, current)
	if err != nil {
		return errors.Join(ErrNativeOwnershipLost, err)
	}
	var version struct {
		Product string `json:"product"`
	}
	err = client.call(ctx, "", "Browser.getVersion", map[string]any{}, &version)
	if err == nil && version.Product == "" {
		err = ErrNativeOwnershipLost
	}
	if err == nil {
		err = client.eventError()
	}
	client.close()
	if err != nil {
		return errors.Join(ErrNativeOwnershipLost, err)
	}
	current, err = nativeBrowserEndpoint(ctx, w.cfg.Endpoint)
	if err != nil || current != generation {
		return errors.Join(ErrNativeOwnershipLost, err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || w.tainted || len(w.operations) == 0 || w.generation != generation {
		return ErrNativeOwnershipLost
	}
	w.ready = true
	return nil
}

func (r *nativeResources) configure(ctx context.Context) error {
	for _, command := range []struct {
		method string
		params any
	}{
		{"Page.enable", map[string]any{}},
		{"Network.enable", map[string]any{"maxPostDataSize": 1}},
		{"Network.setBlockedURLs", map[string]any{"urls": []string{"http://*", "ws://*", "wss://*", "file://*", "ftp://*"}}},
		{"Browser.setDownloadBehavior", map[string]any{"behavior": "deny", "browserContextId": r.contextID, "eventsEnabled": true}},
		{"Target.setAutoAttach", map[string]any{"autoAttach": true, "waitForDebuggerOnStart": true, "flatten": true}},
		{"Fetch.enable", map[string]any{"handleAuthRequests": true, "patterns": []any{map[string]any{"urlPattern": "*", "requestStage": "Request"}}}},
	} {
		session := r.sessionID
		if command.method == "Browser.setDownloadBehavior" {
			session = ""
		}
		if err := r.client.call(ctx, session, command.method, command.params, nil); err != nil {
			if r.diagnostic != nil {
				r.diagnostic("configure", err)
			}
			return err
		}
	}
	var tree struct {
		FrameTree struct {
			Frame struct {
				ID string `json:"id"`
			} `json:"frame"`
		} `json:"frameTree"`
	}
	if err := r.client.call(ctx, r.sessionID, "Page.getFrameTree", map[string]any{}, &tree); err != nil {
		if r.diagnostic != nil {
			r.diagnostic("configure", err)
		}
		return err
	}
	if tree.FrameTree.Frame.ID == "" {
		err := errors.New("native_browser_main_frame_unavailable")
		if r.diagnostic != nil {
			r.diagnostic("configure", err)
		}
		return err
	}
	r.mainFrameID = tree.FrameTree.Frame.ID
	return nil
}

func nativeURL(raw string) error {
	if err := ValidatePublicURL(raw); err != nil {
		return err
	}
	u, _ := url.Parse(raw)
	if u.Scheme != "https" || (u.Port() != "" && u.Port() != "443") {
		return ErrNativeUnsupported
	}
	return nil
}

func nativeProxyChallenge(origin string, cfg NativeBrowserConfig) bool {
	u, e := url.Parse(origin)
	if e != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return false
	}
	p, _ := url.Parse(cfg.ProxyURL)
	port := func(u *url.URL) string {
		if u.Port() != "" {
			return u.Port()
		}
		return "80"
	}
	return u.Scheme == p.Scheme && strings.EqualFold(u.Hostname(), p.Hostname()) && port(u) == port(p)
}

// Fetch leaves all origin request bytes, TLS, headers, cookies and bodies in the
// browser. It never fulfills responses or overrides UA, hints or viewport.
func (w *NativeBrowser) Fetch(ctx context.Context, raw string, opts NativeBrowserOptions) (doc RenderedDocument, err error) {
	if err = nativeURL(raw); err != nil {
		return doc, err
	}
	wireLimit, err := pageLimit(opts.MaxWireBytes)
	if err != nil {
		return doc, err
	}
	domLimit, err := pageLimit(opts.MaxDOMBytes)
	if err != nil {
		return doc, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	if timeout < 0 || timeout > 30*time.Second || opts.MinSettle < 0 || opts.MinSettle > 5*time.Second || len(opts.ReadySelector) > 2048 {
		return doc, errors.New("native_browser_invalid_options")
	}
	ctx, done, err := w.begin(ctx, false)
	if err != nil {
		return doc, err
	}
	defer done()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := w.open(ctx)
	if err != nil {
		return doc, err
	}
	var targetCrashed atomic.Bool
	diagnosticStage := "configure"
	defer func() {
		connected, cleanupErr := res.cleanupWithConnectionState(targetCrashed.Load())
		if cleanupErr != nil {
			w.taint()
			doc = RenderedDocument{}
			err = errors.Join(err, ErrNativeOwnershipLost, cleanupErr)
			return
		}
		if targetCrashed.Load() {
			doc = RenderedDocument{}
			if !nativeCrashRecoveryAllowed(true, err, connected, cleanupErr) {
				w.taint()
				err = errors.Join(err, ErrNativeOwnershipLost)
				return
			}
			if recoveryErr := w.verifyCrashRecovery(res.generation); recoveryErr != nil {
				w.taint()
				err = errors.Join(err, ErrNativeOwnershipLost, recoveryErr)
			}
		}
	}()
	client, session := res.client, res.sessionID
	if err = res.configure(ctx); err != nil {
		w.taint()
		return doc, errors.Join(ErrNativeNotReady, err)
	}
	diagnosticStage = "navigate"
	work, stop := context.WithCancelCause(ctx)
	var status atomic.Int64
	var mainFrame atomic.Value
	mainFrame.Store(res.mainFrameID)
	var lastActivity atomic.Int64
	lastActivity.Store(time.Now().UnixNano())
	var activeRequests atomic.Int64
	eventsDone := make(chan struct{})
	eventRPCs := newNativeEventRPCs(nativeEventRPCMaxInFlight)
	go func() {
		defer close(eventsDone)
		terminalPending := false
		handleRPCResult := func(result nativeEventRPCResult) bool {
			if result.err != nil && !result.ignoreRPCError {
				stop(result.err)
				eventRPCs.complete()
				return true
			}
			if result.terminal != nil {
				stop(result.terminal)
				eventRPCs.complete()
				return true
			}
			eventRPCs.complete()
			return false
		}
		var wire float64
		requests := map[string]bool{}
		documentFrames := map[string]string{}
		encodedByRequest := map[string]float64{}
		ownedFrames := map[string]bool{res.mainFrameID: true}
		totalRequests, totalAuth := 0, 0
		authenticated := map[string]bool{}
		for {
			// Prefer completions so a busy event queue cannot starve RPC slots.
			select {
			case result := <-eventRPCs.results:
				if handleRPCResult(result) {
					return
				}
			default:
			}
			select {
			case <-work.Done():
				return
			case result := <-eventRPCs.results:
				if handleRPCResult(result) {
					return
				}
			case <-client.eventFailure:
				cause := client.eventError()
				stop(cause)
				w.taint()
				return
			case <-client.done:
				stop(errors.Join(ErrNativeOwnershipLost, errNativeClientDisconnected))
				w.taint()
				return
			case message := <-client.events:
				method := string(message.Method)
				if detached, malformed := detachedOwnedSession(message, session); malformed {
					stop(errors.Join(ErrNativeOwnershipLost, ErrNativePolicy, errNativeMalformedDetach))
					w.taint()
					return
				} else if detached {
					stop(errors.Join(ErrNativeOwnershipLost, errNativeOwnedDetach))
					w.taint()
					return
				}
				if terminalPending && (method == "Fetch.authRequired" || method == "Fetch.requestPaused") {
					continue
				}
				if method == "Browser.downloadWillBegin" {
					var event struct {
						FrameID string `json:"frameId"`
					}
					if json.Unmarshal(message.Params, &event) == nil && (ownedFrames[event.FrameID] || event.FrameID == mainFrame.Load().(string)) {
						stop(ErrNativeUnsupported)
						return
					}
				}
				if string(message.SessionID) != session {
					continue
				}
				lastActivity.Store(time.Now().UnixNano())
				switch method {
				case "Page.frameAttached":
					var event struct {
						ID     string `json:"frameId"`
						Parent string `json:"parentFrameId"`
					}
					if json.Unmarshal(message.Params, &event) == nil && (ownedFrames[event.Parent] || event.Parent == mainFrame.Load().(string)) {
						ownedFrames[event.ID] = true
						if len(ownedFrames) > 128 {
							stop(extractor.ErrWireLimit)
							return
						}
					}
				case "Page.frameNavigated":
					var event struct {
						Frame struct {
							ID     string `json:"id"`
							Parent string `json:"parentId"`
						} `json:"frame"`
					}
					if json.Unmarshal(message.Params, &event) == nil && event.Frame.ID != "" && event.Frame.Parent == "" {
						mainFrame.Store(event.Frame.ID)
						ownedFrames[event.Frame.ID] = true
					}
				case "Page.frameDetached":
					var event struct {
						ID string `json:"frameId"`
					}
					if json.Unmarshal(message.Params, &event) == nil {
						delete(ownedFrames, event.ID)
					}
				case "Fetch.authRequired":
					totalAuth++
					if totalAuth > 256 {
						stop(extractor.ErrWireLimit)
						return
					}
					var event struct {
						ID        string                          `json:"requestId"`
						Challenge struct{ Source, Origin string } `json:"authChallenge"`
					}
					if json.Unmarshal(message.Params, &event) != nil {
						stop(ErrNativePolicy)
						return
					}
					response := map[string]any{"response": "CancelAuth"}
					allowed := event.Challenge.Source == "Proxy" && nativeProxyChallenge(event.Challenge.Origin, w.cfg) && !authenticated[event.ID]
					if allowed {
						response = map[string]any{"response": "ProvideCredentials", "username": "extractor", "password": w.cfg.ProxyToken}
						authenticated[event.ID] = true
					}
					var terminal error
					if !allowed {
						code := 401
						if event.Challenge.Source == "Proxy" {
							code = 407
						}
						terminal = &PublicHTTPStatusError{StatusCode: code}
						terminalPending = true
					}
					if !eventRPCs.submit(func() error {
						return client.call(work, session, "Fetch.continueWithAuth", map[string]any{"requestId": event.ID, "authChallengeResponse": response}, nil)
					}, terminal, false) {
						stop(extractor.ErrWireLimit)
						return
					}
				case "Fetch.requestPaused":
					var event struct {
						ID      string `json:"requestId"`
						Request struct {
							URL string `json:"url"`
						} `json:"request"`
					}
					if json.Unmarshal(message.Params, &event) != nil {
						stop(ErrNativePolicy)
						return
					}
					if policyErr := nativeURL(event.Request.URL); policyErr != nil {
						terminalPending = true
						if !eventRPCs.submit(func() error {
							return client.call(work, session, "Fetch.failRequest", map[string]any{"requestId": event.ID, "errorReason": "BlockedByClient"}, nil)
						}, policyErr, true) {
							stop(extractor.ErrWireLimit)
							return
						}
						continue
					}
					if !eventRPCs.submit(func() error {
						return client.call(work, session, "Fetch.continueRequest", map[string]any{"requestId": event.ID}, nil)
					}, nil, false) {
						stop(extractor.ErrWireLimit)
						return
					}
				case "Network.requestWillBeSent":
					var event struct {
						ID      string `json:"requestId"`
						FrameID string `json:"frameId"`
						Type    string `json:"type"`
					}
					if json.Unmarshal(message.Params, &event) == nil {
						totalRequests++
						if totalRequests > 128 {
							stop(extractor.ErrWireLimit)
							return
						}
						if event.Type == "Document" {
							documentFrames[event.ID] = event.FrameID
						}
						if requests[event.ID] {
							continue
						}
						requests[event.ID] = true
						activeRequests.Add(1)
						if len(requests) > 128 {
							stop(extractor.ErrWireLimit)
							return
						}
					}
				case "Network.loadingFinished", "Network.loadingFailed":
					var event struct {
						ID      string  `json:"requestId"`
						Encoded float64 `json:"encodedDataLength"`
					}
					if json.Unmarshal(message.Params, &event) == nil && requests[event.ID] {
						wire += max(0, event.Encoded-encodedByRequest[event.ID])
						delete(encodedByRequest, event.ID)
						if wire > float64(wireLimit) {
							stop(extractor.ErrWireLimit)
							return
						}
						delete(requests, event.ID)
						activeRequests.Add(-1)
					}
				case "Network.dataReceived":
					var event struct {
						ID      string  `json:"requestId"`
						Encoded float64 `json:"encodedDataLength"`
					}
					if json.Unmarshal(message.Params, &event) == nil {
						wire += event.Encoded
						encodedByRequest[event.ID] += event.Encoded
						if wire > float64(wireLimit) {
							stop(extractor.ErrWireLimit)
							return
						}
					}
				case "Network.responseReceived":
					var event struct {
						Type     string `json:"type"`
						ID       string `json:"requestId"`
						FrameID  string `json:"frameId"`
						Response struct {
							Status  int            `json:"status"`
							Headers map[string]any `json:"headers"`
						} `json:"response"`
					}
					if json.Unmarshal(message.Params, &event) != nil {
						stop(ErrNativePolicy)
						return
					}
					frameID := event.FrameID
					if frameID == "" {
						frameID = documentFrames[event.ID]
					}
					if event.Type == "Document" && frameID != "" && frameID == mainFrame.Load().(string) {
						status.Store(int64(event.Response.Status))
						var retry *int
						for name, value := range event.Response.Headers {
							if strings.EqualFold(name, "Retry-After") {
								retry = parseRetryAfter(fmt.Sprint(value), time.Now())
							}
						}
						if event.Response.Status != 200 {
							stop(&PublicHTTPStatusError{StatusCode: event.Response.Status, RetryAfterSeconds: retry})
							return
						}
					}
				case "Network.webSocketCreated", "Page.windowOpen", "Target.attachedToTarget":
					stop(ErrNativeUnsupported)
					return
				case "Inspector.targetCrashed":
					targetCrashed.Store(true)
					w.unavailable()
					stop(ErrNativeTargetCrashed)
					return
				}
			}
		}
	}()
	defer func() {
		stop(nil)
		<-eventsDone
		for _, result := range eventRPCs.join() {
			if result.err != nil && !result.ignoreRPCError {
				if targetCrashed.Load() {
					if !nativeCrashCancellation(result.err, context.Cause(work), true) {
						err = errors.Join(err, result.err)
					}
				} else if err == nil {
					err = result.err
				}
			}
			if result.terminal != nil {
				if targetCrashed.Load() {
					err = errors.Join(err, result.terminal)
				} else if err == nil {
					err = result.terminal
				}
			}
		}
		if targetCrashed.Load() {
			if err == nil {
				err = ErrNativeTargetCrashed
			} else if !errors.Is(err, ErrNativeTargetCrashed) {
				err = errors.Join(err, ErrNativeTargetCrashed)
			}
		}
		if err != nil {
			doc = RenderedDocument{}
		}
		stage := diagnosticStage
		if cause := context.Cause(work); cause != nil &&
			!errors.Is(cause, context.DeadlineExceeded) && !errors.Is(cause, context.Canceled) {
			stage = "events"
		}
		w.reportDiagnostic(stage, err)
	}()
	var navigation struct {
		Error      string `json:"errorText"`
		FrameID    string `json:"frameId"`
		IsDownload bool   `json:"isDownload"`
	}
	if err = client.call(work, session, "Page.navigate", map[string]any{"url": raw}, &navigation); err != nil {
		return doc, browserClientCause(work, client, err)
	}
	if navigation.IsDownload {
		return doc, ErrNativeUnsupported
	}
	if navigation.Error != "" {
		if cause := context.Cause(work); cause != nil {
			return doc, cause
		}
		return doc, errors.New("native_browser_navigation_failed")
	}
	if navigation.FrameID != "" {
		mainFrame.Store(navigation.FrameID)
	}
	diagnosticStage = "dom"
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	selector, _ := json.Marshal(opts.ReadySelector)
	started, stableSince := time.Now(), time.Now()
	previous := ""
	for {
		expression := fmt.Sprintf(`(() => {
 if (!document.body || document.readyState !== 'complete') return {ready:false};
 if (document.body.textContent.length > %d) return {ready:true,tooLarge:true};
 const html=document.documentElement.outerHTML;
 if (html.length>%d) return {ready:true,tooLarge:true};
 const bytes=new TextEncoder().encode(html).length;
 if (bytes>%d) return {ready:true,tooLarge:true};
 const selector=%s;
 if (selector && !document.querySelector(selector)) return {ready:false};
 if (!document.body.innerText.trim()) return {ready:false};
 return {ready:true,html:html,bytes:bytes,url:location.href};
})()`, domLimit, domLimit, domLimit, selector)
		var evaluated struct {
			Result struct {
				Value struct {
					Ready, TooLarge bool
					HTML            string `json:"html"`
					Bytes           int    `json:"bytes"`
					URL             string `json:"url"`
				} `json:"value"`
			} `json:"result"`
			Exception json.RawMessage `json:"exceptionDetails"`
		}
		if err = client.call(work, session, "Runtime.evaluate", map[string]any{"expression": expression, "returnByValue": true}, &evaluated); err != nil {
			return doc, browserClientCause(work, client, err)
		}
		if len(evaluated.Exception) > 0 {
			return doc, errors.New("native_browser_evaluation_failed")
		}
		value := evaluated.Result.Value
		if value.Ready {
			if cause := client.eventError(); cause != nil {
				stop(cause)
				w.taint()
				return doc, cause
			}
			if value.TooLarge || int64(len(value.HTML)) > domLimit || int64(value.Bytes) > domLimit {
				return doc, extractor.ErrOutputLimit
			}
			if err = nativeURL(value.URL); err != nil {
				return doc, err
			}
			if value.HTML != previous {
				previous = value.HTML
				stableSince = time.Now()
			}
			minimumWait := max(500*time.Millisecond, opts.MinSettle)
			if time.Since(started) >= minimumWait && time.Since(stableSince) >= 250*time.Millisecond && time.Since(time.Unix(0, lastActivity.Load())) >= 250*time.Millisecond && activeRequests.Load() == 0 && eventRPCs.pending.Load() == 0 && status.Load() == 200 {
				if cause := context.Cause(work); cause != nil {
					return doc, cause
				}
				return RenderedDocument{Document: Document{HTML: []byte(value.HTML), URL: value.URL, StatusCode: 200, Encoding: "utf-8"}, DOMBytes: value.Bytes}, nil
			}
		}
		select {
		case <-work.Done():
			return doc, context.Cause(work)
		case <-ticker.C:
		}
	}
}
