package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestPublicBrowserRejectsInvalidMinimumSettle(t *testing.T) {
	for _, duration := range []time.Duration{-time.Nanosecond, 5*time.Second + time.Nanosecond} {
		_, err := BrowserFetchPublic(context.Background(), "", "https://example.com/", BrowserOptions{MinSettle: duration})
		if err == nil || !strings.Contains(err.Error(), "invalid_timeout") {
			t.Errorf("MinSettle=%s error=%v", duration, err)
		}
	}
}

func TestBrowserDiscoveryClosesConnectionAfterResponse(t *testing.T) {
	closed := make(chan struct{}, 2)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !r.Close {
			t.Error("discovery request did not ask the server to close the relay connection")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"webSocketDebuggerUrl":"ws://ignored.example/devtools/browser/test"}`)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	server.Start()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint, err := configuredBrowserEndpointWithResolver(ctx, server.URL, fixedResolver{[]netip.Addr{netip.MustParseAddr("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(endpoint, "/devtools/browser/test") {
		t.Fatalf("discovered websocket endpoint changed: %s", endpoint)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("discovery relay connection remained open after the response body was consumed")
	}
}

func TestNativeBrowserDiscoveryRetriesTransientSocketCloses(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) <= 2 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack discovery connection: %v", err)
				return
			}
			_ = conn.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"webSocketDebuggerUrl":"ws://ignored.example/devtools/browser/test"}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	endpoint, err := nativeBrowserEndpointWithResolver(ctx, server.URL, fixedResolver{[]netip.Addr{netip.MustParseAddr("127.0.0.1")}})
	if err != nil || !strings.HasSuffix(endpoint, "/devtools/browser/test") {
		t.Fatalf("discovery did not recover after transient socket closes: endpoint=%q err=%v", endpoint, err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("discovery GET count=%d, want two failed GETs and one successful GET", got)
	}
}

func TestNativeBrowserDiscoveryBoundsRepeatedSocketCloses(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack discovery connection: %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := nativeBrowserEndpointWithResolver(ctx, server.URL, fixedResolver{[]netip.Addr{netip.MustParseAddr("127.0.0.1")}})
	if err == nil {
		t.Fatal("discovery succeeded despite every connection closing")
	}
	if got := requests.Load(); got != nativeDiscoveryAttempts {
		t.Fatalf("discovery GET count=%d, want bounded maximum %d", got, nativeDiscoveryAttempts)
	}
}

func TestNativeBrowserDiscoveryStopsOnNonTransientResponse(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "http status", status: http.StatusServiceUnavailable, body: `{}`},
		{name: "malformed json", status: http.StatusOK, body: `{"webSocketDebuggerUrl":`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := nativeBrowserEndpointWithResolver(ctx, server.URL, fixedResolver{[]netip.Addr{netip.MustParseAddr("127.0.0.1")}})
			if err == nil || requests.Load() != 1 {
				t.Fatalf("non-transient discovery response retried: requests=%d err=%v", requests.Load(), err)
			}
		})
	}
}

func TestNativeBrowserDiscoveryBackoffUsesOriginalContext(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		attempted := make(chan struct{})
		var attempts atomic.Int32
		type result struct {
			err error
		}
		done := make(chan result, 1)
		go func() {
			_, err := retryBrowserDiscovery(ctx, func(context.Context) (string, error) {
				attempts.Add(1)
				close(attempted)
				return "", io.EOF
			})
			done <- result{err: err}
		}()
		<-attempted
		cancel()
		err := (<-done).err
		if !errors.Is(err, context.Canceled) || attempts.Load() != 1 {
			t.Fatalf("cancellation did not interrupt backoff: attempts=%d err=%v", attempts.Load(), err)
		}
	})
	t.Run("deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		var attempts atomic.Int32
		_, err := retryBrowserDiscovery(ctx, func(context.Context) (string, error) {
			attempts.Add(1)
			return "", io.EOF
		})
		if !errors.Is(err, context.DeadlineExceeded) || attempts.Load() > 1 {
			t.Fatalf("deadline did not bound retry attempts: attempts=%d err=%v", attempts.Load(), err)
		}
	})
}

func TestTransientBrowserDiscoveryErrorClassification(t *testing.T) {
	for _, err := range []error{io.EOF, io.ErrUnexpectedEOF, fmt.Errorf("dial: %w", syscall.ECONNRESET), fmt.Errorf("write: %w", syscall.EPIPE)} {
		if !transientBrowserDiscoveryError(err) {
			t.Errorf("transient discovery error was not retried: %v", err)
		}
	}
	for _, err := range []error{
		context.DeadlineExceeded,
		&net.DNSError{Err: "DNS transport reset", UnwrapErr: syscall.ECONNRESET},
		errors.Join(ErrNativeOwnershipLost, io.EOF),
		errors.Join(ErrNativePolicy, io.EOF),
		errors.New("browser_discovery_status"),
		errors.New("invalid character 'x' looking for beginning of value"),
	} {
		if transientBrowserDiscoveryError(err) {
			t.Errorf("non-transient discovery error was retried: %v", err)
		}
	}
}

func TestPublicBrowserLiveJSAndEgress(t *testing.T) {
	endpoint := browserTestEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ws, err := configuredBrowserEndpoint(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := dialBrowser(ctx, ws)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.close()
	var foreign struct {
		TargetID string `json:"targetId"`
	}
	if err := observer.call(ctx, "", "Target.createTarget", map[string]any{"url": "about:blank"}, &foreign); err != nil {
		t.Fatal(err)
	}
	defer observer.call(context.Background(), "", "Target.closeTarget", map[string]any{"targetId": foreign.TargetID}, nil)
	baseline := browserTargets(t, observer)
	var privateRequests atomic.Int32
	private := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { privateRequests.Add(1); w.Write([]byte("private")) }))
	defer private.Close()
	var requests atomic.Int32
	backend := func(ctx context.Context, raw string, opts Options, follow bool) (publicResponse, error) {
		requests.Add(1)
		if follow {
			t.Error("browser must follow redirects through intercepted navigation")
		}
		var html, mime string
		switch raw {
		case "https://fixture.example/start":
			return publicResponse{status: 302, header: http.Header{"Location": []string{"https://fixture.example/rendered"}}, body: []byte{}, wire: 0}, nil
		case "https://fixture.example/rendered":
			html = `<!doctype html><html><head><title>Public JS fixture</title></head><body><p>Loading shell</p><script>
 setTimeout(async()=>{const response=await fetch('/data');const value=await response.json();document.body.dataset.rendered=value.answer;document.body.insertAdjacentHTML('beforeend','<article>Completed article from asynchronous fetch.</article>')},120);
 const attempt=fn=>{try{fn()}catch(e){}};
 attempt(()=>fetch('` + private.URL + `/fetch'));
 attempt(()=>new WebSocket('` + strings.Replace(private.URL, "http:", "ws:", 1) + `/socket'));
 attempt(()=>new Worker(URL.createObjectURL(new Blob(["fetch('` + private.URL + `/worker')"],{type:'text/javascript'}))));
 attempt(()=>{const f=document.createElement('iframe');f.src='` + private.URL + `/frame';document.body.append(f)});
 attempt(()=>window.open('` + private.URL + `/popup'));
</script><img src="https://fixture.example/image"></body></html>`
			mime = "text/html"
		case "https://fixture.example/late":
			html = `<!doctype html><html><body><article id="first">Initial article</article><script>
setTimeout(()=>{const article=document.createElement('article');article.id='second';article.textContent='Late second article';document.body.append(article)},1300);
</script></body></html>`
			mime = "text/html"
		case "https://fixture.example/data":
			html = `{"answer":"settled"}`
			mime = "application/json"
		case "https://fixture.example/favicon.ico":
			return publicResponse{}, errors.New("no favicon")
		default:
			t.Errorf("unaccounted request reached backend: %s", raw)
			return publicResponse{}, errors.New("unexpected request")
		}
		return publicResponse{status: 200, header: http.Header{"Content-Type": []string{mime}}, body: []byte(html), wire: int64(len(html)), url: raw}, nil
	}
	got, err := browserFetchPublic(ctx, endpoint, "https://fixture.example/start", BrowserOptions{}, backend)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got.HTML), `data-rendered="settled"`) || got.URL != "https://fixture.example/rendered" {
		t.Fatalf("asynchronous DOM not captured: %+v", got)
	}
	if privateRequests.Load() != 0 {
		t.Fatalf("Chrome bypassed public policy: %d private requests", privateRequests.Load())
	}
	if requests.Load() < 3 {
		t.Fatalf("JS fetch did not run: %d calls", requests.Load())
	}
	late, err := browserFetchPublic(ctx, endpoint, "https://fixture.example/late", BrowserOptions{MinSettle: 2 * time.Second}, backend)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(late.HTML), "<article") != 2 || !strings.Contains(string(late.HTML), `id="second"`) {
		t.Fatalf("minimum settle missed delayed article: %s", string(late.HTML))
	}
	final := browserTargets(t, observer)
	if len(final) != len(baseline) || !final[foreign.TargetID] {
		t.Fatalf("owned cleanup harmed targets: baseline=%v final=%v", baseline, final)
	}

	short, stop := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer stop()
	_, err = browserFetchPublic(short, endpoint, "https://fixture.example/empty", BrowserOptions{}, func(context.Context, string, Options, bool) (publicResponse, error) {
		return publicResponse{status: 200, header: http.Header{"Content-Type": []string{"text/html"}}, body: []byte(`<html><body></body></html>`)}, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled render: %v", err)
	}
	final = browserTargets(t, observer)
	if len(final) != len(baseline) || !final[foreign.TargetID] {
		t.Fatalf("cancelled cleanup harmed targets: %v", final)
	}
}
