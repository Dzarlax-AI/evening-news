package transport

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func TestBrowserCDPEventFiltering(t *testing.T) {
	required := []string{
		"Browser.downloadWillBegin", "Page.frameAttached", "Page.frameNavigated", "Page.frameDetached",
		"Fetch.authRequired", "Fetch.requestPaused", "Network.requestWillBeSent",
		"Network.loadingFinished", "Network.loadingFailed", "Network.dataReceived", "Network.responseReceived",
		"Network.webSocketCreated", "Page.windowOpen", "Target.attachedToTarget",
		"Inspector.targetCrashed", "Target.detachedFromTarget",
	}
	for _, overload := range []bool{false, true} {
		t.Run(fmt.Sprint("required_overload=", overload), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, _, _, err := ws.UpgradeHTTP(r, w)
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				data, _, err := wsutil.ReadClientData(conn)
				if err != nil {
					return
				}
				var command struct {
					ID int64 `json:"id"`
				}
				if json.Unmarshal(data, &command) != nil {
					return
				}
				write := func(value any) bool {
					payload, err := json.Marshal(value)
					return err == nil && wsutil.WriteServerMessage(conn, ws.OpText, payload) == nil
				}
				// No consumer drains events while the command waits for its reply.
				for i := 0; i < 1024; i++ {
					method := []string{"Network.requestWillBeSentExtraInfo", "Network.responseReceivedExtraInfo",
						"Page.lifecycleEvent", "Runtime.consoleAPICalled"}[i%4]
					if overload {
						method = "Fetch.requestPaused"
					}
					if !write(map[string]any{"method": method, "sessionId": "owned-session", "params": map[string]any{}}) {
						return
					}
				}
				for _, method := range required {
					if !write(map[string]any{"method": method, "sessionId": "owned-session", "params": map[string]any{}}) {
						return
					}
				}
				if !write(map[string]any{"id": command.ID, "result": map[string]bool{"received": true}}) {
					return
				}
				// Keep the reader alive until the client closes it.
				_, _, _ = wsutil.ReadClientData(conn)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			client, err := dialBrowser(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
			if err != nil {
				t.Fatal(err)
			}
			defer client.close()
			var result struct {
				Received bool `json:"received"`
			}
			err = client.call(ctx, "", "Browser.getVersion", map[string]any{}, &result)
			if overload {
				if err == nil || !strings.Contains(err.Error(), "event_overflow") {
					t.Fatalf("required-event overload must fail closed: %v", err)
				}
				if len(client.events) != cap(client.events) {
					t.Fatal("event queue is not bounded")
				}
				return
			}
			if err != nil || !result.Received {
				t.Fatalf("irrelevant events blocked command reply: %v", err)
			}
			if len(client.events) != len(required) {
				t.Fatalf("queued %d events, want %d", len(client.events), len(required))
			}
			for _, method := range required {
				event := <-client.events
				if string(event.Method) != method || string(event.SessionID) != "owned-session" {
					t.Fatalf("required event changed: %s", event.Method)
				}
			}
		})
	}
}

func browserTestEndpoint(t *testing.T) string {
	t.Helper()
	endpoint := os.Getenv("GO_BROWSER_TEST_ENDPOINT")
	if endpoint == "" {
		if os.Getenv("GO_BROWSER_TEST_REQUIRED") == "1" {
			t.Fatal("GO_BROWSER_TEST_REQUIRED=1 requires GO_BROWSER_TEST_ENDPOINT")
		}
		t.Skip("set GO_BROWSER_TEST_ENDPOINT to a dedicated loopback Chrome debugger; tests never launch Chrome")
	}
	return endpoint
}

func TestBrowserInputBounds(t *testing.T) {
	for _, tc := range []struct {
		endpoint, page string
		opts           BrowserOptions
	}{
		{"http://example.com:9222", "http://127.0.0.1/", BrowserOptions{}},
		{"http://127.0.0.1:9222", "https://example.com/", BrowserOptions{}},
		{"http://127.0.0.1:9222", "file:///etc/passwd", BrowserOptions{}},
		{"http://127.0.0.1:9222", "http://127.0.0.1/", BrowserOptions{Timeout: 31 * time.Second}},
		{"http://127.0.0.1:9222", "http://127.0.0.1/", BrowserOptions{MaxDOMBytes: MaxPageBytes + 1}},
		{"http://127.0.0.1:9222", "http://127.0.0.1/", BrowserOptions{MaxWireBytes: -1}},
	} {
		if _, err := BrowserFetch(context.Background(), tc.endpoint, tc.page, tc.opts); err == nil {
			t.Errorf("accepted invalid input: %+v", tc)
		}
	}
}

func TestBrowserLive(t *testing.T) {
	endpoint := browserTestEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wsURL, err := browserEndpoint(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	observer, err := dialBrowser(ctx, wsURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(observer.close)
	var foreign struct {
		TargetID string `json:"targetId"`
	}
	if err := observer.call(ctx, "", "Target.createTarget", map[string]any{"url": "about:blank"}, &foreign); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		if err := observer.call(cleanup, "", "Target.closeTarget", map[string]any{"targetId": foreign.TargetID}, nil); err != nil {
			t.Error(err)
		}
	})
	baseline := browserTargets(t, observer)
	if !baseline[foreign.TargetID] {
		t.Fatal("observer foreign target missing")
	}
	var imageRequests, fontRequests, mediaRequests, websocketRequests, workerRequests, popupRequests, frameRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		switch r.URL.Path {
		case "/rendered":
			fmt.Fprint(w, `<!doctype html><html lang="en"><head><title>JS fixture</title><meta property="og:title" content="Preserved metadata"><style>@font-face{font-family:blocked;src:url('/font.woff2')}body{font-family:blocked}</style></head><body data-origin="fixture"><img src="/image.png"><audio src="/media.mp3" preload="auto"></audio><script>setTimeout(()=>{document.body.insertAdjacentHTML('beforeend','<article data-rendered="yes"><h1>Rendered article</h1><p>Text inserted by local JavaScript.</p></article>');document.body.dataset.ua=navigator.userAgent;},120)</script></body></html>`)
		case "/image.png":
			imageRequests.Add(1)
		case "/font.woff2":
			fontRequests.Add(1)
		case "/media.mp3":
			mediaRequests.Add(1)
		case "/websocket":
			fmt.Fprint(w, `<!doctype html><html><body><script>const socket = new WebSocket('ws://' + location.host + '/ws');socket.onerror=()=>{document.body.textContent='WebSocket blocked'};socket.onopen=()=>{document.body.textContent='WebSocket opened'};</script></body></html>`)
		case "/ws":
			websocketRequests.Add(1)
			http.Error(w, "unexpected websocket handshake", http.StatusBadRequest)
		case "/children":
			fmt.Fprint(w, `<!doctype html><html><body><script>
 const attempt=code=>{try{code()}catch(error){}};
 attempt(()=>new Worker(URL.createObjectURL(new Blob(["fetch('http://"+location.host+"/worker-data')"],{type:'text/javascript'}))));
 attempt(()=>new SharedWorker(URL.createObjectURL(new Blob(["fetch('http://"+location.host+"/worker-data')"],{type:'text/javascript'}))));
 attempt(()=>window.open('/popup-data'));
 attempt(()=>{const frame=document.createElement('iframe');frame.src='/frame-data';document.body.append(frame)});
 setTimeout(()=>document.body.insertAdjacentHTML('beforeend','<p>Children blocked</p>'),400);
</script></body></html>`)
		case "/worker-data":
			workerRequests.Add(1)
			fmt.Fprint(w, strings.Repeat("worker bytes", 4096))
		case "/popup-data":
			popupRequests.Add(1)
			fmt.Fprint(w, "popup")
		case "/frame-data":
			frameRequests.Add(1)
			fmt.Fprint(w, "frame")
		case "/gzip-rendered":
			w.Header().Set("Content-Encoding", "gzip")
			compressed := gzip.NewWriter(w)
			fmt.Fprint(compressed, `<!doctype html><html><body><script>document.body.textContent='Decoded gzip text'</script></body></html>`)
			_ = compressed.Close()
		case "/strict-policy":
			w.Header().Set("Content-Security-Policy", "script-src 'none'")
			fmt.Fprint(w, `<!doctype html><html><body><p>Existing policy text</p><script>document.body.dataset.escaped='yes'</script></body></html>`)
		case "/empty":
			fmt.Fprint(w, `<!doctype html><html><body></body></html>`)
		case "/large-dom":
			fmt.Fprint(w, `<!doctype html><html><body><script>document.body.textContent='界'.repeat(1000)</script></body></html>`)
		case "/huge-dom":
			fmt.Fprint(w, `<!doctype html><html><body><script>document.body.textContent='x'.repeat(6*1024*1024)</script></body></html>`)
		case "/large-wire":
			fmt.Fprint(w, `<html><body>`+strings.Repeat("wire body ", 1200)+`</body></html>`)
		case "/chunked-wire":
			fmt.Fprint(w, `<html><body>`)
			w.(http.Flusher).Flush()
			fmt.Fprint(w, strings.Repeat("wire body ", 1200)+`</body></html>`)
		case "/external":
			http.Redirect(w, r, "http://192.0.2.1/forbidden", http.StatusFound)
		default:
			http.Error(w, "missing", http.StatusNotFound)
		}
	}))
	defer server.Close()
	t.Run("javascript metadata mobile headers and blocked resources", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/rendered", BrowserOptions{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range []string{`property="og:title" content="Preserved metadata"`, `data-rendered="yes"`, `lang="en"`, `data-origin="fixture"`, `Rendered article`, `Mobile Safari`} {
			if !strings.Contains(string(doc.HTML), expected) {
				t.Errorf("missing %q in rendered HTML", expected)
			}
		}
		if doc.DOMBytes != len(doc.HTML) || doc.Encoding != "utf-8" || doc.URL != server.URL+"/rendered" || doc.StatusCode != 200 {
			t.Errorf("invalid metadata: %+v", doc)
		}
		if imageRequests.Load() != 0 || fontRequests.Load() != 0 || mediaRequests.Load() != 0 {
			t.Errorf("blocked resources reached server: images=%d fonts=%d media=%d", imageRequests.Load(), fontRequests.Load(), mediaRequests.Load())
		}
	})
	t.Run("UTF8 byte guard before transfer", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/large-dom", BrowserOptions{Timeout: 5 * time.Second, MaxDOMBytes: 1500})
		if !errors.Is(err, extractor.ErrOutputLimit) {
			t.Fatalf("expected UTF8 DOM limit, got %v", err)
		}
	})
	t.Run("WebSocket handshake blocked before request", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/websocket", BrowserOptions{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(doc.HTML), "WebSocket blocked") {
			t.Fatalf("WebSocket error callback did not render: %s", doc.HTML)
		}
		if got := websocketRequests.Load(); got != 0 {
			t.Fatalf("WebSocket handshakes reached loopback server: %d", got)
		}
	})
	t.Run("workers frames and popups blocked before child requests", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/children", BrowserOptions{Timeout: 5 * time.Second, MaxWireBytes: 4096})
		if err != nil {
			t.Fatalf("%v worker=%d popup=%d frame=%d", err, workerRequests.Load(), popupRequests.Load(), frameRequests.Load())
		}
		if !strings.Contains(string(doc.HTML), "Children blocked") {
			t.Fatal("page did not render after blocked child attempts")
		}
		if workerRequests.Load() != 0 || popupRequests.Load() != 0 || frameRequests.Load() != 0 {
			t.Fatalf("child request escaped: worker=%d popup=%d frame=%d", workerRequests.Load(), popupRequests.Load(), frameRequests.Load())
		}
		assertBrowserTargets(t, observer, baseline)
	})
	t.Run("compressed response decoded before CSP fulfillment", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/gzip-rendered", BrowserOptions{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(doc.HTML), "<body>Decoded gzip text</body>") {
			t.Fatalf("compressed response corrupted: %s", doc.HTML)
		}
	})
	t.Run("existing CSP remains enforced", func(t *testing.T) {
		doc, err := BrowserFetch(context.Background(), endpoint, server.URL+"/strict-policy", BrowserOptions{Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(doc.HTML), "Existing policy text") || strings.Contains(string(doc.HTML), `data-escaped="yes"`) {
			t.Fatalf("existing CSP was relaxed: %s", doc.HTML)
		}
	})
	t.Run("character guard before transfer", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/huge-dom", BrowserOptions{Timeout: 5 * time.Second})
		if !errors.Is(err, extractor.ErrOutputLimit) {
			t.Fatalf("expected DOM limit, got %v", err)
		}
	})
	t.Run("wire bound", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/large-wire", BrowserOptions{Timeout: 5 * time.Second, MaxWireBytes: 1024})
		if !errors.Is(err, extractor.ErrWireLimit) {
			t.Fatalf("expected wire limit, got %v", err)
		}
	})
	t.Run("chunked wire bound", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/chunked-wire", BrowserOptions{Timeout: 5 * time.Second, MaxWireBytes: 1024})
		if !errors.Is(err, extractor.ErrWireLimit) {
			t.Fatalf("expected chunked wire limit, got %v", err)
		}
	})
	t.Run("HTTP failure", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/missing", BrowserOptions{Timeout: 5 * time.Second})
		if err == nil || !strings.Contains(err.Error(), "prototype_browser_status: 404") {
			t.Fatalf("expected HTTP failure, got %v", err)
		}
	})
	t.Run("redirect isolation", func(t *testing.T) {
		_, err := BrowserFetch(context.Background(), endpoint, server.URL+"/external", BrowserOptions{Timeout: 5 * time.Second})
		if !errors.Is(err, ErrNonLoopback) {
			t.Fatalf("expected non-loopback rejection, got %v", err)
		}
	})
	t.Run("caller cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		timer := time.AfterFunc(300*time.Millisecond, cancel)
		defer timer.Stop()
		defer cancel()
		_, err := BrowserFetch(ctx, endpoint, server.URL+"/empty", BrowserOptions{Timeout: 5 * time.Second})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected caller cancellation, got %v", err)
		}
	})
	t.Run("repeated success failure deadline cleanup", func(t *testing.T) {
		for i := 0; i < 6; i++ {
			path := "/rendered"
			opts := BrowserOptions{Timeout: 3 * time.Second}
			if i%3 == 1 {
				path = "/empty"
				opts.Timeout = 250 * time.Millisecond
			}
			if i%3 == 2 {
				path = "/large-dom"
				opts.MaxDOMBytes = 1500
			}
			_, err := BrowserFetch(context.Background(), endpoint, server.URL+path, opts)
			if i%3 == 0 && err != nil {
				t.Fatalf("cycle %d: %v", i, err)
			}
			if i%3 != 0 && err == nil {
				t.Fatalf("cycle %d should fail", i)
			}
			assertBrowserTargets(t, observer, baseline)
		}
	})
	assertBrowserTargets(t, observer, baseline)
}

func browserTargets(t *testing.T, c *browserCDP) map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var response struct {
		TargetInfos []struct {
			TargetID string `json:"targetId"`
		} `json:"targetInfos"`
	}
	if err := c.call(ctx, "", "Target.getTargets", map[string]any{}, &response); err != nil {
		t.Fatal(err)
	}
	result := make(map[string]bool)
	for _, info := range response.TargetInfos {
		result[info.TargetID] = true
	}
	return result
}

func assertBrowserTargets(t *testing.T, c *browserCDP, want map[string]bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got := browserTargets(t, c)
		if fmt.Sprint(sortedTargetIDs(got)) == fmt.Sprint(sortedTargetIDs(want)) {
			return
		}
		if time.Now().After(deadline) {
			before, _ := json.Marshal(want)
			after, _ := json.Marshal(got)
			t.Fatalf("target leak or foreign target closed: baseline=%s current=%s", before, after)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func sortedTargetIDs(ids map[string]bool) string {
	// JSON marshaling sorts map keys, making the complete ID set comparable.
	encoded, _ := json.Marshal(ids)
	return string(encoded)
}
