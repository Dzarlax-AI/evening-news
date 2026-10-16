package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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
