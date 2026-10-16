package transport

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/andybalholm/brotli"
	"golang.org/x/text/encoding/charmap"
)

func encodedBody(t *testing.T, encoding string, body []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	var writer io.WriteCloser
	switch encoding {
	case "gzip":
		writer = gzip.NewWriter(&output)
	case "deflate":
		writer = zlib.NewWriter(&output)
	case "br":
		writer = brotli.NewWriter(&output)
	default:
		return body
	}
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

// E02/E03: profiles match the primary and alternate Python request headers.
func TestHTTPHeadersAndStatus(t *testing.T) {
	for _, alternate := range []bool{false, true} {
		t.Run(fmt.Sprint(alternate), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				want := ChromeUserAgent
				if alternate {
					want = SafariUserAgent
				}
				if r.UserAgent() != want {
					t.Errorf("UA %q", r.UserAgent())
				}
				expected := map[string]string{
					"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
					"Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
					"Accept-Encoding": "gzip, deflate, br",
					"DNT":             "1", "Connection": "keep-alive", "Upgrade-Insecure-Requests": "1",
					"Cache-Control": "no-cache", "Pragma": "no-cache",
				}
				for key, value := range expected {
					if r.Header.Get(key) != value {
						t.Errorf("header %s=%q", key, r.Header.Get(key))
					}
				}
				io.WriteString(w, "<p>Привет UTF-8</p>")
			}))
			defer server.Close()
			doc, err := Fetch(context.Background(), server.URL, Options{AlternateHeaders: alternate})
			if err != nil {
				t.Fatal(err)
			}
			if doc.StatusCode != 200 || doc.Encoding != "utf-8" || string(doc.HTML) != "<p>Привет UTF-8</p>" {
				t.Fatalf("document %#v", doc)
			}
		})
	}
	for _, status := range []int{201, 204, 206, 301, 304, 400, 403, 404, 429, 500} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status) }))
			defer server.Close()
			if _, err := Fetch(context.Background(), server.URL, Options{}); err == nil || !strings.Contains(err.Error(), "prototype_http_status") {
				t.Fatalf("status %d: %v", status, err)
			}
		})
	}
}

// X01: compressed bodies are read through HTTP framing and decoded explicitly.
func TestHTTPEncodingAndFraming(t *testing.T) {
	body := []byte("<html><body>Привет мир. " + strings.Repeat("content ", 100) + "</body></html>")
	for _, encoding := range []string{"identity", "gzip", "deflate", "br"} {
		for _, chunked := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/chunked=%v", encoding, chunked), func(t *testing.T) {
				wire := encodedBody(t, encoding, body)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Encoding", encoding)
					w.Header().Set("Content-Type", "text/html; charset=utf-8")
					if !chunked {
						w.Header().Set("Content-Length", strconv.Itoa(len(wire)))
					}
					if chunked {
						w.(http.Flusher).Flush()
					}
					for start := 0; start < len(wire); start += 7 {
						w.Write(wire[start:min(start+7, len(wire))])
					}
				}))
				defer server.Close()
				doc, err := Fetch(context.Background(), server.URL, Options{})
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(doc.HTML, body) {
					t.Fatalf("body mismatch: %q", doc.HTML)
				}
			})
		}
	}
}

func TestHTTPRejectsMalformedEncodings(t *testing.T) {
	body := []byte("<p>" + strings.Repeat("article content ", 100) + "</p>")
	for _, encoding := range []string{"gzip", "deflate", "br"} {
		wire := encodedBody(t, encoding, body)
		for _, variant := range []struct {
			name string
			wire []byte
		}{
			{"invalid", []byte("invalid compressed bytes")},
			{"truncated", wire[:len(wire)-2]},
			{"trailing", append(append([]byte{}, wire...), []byte("trailing garbage")...)},
		} {
			t.Run(encoding+"/"+variant.name, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Encoding", encoding)
					w.Write(variant.wire)
				}))
				defer server.Close()
				if _, err := Fetch(context.Background(), server.URL, Options{}); err == nil {
					t.Fatal("accepted malformed encoding")
				}
			})
		}
	}
	for _, encoding := range []string{"compress", "gzip, br", "zstd"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", encoding)
			w.Write(body)
		}))
		_, err := Fetch(context.Background(), server.URL, Options{})
		server.Close()
		if err == nil || !strings.Contains(err.Error(), "unsupported_content_encoding") {
			t.Fatalf("encoding %s: %v", encoding, err)
		}
	}
}

func TestHTTPWireAndOutputBounds(t *testing.T) {
	for _, encoding := range []string{"identity", "gzip", "deflate", "br"} {
		for _, size := range []int{MaxPageBytes, MaxPageBytes + 1} {
			t.Run(fmt.Sprintf("%s/%d", encoding, size), func(t *testing.T) {
				wire := encodedBody(t, encoding, bytes.Repeat([]byte("x"), size))
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Encoding", encoding)
					w.(http.Flusher).Flush()
					w.Write(wire)
				}))
				defer server.Close()
				doc, err := Fetch(context.Background(), server.URL, Options{})
				if size == MaxPageBytes {
					if err != nil || len(doc.HTML) != size {
						t.Fatalf("exact cap: len=%d err=%v", len(doc.HTML), err)
					}
				} else if !errors.Is(err, extractor.ErrWireLimit) && !errors.Is(err, extractor.ErrOutputLimit) {
					t.Fatalf("over cap: %v", err)
				}
			})
		}
	}
	// A false Content-Length from a transport is never used as the body bound.
	for _, length := range []int64{-1, 1, 100000} {
		t.Run(fmt.Sprint("dishonest-length/", length), func(t *testing.T) {
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(strings.Repeat("a", 101))), ContentLength: length, Request: r}, nil
			})}
			_, err := Fetch(context.Background(), "http://127.0.0.1/", Options{Client: client, MaxWireBytes: 100})
			if !errors.Is(err, extractor.ErrWireLimit) {
				t.Fatalf("dishonest length: %v", err)
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "999")
		io.WriteString(w, "short body")
	}))
	defer server.Close()
	if _, err := Fetch(context.Background(), server.URL, Options{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated HTTP body: %v", err)
	}
}

func TestHTTPCharset(t *testing.T) {
	russian := "Привет мир. Это новостная статья на русском языке. Сегодня прекрасная погода, читатели обсуждают важные события. "
	cp1251, err := charmap.Windows1251.NewEncoder().Bytes([]byte(strings.Repeat(russian, 12)))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, contentType string
		body, want        []byte
		encoding          string
	}{
		{"declared-utf8", "text/html; charset=UTF-8", []byte(russian), []byte(russian), "utf-8"},
		{"detected-utf8", "text/html", []byte(russian), []byte(russian), "utf-8"},
		{"declared-cp1251", "text/html; charset=windows-1251", cp1251, []byte(strings.Repeat(russian, 12)), "windows-1251"},
		{"detected-cp1251", "text/html", cp1251, []byte(strings.Repeat(russian, 12)), "windows-1251"},
		{"invalid-declaration", "text/html; charset=made-up-charset", cp1251, []byte(strings.Repeat(russian, 12)), "windows-1251"},
		{"invalid-utf8-declaration", "text/html; charset=utf-8", cp1251, []byte(strings.Repeat(russian, 12)), "windows-1251"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				w.Write(test.body)
			}))
			defer server.Close()
			doc, err := Fetch(context.Background(), server.URL, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if doc.Encoding != test.encoding || !bytes.Equal(doc.HTML, test.want) {
				t.Fatalf("encoding=%q body=%q", doc.Encoding, doc.HTML)
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=windows-1251")
		w.Write(bytes.Repeat([]byte{0xc0}, 100))
	}))
	defer server.Close()
	if _, err := Fetch(context.Background(), server.URL, Options{MaxDecodedBytes: 150}); !errors.Is(err, extractor.ErrOutputLimit) {
		t.Fatalf("UTF-8 expansion bound: %v", err)
	}
}

func TestHTTPRedirectsAndLoopbackAdmission(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		if n > 0 {
			http.Redirect(w, r, "/?n="+strconv.Itoa(n-1), http.StatusFound)
			return
		}
		io.WriteString(w, "done")
	}))
	defer server.Close()
	for _, n := range []int{0, 5, 6} {
		calls.Store(0)
		doc, err := Fetch(context.Background(), server.URL+"/?n="+strconv.Itoa(n), Options{})
		if n <= 5 {
			if err != nil || string(doc.HTML) != "done" || calls.Load() != int32(n+1) {
				t.Fatalf("redirects %d: %v calls=%d", n, err, calls.Load())
			}
		} else if err == nil || calls.Load() != 6 {
			t.Fatalf("redirect cap: %v calls=%d", err, calls.Load())
		}
	}
	for _, rawURL := range []string{"https://example.com/", "http://192.168.1.1/", "http://0.0.0.0/", "http://[::]/", "http://127.0.0.1.example.com/", "ftp://127.0.0.1/", "http://user:pass@127.0.0.1/"} {
		called := false
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			called = true
			return nil, errors.New("unexpected transport")
		})}
		if _, err := Fetch(context.Background(), rawURL, Options{Client: client}); err == nil || called {
			t.Fatalf("admission %s: err=%v called=%v", rawURL, err, called)
		}
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://example.com/", http.StatusFound)
	}))
	defer redirect.Close()
	if _, err := Fetch(context.Background(), redirect.URL, Options{}); !errors.Is(err, ErrNonLoopback) {
		t.Fatalf("redirect revalidation: %v", err)
	}
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "secure") }))
	defer secure.Close()
	if doc, err := Fetch(context.Background(), secure.URL, Options{Client: secure.Client()}); err != nil || string(doc.HTML) != "secure" {
		t.Fatalf("loopback TLS: %v", err)
	}
}

func TestHTTPCancellationTimeoutAndBodyClosure(t *testing.T) {
	for _, duringBody := range []bool{false, true} {
		t.Run(fmt.Sprint("timeout-body=", duringBody), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if duringBody {
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					io.WriteString(w, "partial")
					w.(http.Flusher).Flush()
				}
				<-r.Context().Done()
			}))
			defer server.Close()
			_, err := Fetch(context.Background(), server.URL, Options{Timeout: 30 * time.Millisecond})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("timeout: %v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Fetch(ctx, "http://127.0.0.1:1/", Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	for _, status := range []int{200, 403} {
		body := &trackingBody{Reader: strings.NewReader("ok")}
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: status, Header: http.Header{}, Body: body, Request: r}, nil
		})}
		Fetch(context.Background(), "http://127.0.0.1/", Options{Client: client})
		if !body.closed {
			t.Fatalf("status %d: response body not closed", status)
		}
	}
}

func TestHTTPRejectsInvalidOptions(t *testing.T) {
	for _, opts := range []Options{
		{MaxWireBytes: -1}, {MaxDecodedBytes: -1},
		{MaxWireBytes: MaxPageBytes + 1}, {MaxDecodedBytes: MaxPageBytes + 1},
		{Timeout: -time.Second}, {Timeout: 31 * time.Second},
	} {
		if _, err := Fetch(context.Background(), "http://127.0.0.1:1/", opts); err == nil || !strings.Contains(err.Error(), "invalid_") {
			t.Fatalf("options %#v: %v", opts, err)
		}
	}
}

func TestHTTPLocalhostUsesGuardedDial(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "localhost") }))
	defer server.Close()
	localURL := strings.Replace(server.URL, "127.0.0.1", "localhost", 1)
	if doc, err := Fetch(context.Background(), localURL, Options{}); err != nil || string(doc.HTML) != "localhost" {
		t.Fatalf("localhost: %v", err)
	}
	if _, err := dialLoopback(context.Background(), "tcp", "example.com:80"); !errors.Is(err, ErrNonLoopback) {
		t.Fatalf("guarded dial: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackingBody struct {
	io.Reader
	closed bool
}

func (b *trackingBody) Close() error { b.closed = true; return nil }
