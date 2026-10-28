package media

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestPostTargetExactSourceAndSafety(t *testing.T) {
	for _, v := range []struct {
		article, source string
		ok              bool
	}{
		{"https://t.me/fixture/1", "https://t.me/s/fixture", true},
		{"https://TELEGRAM.ME/s/FIXTURE/42", "https://t.me/fixture", true},
		{"https://evil.test/fixture/1", "https://t.me/fixture", false},
		{"https://t.me@evil.test/fixture/1", "https://t.me/fixture", false},
		{"https://t.me/other/1", "https://t.me/fixture", false},
		{"https://t.me/fixture/1?single", "https://t.me/fixture", false},
		{"https://t.me/fixture/1#fragment", "https://t.me/fixture", false},
		{"https://t.me/fixture/0", "https://t.me/fixture", false},
		{"https://t.me/fixture/1/extra", "https://t.me/fixture", false},
		{"https://t.me:bad/fixture/1", "https://t.me/fixture", false},
		{"https://t.me/fixture%2F1", "https://t.me/fixture", false},
	} {
		_, _, ok := PostTarget(v.article, v.source)
		if ok != v.ok {
			t.Errorf("%s ok=%v", v.article, ok)
		}
	}
}
func TestFetchBoundedCompressedWithoutNetwork(t *testing.T) {
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	_, _ = z.Write([]byte("captured fixture"))
	_ = z.Close()
	svc := New()
	svc.HTTP = &http.Client{Transport: transportFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://t.me/s/fixture/1" || r.Header.Get("User-Agent") != "Mozilla/5.0" {
			t.Error("unexpected request")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Encoding": []string{"gzip"}}, Body: io.NopCloser(bytes.NewReader(compressed.Bytes()))}, nil
	})}
	svc.MaxBytes = 100
	got, ok, err := svc.fetch(context.Background(), "https://t.me/s/fixture/1")
	if err != nil || !ok || got != "captured fixture" {
		t.Fatalf("got %q %v %v", got, ok, err)
	}
	svc.MaxBytes = 4
	if _, _, err = svc.fetch(context.Background(), "https://t.me/s/fixture/1"); err == nil {
		t.Fatal("oversized compressed input accepted")
	}
	svc.HTTP = &http.Client{Transport: transportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 302, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if _, ok, err = svc.fetch(context.Background(), "https://t.me/s/fixture/1"); err != nil || ok {
		t.Fatalf("redirect accepted: %v %v", ok, err)
	}
}
