package transport

import (
	"context"
	"errors"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

type rt func(*http.Request) (*http.Response, error)

func (f rt) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type blockedBody struct {
	ctx     context.Context
	started chan struct{}
}

func (b *blockedBody) Read([]byte) (int, error) {
	close(b.started)
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}
func (b *blockedBody) Close() error { return nil }
func TestStatusRetryAndSafeID(t *testing.T) {
	for _, status := range []int{202, 204, 429} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &http.Client{Transport: rt(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"999999999999999999999999999999999"}, "X-Request-Id": []string{"unsafe\nvalue"}}, Body: io.NopCloser(strings.NewReader("private response"))}, nil
			})}
			_, id, err := Do(context.Background(), ai.Config{HTTPClient: client}, "test", "http://fake.invalid", "Authorization", "secret", nil)
			var e *ai.Error
			if !errors.As(err, &e) || e.StatusCode != status || e.Kind != "http" || e.RetryAfter != time.Duration(math.MaxInt64) || id != "" || strings.Contains(err.Error(), "private") {
				t.Fatalf("id %q err %+v", id, err)
			}
		})
	}
}
func TestBlockedReadContext(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[timeout], func(t *testing.T) {
			started := make(chan struct{})
			var ctx context.Context
			var cancel context.CancelFunc
			if timeout {
				ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			client := &http.Client{Transport: rt(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &blockedBody{ctx: r.Context(), started: started}}, nil
			})}
			done := make(chan error, 1)
			go func() {
				_, _, err := Do(ctx, ai.Config{HTTPClient: client}, "test", "http://fake.invalid", "Authorization", "secret", nil)
				done <- err
			}()
			<-started
			if !timeout {
				cancel()
			}
			err := <-done
			expected := context.Canceled
			if timeout {
				expected = context.DeadlineExceeded
			}
			var e *ai.Error
			if !errors.Is(err, expected) || !errors.As(err, &e) || e.Retryable != timeout {
				t.Fatalf("%+v", err)
			}
		})
	}
}
func TestValidateInputs(t *testing.T) {
	cfg := ai.Config{APIKey: "key"}
	req := ai.Request{Model: "model"}
	if err := Validate(cfg, req, "test"); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"file:///tmp/a", "https://u:p@example.test", "https://example.test/?secret=x", "https://example.test/#fragment", "https://example.test?", "//example.test", "https://"} {
		c := cfg
		c.BaseURL = v
		if Validate(c, req, "test") == nil {
			t.Fatalf("accepted %q", v)
		}
	}
	for _, v := range []string{"", "with spaces", "model/path", strings.Repeat("x", 129)} {
		r := req
		r.Model = v
		if Validate(cfg, r, "test") == nil {
			t.Fatalf("accepted model %q", v)
		}
	}
	for _, v := range []float64{math.NaN(), math.Inf(1), -0.1, 2.1} {
		r := req
		r.Temperature = &v
		if Validate(cfg, r, "test") == nil {
			t.Fatalf("accepted temperature %v", v)
		}
	}
	for _, v := range []int{0, -1} {
		r := req
		r.MaxOutputTokens = &v
		if Validate(cfg, r, "test") == nil {
			t.Fatalf("accepted tokens %v", v)
		}
	}
}
