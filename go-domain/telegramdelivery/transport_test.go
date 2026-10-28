package telegramdelivery

import (
	"context"
	"io"
	"net/http"
	"strings"

	"sync/atomic"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type permits struct{ count atomic.Int32 }

func (p *permits) Acquire(context.Context) error { p.count.Add(1); return nil }
func TestSendClassificationAndNoRetry(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   State
	}{
		{"success", 200, `{"ok":true,"result":{"message_id":42}}`, Sent},
		{"malformed", 200, `{`, Unknown}, {"empty", 200, ``, Unknown},
		{"rejected", 400, `{"ok":false}`, Rejected}, {"unauthorized", 401, `{"ok":false}`, Rejected},
		{"rate limited", 429, `{"ok":false,"parameters":{"retry_after":3}}`, Rejected},
		{"server error", 500, `{"ok":false}`, Unknown}, {"redirect", 302, `{"ok":false}`, Unknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var posts atomic.Int32
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				posts.Add(1)
				return &http.Response{StatusCode: tt.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tt.body)), Request: r}, nil
			})}
			p := &permits{}
			result := (Transport{Endpoint: "https://fixture.invalid", Token: "test", Admission: p, Client: client}).Send(context.Background(), "sendMessage", map[string]string{"chat_id": "fixture", "text": "synthetic"})
			if result.State != tt.want || posts.Load() != 1 || p.count.Load() != 1 {
				t.Fatalf("result=%+v posts=%d permits=%d", result, posts.Load(), p.count.Load())
			}
			if tt.name == "success" && result.MessageID != 42 {
				t.Fatalf("message ID: %d", result.MessageID)
			}
			if tt.name == "rate limited" && (result.RetryAfterSeconds == nil || *result.RetryAfterSeconds != 3) {
				t.Fatal("missing retry-after")
			}
		})
	}
}

func TestRetryAfterFiniteNonnegative(t *testing.T) {
	for _, tc := range []struct {
		body, header string
		want         *float64
	}{
		{`{"ok":false,"parameters":{"retry_after":-1}}`, "+Inf", nil},
		{`{"ok":false}`, "NaN", nil},
		{`{"ok":false}`, "-1", nil},
		{`{"ok":false}`, "2", ptr(2)},
	} {
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{tc.header}}, Body: io.NopCloser(strings.NewReader(tc.body)), Request: r}, nil
		})}
		got := (Transport{Client: client, Endpoint: "https://fixture.invalid", Token: "test"}).Send(context.Background(), "sendMessage", struct{}{})
		if (got.RetryAfterSeconds == nil) != (tc.want == nil) || got.RetryAfterSeconds != nil && *got.RetryAfterSeconds != *tc.want {
			t.Fatalf("got %+v", got)
		}
	}
}
func ptr(v float64) *float64 { return &v }
