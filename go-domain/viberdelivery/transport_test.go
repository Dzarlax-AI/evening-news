package viberdelivery

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestSendOutcomes(t *testing.T) {
	for _, tc := range []struct {
		body  string
		code  int
		want  State
		token string
	}{
		{`{"status":0,"message_token":9223372036854775807123}`, 200, Sent, "9223372036854775807123"},
		{`{"status":0,"message_token":"123"}`, 200, Sent, "123"},
		{`{"status":0}`, 200, Unknown, ""}, {`{"status":0,"message_token":"-1"}`, 200, Unknown, ""}, {`{"status":0,"message_token":1e3}`, 200, Unknown, ""}, {`{"status":0,"message_token":"0"}`, 200, Unknown, ""}, {`{`, 200, Unknown, ""}, {`{"status":12}`, 200, Rejected, ""},
		{`{}`, 429, Rejected, ""}, {`{}`, 500, Unknown, ""},
	} {
		t.Run(tc.body+string(rune(tc.code)), func(t *testing.T) {
			c := &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
				if r.URL.String() != "https://chatapi.viber.com/pa/post" {
					t.Fatal("wrong endpoint")
				}
				raw, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(raw), `"auth_token":"test"`) {
					t.Fatal("auth token missing from JSON")
				}
				return &http.Response{StatusCode: tc.code, Header: http.Header{"Retry-After": []string{"999999"}}, Body: io.NopCloser(strings.NewReader(tc.body)), Request: r}, nil
			})}
			got := (Transport{Client: c, Token: "test"}).Send(context.Background(), map[string]string{"text": "test"})
			if got.State != tc.want || got.MessageToken != tc.token {
				t.Fatalf("got %#v", got)
			}
			if tc.code == 429 && got.RetryAfter != time.Hour {
				t.Fatal(got.RetryAfter)
			}
		})
	}
}
func TestVerifySignature(t *testing.T) {
	body := []byte(`{"event":"subscribed"}`)
	m := hmac.New(sha256.New, []byte("key"))
	m.Write(body)
	sig := hex.EncodeToString(m.Sum(nil))
	if !VerifySignature(body, sig, "key") || VerifySignature(append(body, ' '), sig, "key") || VerifySignature(body, sig, "") {
		t.Fatal("signature verification")
	}
}
