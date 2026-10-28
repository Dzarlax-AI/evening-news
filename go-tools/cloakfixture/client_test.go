package cloakfixture

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestWorkerRequestValidatesAndRedacts(t *testing.T) {
	c, err := NewWorkerClient("http://fixture:8080", "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	c.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Fatal("missing auth")
		}
		var p map[string]any
		if json.NewDecoder(r.Body).Decode(&p) != nil {
			t.Fatal("bad request")
		}
		b, _ := json.Marshal(map[string]any{"contract_version": "1", "request_id": p["request_id"], "outcome": "success", "result": map[string]any{"content": strings.Repeat("я", 200)}})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(b))), Header: make(http.Header)}, nil
	})
	got, err := c.Request(context.Background(), "one", "case", 25000, "")
	if err != nil || got.ContentChars != 200 || got.Outcome != "success" {
		t.Fatalf("%+v %v", got, err)
	}
}
func TestWorkerRejectsBadIdentityAndRedirect(t *testing.T) {
	for _, code := range []int{200, 302} {
		c, _ := NewWorkerClient("http://fixture:8080", "fixture-token")
		c.client.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(`{"contract_version":"1","request_id":"wrong","outcome":"success","result":{}}`)), Header: make(http.Header)}, nil
		})
		_, err := c.Request(context.Background(), "one", "case", 25000, "")
		if err == nil || err.Error() != "worker_response_invalid" {
			t.Fatal(err)
		}
	}
}

func TestWorkerRejectsSuccessEnvelopeOnErrorHTTPStatus(t *testing.T) {
	for _, status := range []int{400, 401, 429, 500, 503} {
		c, _ := NewWorkerClient("http://fixture:8080", "fixture-token")
		c.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			var request map[string]any
			_ = json.NewDecoder(r.Body).Decode(&request)
			body, _ := json.Marshal(map[string]any{"contract_version": "1", "request_id": request["request_id"], "outcome": "success", "result": map[string]any{"content": strings.Repeat("x", 200)}})
			return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
		})
		if _, err := c.Request(context.Background(), "one", "case", 25000, ""); err == nil || err.Error() != "worker_response_invalid" {
			t.Fatalf("status %d: %v", status, err)
		}
	}
}
