package cloakfixture

import (
	"context"
	"encoding/json"
	"errors"
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

func TestWorkerClientSyntheticHTTPAPIEndpoints(t *testing.T) {
	c, err := NewWorkerClient("http://fixture:8080", "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	c.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Fatal("fixture token missing")
		}
		var payload map[string]any
		if json.NewDecoder(r.Body).Decode(&payload) != nil {
			t.Fatal("request JSON invalid")
		}
		switch r.URL.Path {
		case "/v1/extract":
			if payload["strategy"] != "source_content" || payload["use_browser"] != false {
				t.Fatal("supplied-content request selected browser mode")
			}
			response, _ := json.Marshal(map[string]any{"contract_version": "1", "request_id": payload["request_id"], "outcome": "success", "result": map[string]string{"content": strings.Repeat("synthetic ", 30)}})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(response))), Header: make(http.Header)}, nil
		case "/v1/page-monitor/extract":
			if payload["operation"] != "fetch" || payload["use_browser"] != true {
				t.Fatal("PageMonitor request did not select native browser fetch")
			}
			response, _ := json.Marshal(map[string]any{"contract_version": "1", "request_id": payload["request_id"], "outcome": "success", "fetch_method": "native_browser", "items": []map[string]string{{"title": "Synthetic PageMonitor story", "link": "https://native-fixture.test/story"}}})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(response))), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
			return nil, errors.New("unexpected path")
		}
	})
	if result, err := c.SuppliedContentRequest(context.Background()); err != nil || result.Name != "supplied_content" || result.Outcome != "success" || result.ContentChars < 200 {
		t.Fatalf("supplied-content result=%+v err=%v", result, err)
	}
	if result, err := c.PageMonitorRequest(context.Background()); err != nil || result.Outcome != "success" || result.FetchMethod != "native_browser" || result.ItemCount != 1 {
		t.Fatalf("PageMonitor result=%+v err=%v", result, err)
	}
}
