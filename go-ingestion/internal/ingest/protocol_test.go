package ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-ingestion/internal/store"
)

func TestCreateRunRejectsMissingAndInvalidBearerBeforeDatabaseAccess(t *testing.T) {
	handler := NewHandler(nil, Config{Token: "expected-token"})
	for _, tc := range []struct {
		name   string
		header string
	}{
		{name: "missing"},
		{name: "wrong", header: "Bearer wrong-token"},
		{name: "raw-token", header: "expected-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/internal/v1/ingestion/runs", strings.NewReader(`{}`))
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			response := httptest.NewRecorder()
			handler.Routes().ServeHTTP(response, req)
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("unauthorized request status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestCreateRunRejectsDuplicateJSONKeysAndOversizeEnvelope(t *testing.T) {
	handler := NewHandler(nil, Config{Token: "expected-token"})
	cases := []struct {
		name string
		body string
	}{
		{name: "duplicate key", body: `{"version":1,"version":1}`},
		{name: "oversize", body: `{"padding":"` + strings.Repeat("x", maxEnvelopeBytes) + `"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/internal/v1/ingestion/runs", strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer expected-token")
			response := httptest.NewRecorder()
			handler.Routes().ServeHTTP(response, req)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("invalid request status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
}

func TestSelectorAssistRejectsMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "duplicate key", body: `{"version":1,"version":1,"learned_container_selectors":[],"learned_title_selectors":[],"learned_link_selectors":[],"learned_date_selectors":[]}`},
		{name: "oversize", body: strings.Repeat("x", 8193)},
		{name: "invalid selector", body: `{"version":1,"learned_container_selectors":[" "],"learned_title_selectors":[],"learned_link_selectors":[],"learned_date_selectors":[]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			handler := NewHandler(nil, Config{SelectorURL: server.URL, SelectorToken: "selector-secret"})
			_, err := handler.selectorCall(context.Background(), selectorTestFence(), selectorTestSource(), "load", "")
			if err == nil {
				t.Fatal("malformed selector response was accepted")
			}
		})
	}
}

func TestSelectorAssistDoesNotFollowRedirectOrForwardToken(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("selector bearer token was forwarded across redirect: %q", got)
		}
	}))
	defer target.Close()
	var initial atomic.Int32
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		initial.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer selector-secret" {
			t.Errorf("first-hop selector auth=%q", got)
		}
		http.Redirect(w, r, target.URL+"/capture", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	handler := NewHandler(nil, Config{SelectorURL: redirector.URL, SelectorToken: "selector-secret"})
	_, err := handler.selectorCall(context.Background(), selectorTestFence(), selectorTestSource(), "load", "")
	if err == nil {
		t.Fatal("selector assist accepted a redirect response")
	}
	if initial.Load() != 1 || redirected.Load() != 0 {
		t.Fatalf("redirect chain followed: first_hop=%d redirect_target=%d", initial.Load(), redirected.Load())
	}
}

func selectorTestFence() store.Fence { return store.Fence{RunID: "fixture-run", JobID: "fixture-job"} }
func selectorTestSource() store.Source {
	return store.Source{ID: 7, Fingerprint: strings.Repeat("a", 64)}
}
