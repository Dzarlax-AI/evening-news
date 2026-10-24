package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClientDoesNotFollowRedirectsOrForwardExecutorToken(t *testing.T) {
	var redirectedRequests int
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirectedRequests++
		if r.Header.Get("Authorization") != "" {
			t.Error("executor token was forwarded across a redirect")
		}
	}))
	defer redirectTarget.Close()
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, redirectTarget.URL+"/captured", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()

	client, err := New(redirector.URL, "fixture-executor-token-123", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Status(context.Background())
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusTemporaryRedirect {
		t.Fatalf("status error = %v; want HTTP 307", err)
	}
	if redirectedRequests != 0 {
		t.Fatalf("redirect target saw %d requests; want zero", redirectedRequests)
	}
}

func TestClientRejectsNonStrictAndOversizedJSONResponses(t *testing.T) {
	for name, body := range map[string]string{
		"unknown field":  `{"ready":true,"active_executions":0,"executor_instance_id":"fixture","extra":1}`,
		"trailing JSON":  `{"ready":true,"active_executions":0,"executor_instance_id":"fixture"} {}`,
		"oversized body": `{"ready":true,"active_executions":0,"executor_instance_id":"fixture","padding":"` + strings.Repeat("x", 8192) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, body)
			}))
			defer server.Close()
			client, err := New(server.URL, "fixture-executor-token-123", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Status(context.Background()); err == nil {
				t.Fatal("expected invalid response to be rejected")
			}
		})
	}
}

func TestClientRejectsUnknownReceiptStateAndOversizedResult(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano)
	for name, receipt := range map[string]map[string]any{
		"unknown state": {"job_id": "fixture", "attempt_generation": 1, "state": "queued"},
		"oversized result": {"job_id": "fixture", "attempt_generation": 1, "state": "succeeded",
			"result": json.RawMessage(`{"padding":"` + strings.Repeat("x", 4096) + `"}`)},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/internal/v1/job-executions/fixture/1" {
					http.NotFound(w, r)
					return
				}
				receipt["owner_id"] = "fixture-owner"
				receipt["generation"] = 2
				receipt["deadline_at"] = deadline
				receipt["executor_instance_id"] = "fixture-instance"
				_ = json.NewEncoder(w).Encode(receipt)
			}))
			defer server.Close()
			client, err := New(server.URL, "fixture-executor-token-123", time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Receipt(context.Background(), "fixture", 1); err == nil {
				t.Fatal("expected malformed receipt to be rejected")
			}
		})
	}
}
