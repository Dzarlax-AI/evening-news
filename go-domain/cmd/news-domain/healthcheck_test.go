package main

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type healthFake struct {
	ready bool
	t     *testing.T
}

func (f healthFake) Do(r *http.Request) (*http.Response, error) {
	if r.URL.String() != "http://127.0.0.1:8100/internal/v1/status" || r.Header.Get("Authorization") != "Bearer fixture-token-123456" {
		f.t.Fatal("unexpected health request")
	}
	status := 200
	body := `{"ready":true}`
	if !f.ready {
		status = 503
		body = "unready"
	}
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}, nil
}
func TestHealthcheckAuthenticatedReadiness(t *testing.T) {
	t.Setenv("JOB_EXECUTOR_TOKEN", "fixture-token-123456")
	t.Setenv("JOB_EXECUTOR_PORT", "")
	if e := checkHealth(healthFake{true, t}); e != nil {
		t.Fatal(e)
	}
	if e := checkHealth(healthFake{false, t}); e == nil {
		t.Fatal("unready accepted")
	}
}
