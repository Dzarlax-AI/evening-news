package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRuntimeGateRequiresAuthenticatedUnpausedSnapshot(t *testing.T) {
	body := `{"maintenance_mode":true}`
	status := 200
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/runtime-configuration" || r.Header.Get("Authorization") != "Bearer fixture-purpose-token" {
			t.Error("invalid control boundary")
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	defer server.Close()
	gate := runtimeConfigurationGate(server.URL, "fixture-purpose-token")
	if err := gate(context.Background()); err == nil {
		t.Fatal("paused installation admitted recovery")
	}
	body = `{"maintenance_mode":false}`
	if err := gate(context.Background()); err != nil {
		t.Fatal(err)
	}
	body = `{}`
	if err := gate(context.Background()); err == nil {
		t.Fatal("missing state accepted")
	}
	body = `{"maintenance_mode":false}`
	status = 401
	if err := gate(context.Background()); err == nil {
		t.Fatal("authentication error admitted recovery")
	}
}
