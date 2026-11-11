package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNativeBudgetRejectsAmbiguousOrForeignReceipt(t *testing.T) {
	id := strings.Repeat("a", 32)
	valid := `{"contract_version":"1","budget_id":"` + id + `","wire_limit_bytes":1024,"wire_bytes":12,"exhausted":false,"expired":false,"revoked":true,"active_tunnels":0}`
	for name, body := range map[string]string{
		"missing_false": strings.Replace(valid, `"exhausted":false,`, "", 1),
		"null_false":    strings.Replace(valid, `"exhausted":false`, `"exhausted":null`, 1),
		"duplicate":     strings.Replace(valid, `"wire_bytes":12`, `"wire_bytes":12,"wire_bytes":0`, 1),
		"foreign_id":    strings.Replace(valid, id, strings.Repeat("b", 32), 1),
		"active_tunnel": strings.Replace(valid, `"active_tunnels":0`, `"active_tunnels":1`, 1),
		"oversized":     valid + strings.Repeat(" ", 1024),
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodDelete || r.Header.Get("Authorization") != "Bearer synthetic-control-token" {
					t.Error("invalid control request")
				}
				w.Write([]byte(body))
			}))
			defer server.Close()
			budget := nativeRenderBudget{id: id, limit: 1024, url: server.URL, control: "synthetic-control-token"}
			if _, err := budget.revoke(context.Background()); !errors.Is(err, ErrNativeOwnershipLost) {
				t.Fatalf("invalid receipt accepted: %v", err)
			}
		})
	}
}

func TestNativeReplaySharesDecodedBudgetAndClosesBothStreams(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	resources, err := w.open(context.Background(), MaxPageBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer resources.cleanup()
	var remaining atomic.Int64
	body := "<html><body>Original synthetic document</body></html>"
	remaining.Store(int64(len(body)*2 - 1))
	first, err := nativeBoundedReplayBody(context.Background(), resources.client, resources.sessionID, "first", &remaining)
	if err != nil || string(first) != body {
		t.Fatalf("first stream failed: %v", err)
	}
	second, err := nativeBoundedReplayBody(context.Background(), resources.client, resources.sessionID, "second", &remaining)
	if NativeResourceLimitReason(err) != "dom_output" || len(second) != 0 {
		t.Fatal("second stream escaped the shared decoded budget")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	closes := 0
	for _, command := range f.commands {
		if command.method == "IO.close" && command.session == resources.sessionID {
			closes++
		}
	}
	if closes != 2 {
		t.Fatal("successful or rejected stream was not closed")
	}
}
