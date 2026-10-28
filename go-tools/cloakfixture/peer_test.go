package cloakfixture

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func TestPeerChecksForeignSessionAndDisposesOnlyItsContext(t *testing.T) {
	client, server := net.Pipe()
	p := &CDPPeer{conn: client, reader: wsutil.NewClientSideReader(client), contextID: "foreign-context", targetID: "foreign-target", sessionID: "foreign-session", contexts: map[string]bool{"foreign-context": true}, targets: map[string]bool{"foreign-target": true}}
	done := make(chan error, 1)
	go func() {
		defer server.Close()
		for {
			raw, _, err := wsutil.ReadClientData(server)
			if err != nil {
				done <- err
				return
			}
			var request struct {
				ID      int            `json:"id"`
				Method  string         `json:"method"`
				Params  map[string]any `json:"params"`
				Session string         `json:"sessionId"`
			}
			if err = json.Unmarshal(raw, &request); err != nil {
				done <- err
				return
			}
			result := map[string]any{}
			switch request.Method {
			case "Target.getTargets":
				result["targetInfos"] = []map[string]string{{"targetId": "foreign-target"}}
			case "Target.getBrowserContexts":
				result["browserContextIds"] = []string{"foreign-context"}
			case "Runtime.evaluate":
				if request.Session != "foreign-session" {
					t.Error("wrong session")
				}
				result["result"] = map[string]int{"value": 2}
			case "Target.disposeBrowserContext":
				if request.Params["browserContextId"] != "foreign-context" {
					t.Error("wrong context")
				}
			default:
				t.Error("unexpected method")
			}
			b, _ := json.Marshal(map[string]any{"id": request.ID, "result": result})
			if err = wsutil.WriteServerMessage(server, ws.OpText, b); err != nil {
				done <- err
				return
			}
			if request.Method == "Target.disposeBrowserContext" {
				done <- nil
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.AssertClean(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPeerRejectsMissingForeignTarget(t *testing.T) {
	client, server := net.Pipe()
	p := &CDPPeer{conn: client, reader: wsutil.NewClientSideReader(client), contextID: "ctx", targetID: "missing", sessionID: "session"}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer server.Close()
		for i := 0; i < 2; i++ {
			raw, _, err := wsutil.ReadClientData(server)
			if err != nil {
				return
			}
			var r struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
			}
			_ = json.Unmarshal(raw, &r)
			result := map[string]any{}
			if r.Method == "Target.getTargets" {
				result["targetInfos"] = []any{}
			} else {
				result["browserContextIds"] = []string{"ctx"}
			}
			b, _ := json.Marshal(map[string]any{"id": r.ID, "result": result})
			_ = wsutil.WriteServerMessage(server, ws.OpText, b)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := p.probe(ctx); err == nil || err.Error() != "peer_target_or_context_lost" {
		t.Fatal(err)
	}
	client.Close()
	<-done
}
