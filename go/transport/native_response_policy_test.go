package transport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestNativeWorkerPolicyPreservesResponse(t *testing.T) {
	status := 200
	headers := []nativeResponseHeader{{"Content-Type", "text/html; charset=utf-8"}, {"Content-Encoding", "gzip"}, {"Content-Length", "1234"}, {"Set-Cookie", "first=one"}, {"Set-Cookie", "second=two"}, {"Content-Security-Policy", "default-src 'self'"}}
	original := append([]nativeResponseHeader(nil), headers...)
	params, err := nativeWorkerPolicyParams(nativeFetchPaused{ID: "document", ResourceType: "Document", ResponseStatusCode: &status, ResponseStatusText: "Publisher phrase", ResponseHeaders: headers})
	if err != nil {
		t.Fatal(err)
	}
	if params["responseCode"] != 200 || params["responsePhrase"] != "Publisher phrase" {
		t.Fatal("status changed")
	}
	got := params["responseHeaders"].([]nativeResponseHeader)
	if !reflect.DeepEqual(got[:len(headers)], original) || !reflect.DeepEqual(headers, original) || got[len(headers)] != (nativeResponseHeader{"Content-Security-Policy", "worker-src 'none'; connect-src https:"}) {
		t.Fatal("publisher headers changed or worker policy missing")
	}
	raw, _ := json.Marshal(params)
	for _, forbidden := range []string{"body", "postData", "url"} {
		var keys map[string]any
		json.Unmarshal(raw, &keys)
		if _, ok := keys[forbidden]; ok {
			t.Fatal("response body/request rewrite")
		}
	}
}

func TestNativeWorkerPolicyRejectsAmbiguousResponse(t *testing.T) {
	for _, mode := range []string{"no_status", "no_headers", "wrong_type", "header_name", "header_value", "header_count", "phrase", "failed_response"} {
		t.Run(mode, func(t *testing.T) {
			status := 200
			ev := nativeFetchPaused{ID: "document", ResourceType: "Document", ResponseStatusCode: &status, ResponseHeaders: []nativeResponseHeader{}}
			switch mode {
			case "no_status":
				ev.ResponseStatusCode = nil
			case "no_headers":
				ev.ResponseHeaders = nil
			case "wrong_type":
				ev.ResourceType = "Script"
			case "header_name":
				ev.ResponseHeaders = []nativeResponseHeader{{"Bad\nName", "value"}}
			case "header_value":
				ev.ResponseHeaders = []nativeResponseHeader{{"X-Publisher", "bad\r\nInjected: yes"}}
			case "header_count":
				ev.ResponseHeaders = make([]nativeResponseHeader, 256)
			case "phrase":
				ev.ResponseStatusText = "bad\nphrase"
			case "failed_response":
				ev.ResponseErrorReason = "Failed"
			}
			if _, err := nativeWorkerPolicyParams(ev); err == nil {
				t.Fatal("ambiguous document response continued")
			}
		})
	}
}

func TestNativeResponsePolicyContinuationMustComplete(t *testing.T) {
	for _, mode := range []string{"success", "failure", "stalled"} {
		t.Run(mode, func(t *testing.T) {
			f := newNativeFixture(t)
			w := f.worker(t)
			if err := w.Probe(context.Background()); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.responsePolicy = true
			f.failResponse = mode == "failure"
			f.stallResponse = mode == "stalled"
			f.mu.Unlock()
			doc, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{BrowserOptions: BrowserOptions{Timeout: time.Second}})
			if mode == "success" {
				if err != nil || doc.StatusCode != 200 {
					t.Fatalf("guarded response failed: %v", err)
				}
			} else {
				if err == nil || len(doc.HTML) != 0 {
					t.Fatal("unconfirmed CSP returned document")
				}
				if mode == "failure" && !errors.Is(err, ErrNativeOwnershipLost) {
					t.Fatalf("CSP failure did not fence ownership: %v", err)
				}
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.responseParams == nil || len(f.responseSessions) != 1 || f.responseSessions[0] != "owned-session" {
				t.Fatal("response continuation not session-scoped")
			}
			body, decodeErr := base64.StdEncoding.DecodeString(f.responseParams["body"].(string))
			if decodeErr != nil || string(body) != "<html><body>Original synthetic document</body></html>" {
				t.Fatal("replay changed publisher body bytes")
			}
			if len(f.contexts) != 0 || len(f.targets) != 1 || !f.targets["foreign-sentinel"] {
				t.Fatal("cleanup changed other targets")
			}
		})
	}
}
func TestNativeIframeReceivesResponsePolicy(t *testing.T) {
	f := newNativeFixture(t)
	w := f.worker(t)
	if err := w.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.responsePolicy = true
	f.childEvents = []nativeFixtureChild{{kind: "iframe", paused: true}}
	f.mu.Unlock()
	if _, err := w.Fetch(context.Background(), "https://origin.example/", NativeBrowserOptions{}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.responseSessions) != 2 {
		t.Fatalf("missing frame policy: %v", f.responseSessions)
	}
	found := false
	for _, session := range f.responseSessions {
		found = found || session == "child-session-0"
	}
	if !found {
		t.Fatal("child response routed to main session")
	}
}

func TestNativeHTTP2ResponseHasNoEmptyPhraseOverride(t *testing.T) {
	status := 200
	params, err := nativeWorkerPolicyParams(nativeFetchPaused{ID: "h2-document", ResourceType: "Document", ResponseStatusCode: &status, ResponseHeaders: []nativeResponseHeader{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := params["responsePhrase"]; ok {
		t.Fatal("HTTP/2 empty phrase sent as an invalid override")
	}
}
