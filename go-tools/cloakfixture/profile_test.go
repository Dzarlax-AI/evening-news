package cloakfixture

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakePeer struct{ checks int }

func (p *fakePeer) AssertClean(context.Context) error { p.checks++; return nil }
func TestProfilesVerifyOneAndTwoSlotBarriers(t *testing.T) {
	dir := t.TempDir()
	origin, e := NewOrigin(dir)
	if e != nil {
		t.Fatal(e)
	}
	if e = os.Mkdir(filepath.Join(dir, "release"), 0700); e != nil {
		t.Fatal(e)
	}
	peer := &fakePeer{}
	for _, slots := range []int{1, 2} {
		sem := make(chan struct{}, slots)
		worker, _ := NewWorkerClient("http://fixture:8080", "secret")
		worker.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
			select {
			case sem <- struct{}{}:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			defer func() { <-sem }()
			var p map[string]any
			if json.NewDecoder(r.Body).Decode(&p) != nil {
				t.Fatal("invalid request")
			}
			url := p["url"].(string)
			req := httptest.NewRequest("GET", url, nil).WithContext(r.Context())
			rec := httptest.NewRecorder()
			origin.ServeHTTP(rec, req)
			b, _ := json.Marshal(map[string]any{"contract_version": "1", "request_id": p["request_id"], "outcome": "success", "result": map[string]string{"content": strings.Repeat("x", 250)}})
			return &http.Response{StatusCode: rec.Code, Body: io.NopCloser(strings.NewReader(string(b))), Header: make(http.Header)}, nil
		})
		p := Profiles{Worker: worker, StateDir: dir, Peer: peer}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		r, err := p.Profile(ctx, slots)
		cancel()
		if err != nil || r.Limit != slots || r.MaxOriginActive != slots || len(r.Requests) != slots+1 {
			t.Fatalf("slots %d result %+v error %v", slots, r, err)
		}
	}
	if peer.checks != 2 {
		t.Fatal("peer not checked")
	}
}
