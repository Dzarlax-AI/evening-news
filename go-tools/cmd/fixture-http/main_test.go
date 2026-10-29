package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type fake struct{ t *testing.T }

func (f fake) Do(r *http.Request) (*http.Response, error) {
	if r.URL.String() != "http://web:8000/api/v1/process/run" || r.Header.Get("Authorization") != "Basic fixture" || r.Header.Get("Idempotency-Key") != "fixture-key" {
		f.t.Fatal("wrong fixture request")
	}
	return &http.Response{StatusCode: 202, Header: http.Header{"Set-Cookie": []string{"private=value"}}, Body: io.NopCloser(strings.NewReader(`{"job_id":"fixture"}`))}, nil
}
func TestBoundedProbe(t *testing.T) {
	raw, _ := json.Marshal(command{Method: "POST", Path: "/api/v1/process/run", Authorization: "Basic fixture", Body: []byte(`{}`), Headers: map[string]string{"Idempotency-Key": "fixture-key"}})
	var out bytes.Buffer
	if e := runWithClient(bytes.NewReader(raw), &out, fake{t}); e != nil {
		t.Fatal(e)
	}
	var result reply
	if json.Unmarshal(out.Bytes(), &result) != nil || result.Status != 202 || string(result.Body) != `{"job_id":"fixture"}` || !result.HasCookie || bytes.Contains(out.Bytes(), []byte("private=value")) {
		t.Fatal("invalid response")
	}
	for _, path := range []string{"http://attacker/", "//attacker/", "/../secret"} {
		raw, _ = json.Marshal(command{Method: "GET", Path: path})
		if e := runWithClient(bytes.NewReader(raw), io.Discard, fake{t}); e == nil {
			t.Fatal("unsafe target accepted")
		}
	}
}

type domainFake struct{ t *testing.T }

func (f domainFake) Do(r *http.Request) (*http.Response, error) {
	if r.URL.String() != "http://executor:8100/internal/v1/status" || r.Header.Get("Authorization") != "Bearer fixture" {
		f.t.Fatal("wrong domain probe")
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ready":true}`))}, nil
}
func TestDomainProbeUsesOnlyFixedInternalTarget(t *testing.T) {
	raw, _ := json.Marshal(command{Target: "domain", Method: "GET", Path: "/internal/v1/status", Authorization: "Bearer fixture"})
	if err := runWithClient(bytes.NewReader(raw), io.Discard, domainFake{t}); err != nil {
		t.Fatal(err)
	}
}

type loginFake struct{ t *testing.T }

func (f loginFake) Do(r *http.Request) (*http.Response, error) {
	if r.URL.String() != "http://web:8000/auth/login" || r.Method != "POST" || r.Header.Get("X-Forwarded-Proto") != "https" {
		f.t.Fatal("wrong managed login probe")
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"access_token":"fixture"}`))}, nil
}

func TestLoginProbeAllowsOnlyFixedTrustedHTTPSAssertion(t *testing.T) {
	valid, _ := json.Marshal(command{Target: "web", Method: "POST", Path: "/auth/login", Headers: map[string]string{"X-Forwarded-Proto": "https"}})
	if err := runWithClient(bytes.NewReader(valid), io.Discard, loginFake{t}); err != nil {
		t.Fatal("fixed login https assertion rejected")
	}
	for name, input := range map[string]command{
		"wrong scheme":       {Target: "web", Method: "POST", Path: "/auth/login", Headers: map[string]string{"X-Forwarded-Proto": "http"}},
		"wrong endpoint":     {Target: "web", Method: "POST", Path: "/auth/me", Headers: map[string]string{"X-Forwarded-Proto": "https"}},
		"wrong method":       {Target: "web", Method: "GET", Path: "/auth/login", Headers: map[string]string{"X-Forwarded-Proto": "https"}},
		"identity header":    {Target: "web", Method: "POST", Path: "/auth/login", Headers: map[string]string{"X-Forwarded-User": "admin"}},
		"invalid article id": {Target: "web", Method: "GET", Path: "/api/v1/articles/../secret", Headers: map[string]string{"X-Forwarded-Proto": "https"}},
		"other target":       {Target: "domain", Method: "GET", Path: "/api/v1/articles/1", Headers: map[string]string{"X-Forwarded-Proto": "https"}},
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := json.Marshal(input)
			if err := runWithClient(bytes.NewReader(raw), io.Discard, loginFake{t}); err == nil {
				t.Fatal("unexpected probe header accepted")
			}
		})
	}
	article, _ := json.Marshal(command{Target: "web", Method: "GET", Path: "/api/v1/articles/123", Headers: map[string]string{"X-Forwarded-Proto": "https"}})
	if err := runWithClient(bytes.NewReader(article), io.Discard, articleFake{t}); err != nil {
		t.Fatal("fixed managed article https assertion rejected")
	}
}

type articleFake struct{ t *testing.T }

func (f articleFake) Do(r *http.Request) (*http.Response, error) {
	if r.URL.String() != "http://web:8000/api/v1/articles/123" || r.Method != "GET" || r.Header.Get("X-Forwarded-Proto") != "https" {
		f.t.Fatal("wrong managed article probe")
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
}
