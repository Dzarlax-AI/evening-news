// Package extractorsmoke checks a synthetic fixture against a running extractor.
package extractorsmoke

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client interface {
	Do(*http.Request) (*http.Response, error)
}

func HTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	tr.DisableCompression = true
	return &http.Client{Transport: tr, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type fixture struct {
	ID    string `json:"id"`
	Input struct {
		Strategy string `json:"strategy"`
		URL      string `json:"url"`
		HTML     string `json:"html"`
	} `json:"input"`
	Expected struct {
		State       string   `json:"state"`
		Contains    []string `json:"contains"`
		NotContains []string `json:"not_contains"`
	} `json:"expected"`
}
type response struct {
	Contract  string `json:"contract_version"`
	Build     string `json:"build_version"`
	RequestID string `json:"request_id"`
	URL       string `json:"original_url"`
	Outcome   string `json:"outcome"`
	Result    struct {
		Content string `json:"content"`
	} `json:"result"`
	Error *struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
	} `json:"error"`
}

func Run(client Client, endpoint, token, build string, manifest []byte) (string, error) {
	fail := func(code string) (string, error) { return "", errors.New(code) }
	u, e := url.Parse(endpoint)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || token == "" || build == "" {
		return fail("invalid_configuration")
	}
	base := strings.TrimRight(endpoint, "/")
	send := func(method, path, authorization string, payload any) (int, []byte, error) {
		var body io.Reader
		if payload != nil {
			raw, e := json.Marshal(payload)
			if e != nil {
				return 0, nil, e
			}
			body = bytes.NewReader(raw)
		}
		r, e := http.NewRequest(method, base+path, body)
		if e != nil {
			return 0, nil, e
		}
		if authorization != "" {
			r.Header.Set("Authorization", "Bearer "+authorization)
		}
		r.Header.Set("Accept-Encoding", "identity")
		if payload != nil {
			r.Header.Set("Content-Type", "application/json")
		}
		res, e := client.Do(r)
		if e != nil {
			return 0, nil, e
		}
		defer res.Body.Close()
		raw, e := io.ReadAll(io.LimitReader(res.Body, 5*1024*1024+1))
		if e != nil || len(raw) > 5*1024*1024 {
			return 0, nil, errors.New("invalid_response")
		}
		return res.StatusCode, raw, nil
	}
	status, raw, e := send("GET", "/health/ready", "", nil)
	var ready struct {
		Capabilities struct {
			HTTP bool `json:"http"`
		} `json:"capabilities"`
	}
	if e != nil || status != 200 || json.Unmarshal(raw, &ready) != nil || !ready.Capabilities.HTTP {
		return fail("service_not_ready")
	}
	status, raw, e = send("GET", "/version", "", nil)
	var version struct {
		Contract string `json:"contract_version"`
		Build    string `json:"build_version"`
	}
	if e != nil || status != 200 || json.Unmarshal(raw, &version) != nil || version.Contract != "1" || version.Build != build {
		return fail("version_mismatch")
	}
	status, _, e = send("POST", "/v1/extract", "", map[string]any{})
	if e != nil || status != 401 {
		return fail("unauthenticated_admitted")
	}
	status, _, e = send("POST", "/v1/extract", token, map[string]any{"request_id": "smoke-injection", "url": "https://example.com/story", "cancel_at": "browser_html_capture"})
	if e != nil || status != 400 {
		return fail("injection_admitted")
	}
	var corpus struct {
		Cases []fixture `json:"cases"`
	}
	if json.Unmarshal(manifest, &corpus) != nil {
		return fail("fixture_invalid")
	}
	var chosen *fixture
	for i := range corpus.Cases {
		c := &corpus.Cases[i]
		if c.Input.Strategy == "readability" && c.Input.HTML != "" && c.Expected.State == "extracted" && len(c.Expected.Contains) > 0 {
			chosen = c
			break
		}
	}
	if chosen == nil {
		return fail("fixture_missing")
	}
	payload := map[string]any{"request_id": "smoke-fixture", "url": chosen.Input.URL, "strategy": "readability", "source_type": "article", "html": chosen.Input.HTML, "use_browser": false, "budget_ms": 10000, "max_bytes": 5 * 1024 * 1024, "max_chars": 100000}
	status, raw, e = send("POST", "/v1/extract", token, payload)
	var result response
	if e != nil || status != 200 || json.Unmarshal(raw, &result) != nil || result.Contract != "1" || result.Build != build || result.RequestID != "smoke-fixture" || result.URL != chosen.Input.URL || result.Outcome != "success" {
		return fail("fixture_failed")
	}
	for _, s := range chosen.Expected.Contains {
		if !strings.Contains(result.Result.Content, s) {
			return fail("fixture_anchor_missing")
		}
	}
	for _, s := range chosen.Expected.NotContains {
		if strings.Contains(result.Result.Content, s) {
			return fail("fixture_noise_retained")
		}
	}
	payload = map[string]any{"request_id": "smoke-denied", "url": "http://127.0.0.1:9/private", "strategy": "auto", "source_type": "article", "use_browser": false, "budget_ms": 5000, "max_bytes": 5 * 1024 * 1024, "max_chars": 100000}
	status, raw, e = send("POST", "/v1/extract", token, payload)
	result = response{}
	if e != nil || status != 200 || json.Unmarshal(raw, &result) != nil || result.Contract != "1" || result.RequestID != "smoke-denied" || result.URL != "http://127.0.0.1:9/private" || (result.Outcome != "rejected" && result.Outcome != "failed") || result.Error == nil || result.Error.Code != "public_url_denied" {
		return fail("loopback_not_denied")
	}
	return chosen.ID, nil
}
