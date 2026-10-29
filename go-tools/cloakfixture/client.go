package cloakfixture

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var finiteCode = regexp.MustCompile(`^[a-z0-9_]{1,64}$`)

type RequestResult struct {
	Name           string  `json:"name"`
	ElapsedMS      float64 `json:"elapsed_ms"`
	Outcome        string  `json:"outcome"`
	Error          string  `json:"error,omitempty"`
	UpstreamStatus *int    `json:"upstream_status,omitempty"`
	ContentChars   int     `json:"content_chars"`
}
type PageMonitorRequestResult struct {
	Name        string `json:"name"`
	Outcome     string `json:"outcome"`
	FetchMethod string `json:"fetch_method"`
	ItemCount   int    `json:"item_count"`
	Title       string `json:"title,omitempty"`
}
type WorkerClient struct {
	endpoint, token string
	client          *http.Client
}

func NewWorkerClient(endpoint, token string) (*WorkerClient, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || token == "" {
		return nil, errors.New("worker_configuration_invalid")
	}
	return &WorkerClient{strings.TrimRight(endpoint, "/"), token, &http.Client{Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *WorkerClient) Request(ctx context.Context, name, caseID string, budget int, route string) (RequestResult, error) {
	var out RequestResult
	if !fixtureID.MatchString(name) || !fixtureID.MatchString(caseID) || budget < 1 || budget > 30000 {
		return out, errors.New("client_request_invalid")
	}
	if route == "" {
		route = "hold/" + caseID + "/" + name
	}
	if route != "error/error" && route != "hold/"+caseID+"/"+name {
		return out, errors.New("client_request_invalid")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return out, errors.New("client_request_invalid")
	}
	id := "cloak-concurrency-" + hex.EncodeToString(nonce[:])
	payload := map[string]any{"request_id": id, "url": "https://native-fixture.test/" + route, "source_type": "article", "strategy": "browser_rendering", "use_browser": true, "budget_ms": budget}
	b, _ := json.Marshal(payload)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/v1/extract", bytes.NewReader(b))
	if err != nil {
		return out, errors.New("client_request_invalid")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	start := time.Now()
	response, err := c.client.Do(request)
	if err != nil {
		return out, errors.New("worker_unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 && response.StatusCode < 400 {
		return out, errors.New("worker_response_invalid")
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil || len(raw) > 1024*1024 {
		return out, errors.New("worker_response_invalid")
	}
	var value struct {
		Contract string `json:"contract_version"`
		ID       string `json:"request_id"`
		Outcome  string `json:"outcome"`
		Error    *struct {
			Code           string `json:"code"`
			UpstreamStatus *int   `json:"upstream_status"`
		} `json:"error"`
		Result *struct {
			Content string `json:"content"`
		} `json:"result"`
	}
	if json.Unmarshal(raw, &value) != nil || value.Contract != "1" || value.ID != id || value.Result == nil || (value.Outcome != "success" && value.Outcome != "failed" && value.Outcome != "rejected") {
		return out, errors.New("worker_response_invalid")
	}
	if value.Outcome == "success" && response.StatusCode != http.StatusOK {
		return out, errors.New("worker_response_invalid")
	}
	out = RequestResult{Name: name, ElapsedMS: float64(time.Since(start).Microseconds()) / 1000, Outcome: value.Outcome, ContentChars: len([]rune(value.Result.Content))}
	if value.Error != nil {
		if !finiteCode.MatchString(value.Error.Code) {
			return RequestResult{}, errors.New("worker_response_invalid")
		}
		out.Error = value.Error.Code
		out.UpstreamStatus = value.Error.UpstreamStatus
	}
	return out, nil
}

// SuppliedContentRequest exercises extraction without browser access or an
// upstream request. Its content is synthetic and supplied in the request.
func (c *WorkerClient) SuppliedContentRequest(ctx context.Context) (RequestResult, error) {
	var out RequestResult
	var useBrowser = false
	payload := map[string]any{
		"request_id":  "cloak-http-only-acceptance",
		"url":         "https://native-fixture.test/http-only",
		"content":     fixtureBody,
		"source_type": "article",
		"strategy":    "source_content",
		"use_browser": useBrowser,
		"budget_ms":   5000,
	}
	raw, status, err := c.post(ctx, "/v1/extract", payload)
	if err != nil || status != http.StatusOK {
		return out, errors.New("worker_response_invalid")
	}
	var value struct {
		Contract string `json:"contract_version"`
		ID       string `json:"request_id"`
		Outcome  string `json:"outcome"`
		Result   struct {
			Content string `json:"content"`
		} `json:"result"`
	}
	if json.Unmarshal(raw, &value) != nil || value.Contract != "1" || value.ID != payload["request_id"] || value.Outcome != "success" || len([]rune(value.Result.Content)) < 200 {
		return out, errors.New("worker_response_invalid")
	}
	return RequestResult{Name: "supplied_content", Outcome: value.Outcome, ContentChars: len([]rune(value.Result.Content))}, nil
}

// PageMonitorRequest verifies the native browser PageMonitor endpoint against
// the isolated synthetic origin. The returned receipt contains no request
// credentials or captured page body.
func (c *WorkerClient) PageMonitorRequest(ctx context.Context) (PageMonitorRequestResult, error) {
	var out PageMonitorRequestResult
	useBrowser, waitForJS := true, false
	payload := map[string]any{
		"contract_version":  "1",
		"request_id":        "cloak-page-monitor-acceptance",
		"operation":         "fetch",
		"url":               "https://native-fixture.test/page-monitor",
		"source_name":       "Synthetic Cloak Fixture",
		"budget_ms":         12000,
		"limit":             5,
		"min_title_length":  10,
		"use_browser":       useBrowser,
		"wait_for_js":       waitForJS,
		"article_selectors": []string{"article"},
		"title_selectors":   []string{"h2"},
		"date_selectors":    []string{"time"},
		"link_selectors":    []string{"a"},
	}
	raw, status, err := c.post(ctx, "/v1/page-monitor/extract", payload)
	if err != nil || status != http.StatusOK {
		return out, errors.New("worker_response_invalid")
	}
	var value struct {
		Contract    string `json:"contract_version"`
		ID          string `json:"request_id"`
		Outcome     string `json:"outcome"`
		FetchMethod string `json:"fetch_method"`
		Items       []struct {
			Title string `json:"title"`
			Link  string `json:"link"`
		} `json:"items"`
	}
	if json.Unmarshal(raw, &value) != nil || value.Contract != "1" || value.ID != payload["request_id"] || value.Outcome != "success" || value.FetchMethod != "native_browser" || len(value.Items) != 1 || value.Items[0].Title != "Synthetic PageMonitor story" || value.Items[0].Link != "https://native-fixture.test/story" {
		return out, errors.New("worker_response_invalid")
	}
	return PageMonitorRequestResult{Name: "native_page_monitor", Outcome: value.Outcome, FetchMethod: value.FetchMethod, ItemCount: len(value.Items), Title: value.Items[0].Title}, nil
}

func (c *WorkerClient) post(ctx context.Context, path string, payload any) ([]byte, int, error) {
	var invalid []byte
	b, err := json.Marshal(payload)
	if err != nil {
		return invalid, 0, errors.New("worker_request_invalid")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+path, bytes.NewReader(b))
	if err != nil {
		return invalid, 0, errors.New("worker_request_invalid")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.client.Do(request)
	if err != nil {
		return invalid, 0, errors.New("worker_unavailable")
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil || len(raw) > 1024*1024 {
		return invalid, 0, errors.New("worker_response_invalid")
	}
	return raw, response.StatusCode, nil
}

func ensureSuccess(rows ...RequestResult) error {
	for _, r := range rows {
		if r.Outcome != "success" || r.ContentChars < 200 {
			return fmt.Errorf("synthetic_article_request_failed")
		}
	}
	return nil
}
