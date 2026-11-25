package topicshadow

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testQuestion() []ChoiceQuestion {
	return []ChoiceQuestion{{Name: "topic", Instructions: "Choose the best topic", Choices: []ChoiceOption{{Value: "tech", Description: "Technology"}, {Value: "other", Description: "Other"}}}}
}

func TestDecideParsesChoiceUsageModelAndRefusal(t *testing.T) {
	cases := []struct {
		name, response string
		refusal        bool
	}{
		{"answer", `{"model":"gpt-6-luna-2026-11-01","answers":[{"type":"choice","name":"topic","choice":"tech","probabilities":[{"value":"tech","probability":0.8},{"value":"other","probability":0.2}],"confidence":0.8}],"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15}}`, false},
		{"refusal", `{"model":"gpt-6-luna-2026-11-01","refusal":"Cannot classify this input","answers":[],"usage":{"input_tokens":4,"output_tokens":1,"total_tokens":5}}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.String() != "/v1/decisions" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				if r.Header.Get("Authorization") != "Bearer test-secret" {
					t.Errorf("authorization header missing")
				}
				b, _ := io.ReadAll(r.Body)
				if !strings.Contains(string(b), `"model":"gpt-6-luna"`) {
					t.Errorf("model not pinned: %s", b)
				}
				if !strings.Contains(string(b), `"type":"choice"`) {
					t.Errorf("choice question type missing: %s", b)
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, tc.response)
			}))
			defer srv.Close()
			client := NewDecisionsClient("test-secret", srv.Client())
			// Redirect the HTTPS endpoint to the test server without changing the public endpoint contract.
			client.http = &http.Client{Transport: rewriteTransport{base: srv.Client().Transport, target: srv.URL}}
			got, err := client.Decide(context.Background(), "Synthetic article input", testQuestion())
			if err != nil {
				t.Fatal(err)
			}
			if got.Model != "gpt-6-luna-2026-11-01" {
				t.Fatalf("model=%q", got.Model)
			}
			if got.Usage.TotalTokens == 0 {
				t.Fatalf("usage missing: %+v", got.Usage)
			}
			if tc.refusal {
				if got.Refusal == "" || len(got.Answers) != 0 {
					t.Fatalf("unexpected refusal result: %+v", got)
				}
				return
			}
			if len(got.Answers) != 1 || got.Answers[0].Choice != "tech" || len(got.Answers[0].Probabilities) != 2 {
				t.Fatalf("unexpected result: %+v", got)
			}
		})
	}
}

type rewriteTransport struct {
	base   http.RoundTripper
	target string
}

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	copy := req.Clone(req.Context())
	copy.URL.Scheme = "http"
	copy.URL.Host = strings.TrimPrefix(rt.target, "http://")
	return rt.base.RoundTrip(copy)
}

func TestDecideRejectsInvalidResponses(t *testing.T) {
	responses := []string{
		`{"model":"gpt-6-luna","answers":[]}`,
		`{"model":"gpt-6-luna","answers":[{"type":"choice","name":"topic","choice":"unknown"}]}`,
		`{"model":"gpt-6-luna","answers":[{"type":"choice","name":"topic","choice":"tech"},{"type":"choice","name":"topic","choice":"other"}]}`,
		`{"model":"gpt-6-luna","answers":[{"type":"text","name":"topic","choice":"tech"}]}`,
	}
	for _, body := range responses {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) }))
		client := NewDecisionsClient("test", srv.Client())
		client.http = &http.Client{Transport: rewriteTransport{base: srv.Client().Transport, target: srv.URL}}
		_, err := client.Decide(context.Background(), "input", testQuestion())
		srv.Close()
		var decisionsErr *DecisionsError
		if err == nil || !asDecisionsError(err, &decisionsErr) || decisionsErr.Code != "invalid_response" {
			t.Fatalf("response should be rejected: %s, err=%v", body, err)
		}
	}
}

func asDecisionsError(err error, target **DecisionsError) bool {
	d, ok := err.(*DecisionsError)
	if ok {
		*target = d
	}
	return ok
}

func TestDecideReturnsSafeRateLimitErrorAndRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":"private provider body"}`)
	}))
	defer srv.Close()
	client := NewDecisionsClient("test-secret", srv.Client())
	client.http = &http.Client{Transport: rewriteTransport{base: srv.Client().Transport, target: srv.URL}}
	_, err := client.Decide(context.Background(), "input", testQuestion())
	d, ok := err.(*DecisionsError)
	if !ok || d.Code != "rate_limited" || d.StatusCode != http.StatusTooManyRequests || d.RetryAfter != 7*time.Second {
		t.Fatalf("unexpected error: %#v", err)
	}
	if strings.Contains(err.Error(), "private provider body") || strings.Contains(err.Error(), "test-secret") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestQuestionValidationAndHTTPSOnlyEndpoint(t *testing.T) {
	cases := [][]ChoiceQuestion{
		{},
		{{Name: "bad name", Instructions: "pick", Choices: testQuestion()[0].Choices}},
		{{Name: "topic", Instructions: "pick", Choices: []ChoiceOption{{Value: "x", Description: "x"}, {Value: "x", Description: "duplicate"}}}},
		{{Name: "topic", Instructions: "pick", Choices: []ChoiceOption{{Value: "x", Description: "x"}}}},
	}
	for _, questions := range cases {
		_, err := NewDecisionsClient("test", nil).Decide(context.Background(), "input", questions)
		if err == nil {
			t.Fatalf("expected invalid question error: %+v", questions)
		}
		if e, ok := err.(*DecisionsError); !ok || e.Code != "invalid_request" {
			t.Fatalf("unexpected error: %#v", err)
		}
	}
	if decisionsEndpoint != "https://api.openai.com/v1/decisions" {
		t.Fatalf("endpoint must be TLS-only: %s", decisionsEndpoint)
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	if got := parseRetryAfter(now.Add(5*time.Second).Format(http.TimeFormat), now); got != 5*time.Second {
		t.Fatalf("retry after=%s", got)
	}
	if got := parseRetryAfter("-2", now); got != 0 {
		t.Fatalf("negative retry after=%s", got)
	}
}

func TestDecideParsesTypedQuestionRefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"model":"gpt-6-luna","answers":[{"type":"choice","name":"topic","choice":"tech"},{"type":"refusal","name":"evidence"}],"usage":{"total_tokens":17}}`)
	}))
	defer srv.Close()
	client := NewDecisionsClient("test", &http.Client{Transport: rewriteTransport{base: srv.Client().Transport, target: srv.URL}})
	question := testQuestion()[0]
	question.Name = "evidence"
	result, err := client.Decide(context.Background(), "Synthetic article", append(testQuestion(), question))
	if err != nil || result.Refusal == "" || result.Model != "gpt-6-luna" || result.Usage.TotalTokens != 17 {
		t.Fatalf("expected typed refusal with usage: %+v err=%v", result, err)
	}
}

func TestDecideDoesNotFollowRedirects(t *testing.T) {
	redirected := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirected" {
			redirected <- struct{}{}
			return
		}
		http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	shared := &http.Client{Transport: rewriteTransport{base: srv.Client().Transport, target: srv.URL}}
	client := NewDecisionsClient("test", shared)
	_, err := client.Decide(context.Background(), "Synthetic article", testQuestion())
	var decisionErr *DecisionsError
	if !asDecisionsError(err, &decisionErr) || decisionErr.StatusCode != http.StatusTemporaryRedirect ||
		len(redirected) != 0 || shared.CheckRedirect != nil {
		t.Fatalf("expected one request and unmodified shared client: err=%v", err)
	}
}
