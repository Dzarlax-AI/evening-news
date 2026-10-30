package execution

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestContractFixturesParity(t *testing.T) {
	read := func(name string) []byte {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("..", "tests", "fixtures", "execution-contract-v1", name))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	var envelopeFixture struct {
		Raw    map[string]any `json:"raw"`
		Digest string         `json:"canonical_sha256"`
	}
	if err := json.Unmarshal(read("envelope.json"), &envelopeFixture); err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(envelopeFixture.Raw)
	_, digest, err := parseEnvelope(encoded)
	if err != nil || digest != envelopeFixture.Digest {
		t.Fatalf("frozen envelope digest=%s want=%s err=%v", digest, envelopeFixture.Digest, err)
	}
	var payloads []struct {
		Kind       string          `json:"kind"`
		Input      json.RawMessage `json:"input"`
		Normalized json.RawMessage `json:"normalized"`
	}
	if err = json.Unmarshal(read("job-payloads.json"), &payloads); err != nil {
		t.Fatal(err)
	}
	for _, tc := range payloads {
		got, err := validatePayload(tc.Kind, tc.Input)
		if err != nil {
			t.Errorf("%s rejected frozen payload: %v", tc.Kind, err)
			continue
		}
		var gotValue, wantValue any
		_ = json.Unmarshal(got, &gotValue)
		_ = json.Unmarshal(tc.Normalized, &wantValue)
		if !reflect.DeepEqual(gotValue, wantValue) {
			t.Errorf("%s normalized to %#v; want %#v", tc.Kind, gotValue, wantValue)
		}
	}
	var statusWant map[string]any
	_ = json.Unmarshal(read("status.json"), &statusWant)
	statusEncoded, _ := json.Marshal(statusResponse{ExecutorInstanceID: "33333333-3333-4333-8333-333333333333", ActiveExecutions: 0, Ready: true})
	var statusGot map[string]any
	_ = json.Unmarshal(statusEncoded, &statusGot)
	if !reflect.DeepEqual(statusGot, statusWant) {
		t.Fatalf("worker status contract mismatch: got=%#v want=%#v", statusGot, statusWant)
	}
	var receiptWant map[string]any
	_ = json.Unmarshal(read("receipt.json"), &receiptWant)
	owner, generation, deadline := receiptWant["owner_id"].(string), int64(7), receiptWant["deadline_at"].(string)
	result, _ := json.Marshal(receiptWant["result"])
	accepted, started, finished := receiptWant["accepted_at"].(string), receiptWant["started_at"].(string), receiptWant["finished_at"].(string)
	receipt := Receipt{JobID: receiptWant["job_id"].(string), Attempt: 3, OwnerID: &owner, Generation: &generation, DeadlineAt: &deadline, Instance: "33333333-3333-4333-8333-333333333333", State: "succeeded", AcceptedAt: accepted, StartedAt: &started, FinishedAt: &finished, Result: result}
	gotJSON, _ := json.Marshal(receipt.Map())
	var got map[string]any
	_ = json.Unmarshal(gotJSON, &got)
	if !reflect.DeepEqual(got, receiptWant) {
		t.Fatalf("receipt fixture mismatch: got=%#v want=%#v", got, receiptWant)
	}
	var bounded []struct {
		Expected json.RawMessage `json:"expected"`
	}
	if err = json.Unmarshal(read("bounded-results.json"), &bounded); err != nil {
		t.Fatal(err)
	}
	for i, tc := range bounded {
		got, err := safeResult(tc.Expected)
		if err != nil {
			t.Errorf("bounded result fixture %d rejected: %v", i, err)
			continue
		}
		var gotValue, wantValue any
		_ = json.Unmarshal(got, &gotValue)
		_ = json.Unmarshal(tc.Expected, &wantValue)
		if !reflect.DeepEqual(gotValue, wantValue) {
			t.Errorf("bounded result fixture %d changed: got=%#v want=%#v", i, gotValue, wantValue)
		}
	}
}

func TestParseEnvelopeDigestKeepsOriginalDeadline(t *testing.T) {
	base := `{"version":1,"job_id":"11111111-1111-4111-8111-111111111111","owner_id":"22222222-2222-4222-8222-222222222222","generation":4,"attempt_generation":2,"deadline_at":"2026-10-27T12:00:00Z"}`
	e, digest, err := parseEnvelope([]byte(base))
	if err != nil || e.DeadlineRaw != "2026-10-27T12:00:00Z" || len(digest) != 64 {
		t.Fatalf("parse envelope: env=%+v digest=%q err=%v", e, digest, err)
	}
	changed := strings.Replace(base, "12:00:00Z", "14:00:00+02:00", 1)
	_, second, err := parseEnvelope([]byte(changed))
	if err != nil || second == digest {
		t.Fatalf("deadline spelling must bind the digest: %q %q err=%v", digest, second, err)
	}
}

func TestParseEnvelopeRejectsMalformedInputs(t *testing.T) {
	valid := `{"version":1,"job_id":"11111111-1111-4111-8111-111111111111","owner_id":"22222222-2222-4222-8222-222222222222","generation":4,"attempt_generation":2,"deadline_at":"2026-10-27T12:00:00Z"}`
	cases := []string{
		strings.Replace(valid, `"generation":4`, `"generation":true`, 1),
		strings.Replace(valid, `"generation":4`, `"generation":4.0`, 1),
		strings.Replace(valid, `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(valid, `"version":1`, `"version":1,"unknown":0`, 1),
		strings.Replace(valid, `"job_id":"11111111-1111-4111-8111-111111111111"`, `"job_id":"11111111111141118111111111111111"`, 1),
		strings.Replace(valid, `"attempt_generation":2`, `"attempt_generation":2147483648`, 1),
	}
	for i, raw := range cases {
		if _, _, err := parseEnvelope([]byte(raw)); err == nil {
			t.Errorf("case %d unexpectedly accepted", i)
		}
	}
	if _, _, err := parseEnvelope([]byte(strings.Repeat("x", MaxEnvelopeBytes+1))); err == nil {
		t.Fatal("oversized body accepted")
	}
}

func TestValidateStoredPayloadAllKinds(t *testing.T) {
	valid := map[string]string{
		"news_processing":      `{}`,
		"news_digest":          `{}`,
		"telegram_digest":      `{}`,
		"backup":               `{}`,
		"reprocess_failed":     `{}`,
		"article_reprocess":    `{"article_ids":[8,3]}`,
		"summaries_generate":   `{}`,
		"source_test":          `{"source_id":4}`,
		"serbian_digest_retry": `{"russian_edition_id":"10000000-0000-4000-8000-000000000001"}`,
	}
	for kind, raw := range valid {
		got, err := validatePayload(kind, json.RawMessage(raw))
		if err != nil {
			t.Errorf("%s rejected: %v", kind, err)
			continue
		}
		if kind == "article_reprocess" && string(got) != `{"article_ids":[3,8],"force":true,"reextract_media":false}` {
			t.Errorf("article_reprocess normalization = %s", got)
		}
	}
	bad := map[string]string{
		"news_processing":      `{"surprise":1}`,
		"news_digest":          `{"schedule_id":3}`,
		"telegram_digest":      `{"timeout_seconds":0}`,
		"backup":               `{"timeout_seconds":true}`,
		"reprocess_failed":     `{"limit":101}`,
		"article_reprocess":    `{"article_ids":[2,2]}`,
		"summaries_generate":   `{"anything":false}`,
		"source_test":          `{}`,
		"serbian_digest_retry": `{"russian_edition_id":"10000000-0000-4000-8000-000000000001","destination":"untrusted"}`,
	}
	for kind, raw := range bad {
		if _, err := validatePayload(kind, json.RawMessage(raw)); err == nil {
			t.Errorf("%s invalid payload accepted: %s", kind, raw)
		}
	}
}

func TestPayloadScalarTypesMatchPythonStrictTypes(t *testing.T) {
	invalid := []struct {
		kind, payload string
	}{
		{"source_test", `{"source_id":"123"}`},
		{"reprocess_failed", `{"limit":"5"}`},
		{"news_processing", `{"schedule_id":"3","scheduled_for":"2026-10-27T12:34:56"}`},
		{"article_reprocess", `{"article_ids":["4"]}`},
		{"news_digest", `{"run_processing":null}`},
		{"news_digest", `{"send_telegram":null}`},
		{"reprocess_failed", `{"dry_run":null}`},
		{"article_reprocess", `{"article_ids":[4],"force":null}`},
	}
	for _, tc := range invalid {
		if _, err := validatePayload(tc.kind, json.RawMessage(tc.payload)); err == nil {
			t.Errorf("%s accepted non-Python scalar payload %s", tc.kind, tc.payload)
		}
	}
}

func TestScheduledForUsesPythonISOFormatRoundTrip(t *testing.T) {
	valid := []string{
		"2026-10-27T12:34:56",
		"2026-10-27T12:34:56.123456",
		"2026-10-27T12:34:56+00:00",
		"2026-10-27T12:34:56.123456+02:30",
		"2026-10-27T12:34:56+02:30:15",
	}
	for _, stamp := range valid {
		raw, _ := json.Marshal(map[string]any{"schedule_id": 1, "scheduled_for": stamp})
		if _, err := validatePayload("news_processing", raw); err != nil {
			t.Errorf("Python canonical timestamp %q rejected: %v", stamp, err)
		}
	}
	invalid := []string{
		"2026-10-27",
		"2026-10-27T12:34",
		"2026-10-27 12:34:56",
		"2026-10-27T12:34:56Z",
		"2026-10-27T12:34:56.1",
		"2026-10-27T12:34:56.1234567",
		"2026-10-27T12:34:56.000000",
		"2026-10-27T12:34:56+02:30:00",
	}
	for _, stamp := range invalid {
		raw, _ := json.Marshal(map[string]any{"schedule_id": 1, "scheduled_for": stamp})
		if _, err := validatePayload("news_processing", raw); err == nil {
			t.Errorf("noncanonical Python datetime %q accepted", stamp)
		}
	}
}

func TestAuthPrecedesBodyParsingAndStatusShape(t *testing.T) {
	s, err := New(Config{DatabaseURL: "postgres://example", Token: "secret"}, Handlers{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, authorization string
		want                int
	}{{"missing", "", 401}, {"wrong scheme", "bearer secret", 401}, {"valid", "Bearer secret", 503}} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/internal/v1/job-executions", strings.NewReader("not json"))
			if tc.authorization != "" {
				req.Header.Set("Authorization", tc.authorization)
			}
			rr := httptest.NewRecorder()
			s.Handler().ServeHTTP(rr, req)
			if rr.Code != tc.want {
				t.Fatalf("status = %d, want %d: %s", rr.Code, tc.want, rr.Body.String())
			}
		})
	}
}

func TestSafeResultRejectsContentAndBounds(t *testing.T) {
	for _, raw := range []string{
		`{"secret":"x"}`, `{"articles_processed":-1}`, `{"processed":1e999}`,
		`{"success":1}`, `{"connection_ok":0}`, `{"retryable":null}`,
		`{"status":"success","status":"warning"}`,
		`{"article_outcomes":[{"article_id":1,"article_id":2,"state":"completed","reason":"other"}],"article_outcomes_total":1,"article_outcomes_omitted":0}`,
		`{"article_outcomes":[{"article_id":true,"state":"completed","reason":"other"}],"article_outcomes_total":1,"article_outcomes_omitted":0}`,
		`{"article_outcomes":[{"article_id":1,"state":"completed","reason":"private text"}],"article_outcomes_total":1,"article_outcomes_omitted":0}`,
		`{"article_outcomes":[{"article_id":1,"state":"completed","reason":"other","title":"private"}],"article_outcomes_total":1,"article_outcomes_omitted":0}`,
		`{"article_outcomes":[{"article_id":1,"state":"completed","reason":"other"}],"article_outcomes_total":0,"article_outcomes_omitted":0}`,
		`{"article_outcomes":[{"article_id":1,"state":"completed","reason":"other"}],"article_outcomes_total":1,"article_outcomes_omitted":1}`,
		strings.Repeat(" ", MaxResultBytes+1),
	} {
		if _, err := safeResult(json.RawMessage(raw)); err == nil {
			t.Errorf("unsafe result accepted: %.40s", raw)
		}
	}
	if _, err := safeResult(json.RawMessage(`{"status":"warning","processed":3}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := safeResult(json.RawMessage(`{"article_outcomes":[{"article_id":1,"state":"completed","reason":"other"}],"article_outcomes_total":2,"article_outcomes_omitted":1}`)); err != nil {
		t.Fatalf("valid bounded article outcomes rejected: %v", err)
	}
}

func TestPipelineCancellationAndTerminalProjection(t *testing.T) {
	started := make(chan struct{})
	returned := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(returned) }) }
	defer release()
	p := PipelineFunc(func(ctx context.Context, _ ExecutionContext, _ string, _ json.RawMessage) (Result, error) {
		close(started)
		<-ctx.Done()
		// Deliberately ignore cancellation until released by the test: the runner
		// must not let this late result overwrite a newer lease/attempt.
		<-returned
		return Result{Value: json.RawMessage(`{"processed":99}`)}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, _ = p.Execute(ctx, ExecutionContext{}, "news_processing", json.RawMessage(`{}`))
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("pipeline did not start within bound")
	}
	cancel()
	release()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("cancellation-resistant fake did not unwind")
	}
}
