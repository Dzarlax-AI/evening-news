package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type fakeRepository struct {
	received   SubmitCommand
	payload    map[string]any
	result     SubmitResult
	accept     AcceptedOccurrence
	err        error
	acceptHash string
}

func (f *fakeRepository) Submit(_ context.Context, c SubmitCommand, p map[string]any, _ string) (SubmitResult, error) {
	f.received = c
	f.payload = p
	return f.result, f.err
}
func (f *fakeRepository) Get(context.Context, string) (Job, error)     { return Job{}, nil }
func (f *fakeRepository) Status(context.Context) (WorkerStatus, error) { return WorkerStatus{}, nil }
func (f *fakeRepository) AcceptOccurrence(_ context.Context, c OccurrenceCommand, p map[string]any, m OccurrenceMetadata, h string, l int64) (AcceptedOccurrence, error) {
	f.payload = p
	f.acceptHash = h
	return f.accept, f.err
}

func TestPythonCanonicalPayloadAndOccurrenceHashes(t *testing.T) {
	payload, err := ValidatePayload("article_reprocess", map[string]any{"article_ids": []int{3, 1}})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := CanonicalJSON(payload)
	if err != nil {
		t.Fatal(err)
	}
	coalesce := sha256.Sum256([]byte("article_reprocess\x00" + string(canonical)))
	if got := hex.EncodeToString(coalesce[:]); got != "df44516b04e1b98021ce8c15b0b67c874687e333b85b7943a12b2b9f19e76f7d" {
		t.Fatalf("Python coalesce hash changed: %s (%s)", got, canonical)
	}
	cmd := validOccurrence()
	cmd.Payload = map[string]any{"send_telegram": false}
	fake := &fakeRepository{accept: AcceptedOccurrence{OccurrenceID: cmd.OccurrenceID, JobID: "job", Created: true}}
	if _, err = New(fake).AcceptOccurrence(context.Background(), cmd); err != nil {
		t.Fatal(err)
	}
	if fake.acceptHash != "8f485c9a70dfdfe9df71b504734d0a3f2087a5b6cac4ae7a4f405d9bec913e90" {
		t.Fatalf("Python schedule envelope hash changed: %s", fake.acceptHash)
	}
}

func TestValidatePayloadNormalizesAndRejectsMalformed(t *testing.T) {
	got, err := ValidatePayload("article_reprocess", map[string]any{"article_ids": []int{9, 2}, "reextract_media": true})
	if err != nil {
		t.Fatal(err)
	}
	ids := got["article_ids"].([]int)
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 9 || got["force"] != true {
		t.Fatalf("unexpected normalized payload: %#v", got)
	}
	cases := []struct {
		k string
		p map[string]any
	}{
		{"unknown", map[string]any{}}, {"news_processing", map[string]any{"timeout_seconds": true}},
		{"news_processing", map[string]any{"schedule_id": 1}}, {"source_test", map[string]any{"source_id": 0}},
		{"article_reprocess", map[string]any{"article_ids": []int{1, 1}}}, {"news_digest", map[string]any{"send_telegram": "false"}},
	}
	for _, tc := range cases {
		if _, err := ValidatePayload(tc.k, tc.p); err == nil {
			t.Errorf("expected rejection kind=%s payload=%#v", tc.k, tc.p)
		}
	}
}
func TestSubmitValidatesBeforeRepositoryAndHashesKey(t *testing.T) {
	f := &fakeRepository{result: SubmitResult{Job: Job{ID: "00000000-0000-0000-0000-000000000001"}, Created: true}}
	svc := New(f)
	cmd := SubmitCommand{Kind: "article_reprocess", Payload: map[string]any{"article_ids": []int{3, 1}}, IdempotencyKey: "web:request-1"}
	got, err := svc.Submit(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Created {
		t.Fatal("expected created")
	}
	if f.payload["force"] != true {
		t.Fatalf("defaults not applied: %#v", f.payload)
	}
	if _, err := svc.Submit(context.Background(), SubmitCommand{Kind: "article_reprocess", Payload: map[string]any{"article_ids": []int{1}}, IdempotencyKey: "bad/key"}); err == nil {
		t.Fatal("expected invalid idempotency key")
	}
	if _, err := svc.Submit(context.Background(), SubmitCommand{Kind: "article_reprocess", Payload: map[string]any{}, IdempotencyKey: "ok"}); err == nil {
		t.Fatal("expected invalid payload")
	}
}
func TestPythonISOCanonicalPayloadTimes(t *testing.T) {
	for _, value := range []string{"2026-10-26T10:00:00", "2026-10-26T10:00:00.123000", "2026-10-26T10:00:00+00:00", "2026-10-26T10:00:00.123400-04:00"} {
		payload, err := ValidatePayload("news_processing", map[string]any{"schedule_id": 1, "scheduled_for": value})
		if err != nil || payload["scheduled_for"] != value {
			t.Errorf("canonical Python ISO timestamp %q rejected: %v", value, err)
		}
	}
	for _, value := range []string{"2026-10-26T10:00:00Z", "2026-10-26T10:00:00.1", "2026-10-26 10:00:00"} {
		if _, err := ValidatePayload("news_processing", map[string]any{"schedule_id": 1, "scheduled_for": value}); err == nil {
			t.Errorf("noncanonical ISO timestamp %q accepted", value)
		}
	}
}

func TestOccurrenceValidationAndExistingReceiptCanBeReplayedWithoutWorker(t *testing.T) {
	cmd := OccurrenceCommand{Version: 1, OccurrenceID: "00000000-0000-4000-8000-000000000001", IdempotencyKey: "00000000-0000-4000-8000-000000000001", ScheduleKey: "daily.digest", ScheduleRevision: 2, ScheduledFor: "2026-10-26T10:00:00Z", Operation: "news_digest", Payload: map[string]any{"send_telegram": false}, WorkerMode: false, DispatchMode: "disabled"}
	if err := ValidateOccurrence(cmd); err != nil {
		t.Fatal(err)
	}
	f := &fakeRepository{accept: AcceptedOccurrence{OccurrenceID: cmd.OccurrenceID, JobID: "job-1", Created: false}}
	got, err := New(f).AcceptOccurrence(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if got.Created || got.JobID != "job-1" {
		t.Fatalf("unexpected duplicate result: %#v", got)
	}
	cmd.IdempotencyKey = "other"
	if _, err = New(f).AcceptOccurrence(context.Background(), cmd); err == nil {
		t.Fatal("expected occurrence/header id mismatch")
	}
}
func TestScheduledWorkerErrorsAreTyped(t *testing.T) {
	f := &fakeRepository{err: ErrWorkerUnavailable}
	cmd := validOccurrence()
	cmd.Payload = map[string]any{"send_telegram": false}
	cmd.WorkerMode = true
	cmd.DispatchMode = "service"
	_, err := New(f).AcceptOccurrence(context.Background(), cmd)
	if !errors.Is(err, ErrWorkerUnavailable) {
		t.Fatalf("expected typed worker error, got %v", err)
	}
}
func validOccurrence() OccurrenceCommand {
	return OccurrenceCommand{Version: 1, OccurrenceID: "00000000-0000-4000-8000-000000000001", IdempotencyKey: "00000000-0000-4000-8000-000000000001", ScheduleKey: "daily.digest", ScheduleRevision: 1, ScheduledFor: "2026-10-26T10:00:00Z", Operation: "news_digest", Payload: map[string]any{}}
}

func TestJobJSONUsesPythonNaiveUTCISOFormat(t *testing.T) {
	created := time.Date(2026, 10, 26, 10, 0, 0, 123000000, time.UTC)
	encoded, err := json.Marshal(Job{ID: "job", CreatedAt: &created})
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err = json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	if value["created_at"] != "2026-10-26T10:00:00.123000" {
		t.Fatalf("unexpected timestamp encoding: %s", encoded)
	}
}
