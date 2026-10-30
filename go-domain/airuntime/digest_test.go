package airuntime

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDigestAcceptIdentityAndConflict(t *testing.T) {
	s := NewServer(Config{Token: "01234567890123456789012345678901", RPS: 3}, nil)
	deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond).Format(time.RFC3339Nano)
	input := digestInput{Operation: "summary_status", OperationID: "00000000-0000-0000-0000-000000000003", DomainInstanceID: "00000000-0000-0000-0000-000000000004", Envelope: digestEnvelope{Version: 1, JobID: "00000000-0000-0000-0000-000000000001", Attempt: 1, OwnerID: "00000000-0000-0000-0000-000000000002", Generation: 1, BackendPID: 1, ExecutorID: "00000000-0000-0000-0000-000000000005", Deadline: deadline}, Fingerprint: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", Input: json.RawMessage(`{}`), Kind: "news_digest", Payload: json.RawMessage(`{}`), ExecutorBackendPID: 2}
	req := digestRequest{commonRequest: commonRequest{Version: 1, RequestID: "test", DeadlineAt: deadline}, Input: input}
	invoke := func(r digestRequest) int {
		b, _ := json.Marshal(r)
		q := httptest.NewRequest(http.MethodPost, "/v2/digest/operations", bytes.NewReader(b))
		q.Header.Set("Authorization", "Bearer "+s.config.Token)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, q)
		return w.Code
	}
	if code := invoke(req); code != 200 {
		t.Fatalf("accept %d", code)
	}
	if code := invoke(req); code != 200 {
		t.Fatalf("duplicate %d", code)
	}
	req.Input.DomainInstanceID = "00000000-0000-0000-0000-000000000006"
	if code := invoke(req); code != 409 {
		t.Fatalf("conflict %d", code)
	}
}

func TestSerbianDigestRetryOnlyAcceptsEditionIdentity(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond).Format(time.RFC3339Nano)
	v := digestInput{Operation: "serbian_digest_retry", OperationID: "00000000-0000-0000-0000-000000000003", DomainInstanceID: "00000000-0000-0000-0000-000000000004", Envelope: digestEnvelope{Version: 1, JobID: "00000000-0000-0000-0000-000000000001", Attempt: 1, OwnerID: "00000000-0000-0000-0000-000000000002", Generation: 1, BackendPID: 1, ExecutorID: "00000000-0000-0000-0000-000000000005", Deadline: deadline}, Fingerprint: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a", Input: json.RawMessage(`{}`), Kind: "serbian_digest_retry", Payload: json.RawMessage(`{"russian_edition_id":"10000000-0000-4000-8000-000000000001"}`), ExecutorBackendPID: 2}
	if _, ok := validDigest(v); !ok {
		t.Fatal("valid retry identity rejected")
	}
	for _, payload := range []string{`{}`, `{"russian_edition_id":"bad"}`, `{"russian_edition_id":"10000000-0000-4000-8000-000000000001","destination":"untrusted"}`} {
		v.Payload = json.RawMessage(payload)
		if _, ok := validDigest(v); ok {
			t.Fatal("invalid retry identity accepted")
		}
	}
}
