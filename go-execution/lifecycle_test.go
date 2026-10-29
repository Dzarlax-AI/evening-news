package execution

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type deliveryRiskFixture struct{ mayHaveStarted bool }

func (e deliveryRiskFixture) Error() string                { return "fixture failure" }
func (e deliveryRiskFixture) DeliveryMayHaveStarted() bool { return e.mayHaveStarted }
func (e deliveryRiskFixture) SafeFailureReason() string    { return "fixture_failure" }

func TestPipelineFailureDispositionRespectsDeliveryStage(t *testing.T) {
	payload := json.RawMessage(`{}`)
	for _, tc := range []struct {
		name, wantState, wantCode string
		err                       error
	}{
		{"before_publish", "failed", "worker_error", deliveryRiskFixture{}},
		{"ambiguous_publish", "delivery_unknown", "external_outcome_unknown", deliveryRiskFixture{mayHaveStarted: true}},
		{"cancelled_without_delivery", "interrupted", "worker_interrupted", errors.Join(deliveryRiskFixture{}, context.Canceled)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state, code := pipelineFailureDisposition("telegram_digest", payload, tc.err, nil)
			if state != tc.wantState || code != tc.wantCode {
				t.Fatalf("disposition=(%q,%q) want (%q,%q)", state, code, tc.wantState, tc.wantCode)
			}
		})
	}
	if _, code := pipelineFailureDisposition("news_processing", payload, errors.New("ordinary failure"), nil); code != "worker_error" {
		t.Fatalf("unmarked failure code=%q", code)
	}
}
