package execution

import "errors"

// DeliveryRisk marks handler errors whose outcome is known to be before any
// external delivery attempt, or may be ambiguous after one has started.
// Unmarked handlers retain the conservative job-kind based classification.
type DeliveryRisk interface {
	DeliveryMayHaveStarted() bool
	SafeFailureReason() string
}

func SafeExecutionFailureReason(err error) string {
	var failure interface{ SafeFailureReason() string }
	if errors.As(err, &failure) {
		switch failure.SafeFailureReason() {
		case "digest_domain_status_failed", "digest_processing_failed", "digest_operational_settings_failed", "digest_operational_settings_invalid_failed", "digest_summary_status_failed", "digest_summary_status_result_failed", "digest_summaries_generate_failed", "digest_prepare_failed", "digest_prepare_result_failed", "digest_publish_failed", "digest_publish_result_failed":
			return failure.SafeFailureReason()
		}
	}
	return "handler_failed"
}

func DeliveryOutcomeUnknown(kind string, payload []byte, err error) bool {
	var risk DeliveryRisk
	if errors.As(err, &risk) {
		return risk.DeliveryMayHaveStarted()
	}
	return mayDeliver(kind, payload)
}
