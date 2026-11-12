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
		case "digest_domain_status_failed", "digest_processing_failed", "digest_operational_settings_failed", "digest_operational_settings_invalid_failed", "digest_summary_status_failed", "digest_summary_status_result_failed", "digest_summaries_generate_failed", "digest_summaries_generate_result_failed", "digest_prepare_failed", "digest_prepare_result_failed", "digest_publish_failed", "digest_publish_result_failed",
			"serbian_category_translation_missing", "serbian_article_translation_missing", "serbian_source_stale", "serbian_attempts_exhausted", "serbian_provider_failed", "serbian_delivery_rejected", "telegram_delivery_rejected", "serbian_edition_missing", "editorial_not_ready", "editorial_provider_timeout", "editorial_provider_unavailable", "editorial_invalid_result", "editorial_resource_limit", "telegram_configuration_incomplete", "prepared_digest_missing":
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
