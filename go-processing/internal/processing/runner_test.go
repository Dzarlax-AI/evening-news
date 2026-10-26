package processing

import "testing"

func TestParseCollectorPublicationDateAndPreserveNaiveWallTime(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{"2026-10-14T00:00:00", "2026-10-14 00:00:00"},
		{"2026-10-14T12:34:56.123456789-04:00", "2026-10-14 12:34:56"},
		{"Wed, 14 Oct 2026 12:34:56 -0400", "2026-10-14 12:34:56"},
	} {
		got := parseDate(tc.input)
		if got == nil || got.UTC().Format("2006-01-02 15:04:05") != tc.want {
			t.Errorf("parseDate(%q)=%v want wall time %s", tc.input, got, tc.want)
		}
	}
	if got := parseDate("not a publication date"); got != nil {
		t.Fatalf("malformed date produced a value: %v", got)
	}
}

func TestExtractionQualityRejectsShortConsentAndAcceptsSyntheticArticle(t *testing.T) {
	consent := "Before you continue to Google we use cookies and data to deliver services and improve quality."
	if isGoodExtractionContent(consent, 10000) {
		t.Fatal("short consent page passed extraction quality checks")
	}
	article := "Researchers published a detailed report describing the unusual results of a cooperative quantum experiment. " +
		"The team compared several independent measurements and found that the revised model explained observations more accurately. " +
		"Their analysis also describes remaining uncertainty, practical limitations, and plans for a larger follow-up study."
	if !isGoodExtractionContent(article, 10000) {
		t.Fatal("synthetic article content failed extraction quality checks")
	}
}

func TestScientificTitleFallbackIsLimitedToKnownResearchDomains(t *testing.T) {
	title := "Synthetic study of cooperative quantum systems"
	if !scientificTitleFallback(title, "https://arxiv.org/abs/1234") {
		t.Fatal("scientific paper title fallback was not enabled")
	}
	if scientificTitleFallback(title, "https://ordinary.example/news") {
		t.Fatal("ordinary site unexpectedly received scientific-title fallback")
	}
}
