package extractor

import (
	"context"
	"strings"
	"testing"
)

// These fixtures preserve the candidate-selection guarantees of the retired
// Python HTML processor in the Go implementation that now owns extraction.
func TestQualityOversizedParentPreservesValidArticle(t *testing.T) {
	valid := "Researchers published new evidence about distant galaxies and stellar formation. Scientists compared detailed observations across several independent instruments. Their findings explain important physical processes and future experimental priorities."
	for _, method := range []string{"enhanced_selectors", "enhanced_heuristics"} {
		t.Run(method, func(t *testing.T) {
			html := "<main>" + strings.Repeat("navigation ", 80) + "<article><p>" + valid + "</p></article></main>"
			result := Extract(context.Background(), Input{URL: "https://example.test/article", HTML: html, Strategy: method, MaxChars: 500})
			if result.State != "extracted" || result.Content != valid {
				t.Fatalf("valid child lost under oversized parent: state=%s content=%q", result.State, result.Content)
			}
		})
	}
}

func TestQualityDeepOversizedContainersPreserveValidCandidate(t *testing.T) {
	valid := "Researchers published new evidence about distant galaxies and stellar formation. Scientists compared detailed observations across several independent instruments. Their findings explain important physical processes and future experimental priorities."
	html := strings.Repeat("<div>", 60) + strings.Repeat("large ", 200) + "<p>" + valid + "</p>" + strings.Repeat("</div>", 60)
	result := Extract(context.Background(), Input{URL: "https://example.test/article", HTML: html, Strategy: "enhanced_heuristics", MaxChars: 500})
	if result.State != "extracted" || result.Content != valid {
		t.Fatalf("nested containers crowded out bounded candidate: state=%s content=%q", result.State, result.Content)
	}
}
