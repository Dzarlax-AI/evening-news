package callbacks

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/selectors"
)

type failingSnapshotAnalyzer struct{}

func (failingSnapshotAnalyzer) AnalyzeSnapshot(context.Context, string, string, string, string) (*selectors.Analysis, error) {
	return nil, errors.New("fixture provider unavailable")
}

func TestExtractionStudyValidatesSnapshotAndFallsBackOnProviderFailure(t *testing.T) {
	c := ArticleCompute{SelectorAnalyzer: failingSnapshotAnalyzer{}}
	html := "<main><article>Fixture</article></main>"
	in := studyInput{URL: "https://news.example/story", DocumentURL: "https://publisher.example/story", HTML: html, SnapshotDigest: hash([]byte(html))}
	encode := func(in studyInput) json.RawMessage {
		raw, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	raw := encode(in)
	if err := c.Validate(extractionStudy, raw); err != nil {
		t.Fatal(err)
	}
	value, err := c.Run(context.Background(), Request{Operation: extractionStudy, Input: raw}, authority.Fence{})
	if answer, ok := value.(studyResult); err != nil || !ok || answer.Layout.PageKind != "unknown" || answer.SnapshotDigest != in.SnapshotDigest {
		t.Fatalf("unexpected fallback: %+v, %v", value, err)
	}
	in.HTML += "tampered"
	if err := c.Validate(extractionStudy, encode(in)); err == nil {
		t.Fatal("mismatched snapshot accepted")
	}
	in.HTML = strings.Repeat("x", maxStudyHTMLBytes+1)
	in.SnapshotDigest = hash([]byte(in.HTML))
	if err := c.Validate(extractionStudy, encode(in)); !errors.Is(err, ErrTextLimit) {
		t.Fatalf("oversized snapshot: %v", err)
	}
}
