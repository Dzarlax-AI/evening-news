package callbacks

import (
	"context"
	"encoding/json"
	"net/url"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-domain/selectors"
	"github.com/Dzarlax-AI/evening-news/go/pagelayout"
)

const extractionStudy = "extraction_study"
const maxStudyHTMLBytes = 2 * 1024 * 1024

type SnapshotAnalyzer interface {
	AnalyzeSnapshot(context.Context, string, string, string, string) (*selectors.Analysis, error)
}

type studyInput struct {
	URL            string `json:"url"`
	DocumentURL    string `json:"document_url"`
	HTML           string `json:"html"`
	SnapshotDigest string `json:"snapshot_digest"`
}

type studyResult struct {
	SnapshotDigest string            `json:"snapshot_digest"`
	Layout         pagelayout.Layout `json:"layout"`
}

// The extractor has already admitted the document URL. The analyzer only
// uses these identifiers as context and never fetches them.
func validStudyURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && len(raw) <= 8192 && (u.Scheme == "http" || u.Scheme == "https") &&
		u.Hostname() != "" && u.User == nil && u.Opaque == ""
}

func decodeStudyInput(raw json.RawMessage) (studyInput, error) {
	var input studyInput
	if _, err := object(raw, []string{"url", "document_url", "html", "snapshot_digest"}, nil); err != nil {
		return input, err
	}
	if json.Unmarshal(raw, &input) != nil || input.HTML == "" || !utf8.ValidString(input.HTML) ||
		!validStudyURL(input.URL) || !validStudyURL(input.DocumentURL) {
		return input, invalidInput
	}
	if len(input.HTML) > maxStudyHTMLBytes {
		return input, ErrTextLimit
	}
	if !hashRE.MatchString(input.SnapshotDigest) || hash([]byte(input.HTML)) != input.SnapshotDigest {
		return input, invalidInput
	}
	return input, nil
}

func (c *ArticleCompute) runExtractionStudy(ctx context.Context, raw json.RawMessage) (studyResult, error) {
	input, err := decodeStudyInput(raw)
	if err != nil {
		return studyResult{}, err
	}
	out := studyResult{SnapshotDigest: input.SnapshotDigest, Layout: pagelayout.Layout{PageKind: "unknown"}}
	if c.SelectorAnalyzer == nil {
		return out, nil
	}
	domain, _ := URLHostname(input.DocumentURL)
	a, err := c.SelectorAnalyzer.AnalyzeSnapshot(ctx, input.DocumentURL, input.HTML, "news", domain)
	if ctx.Err() != nil {
		return out, ctx.Err()
	}
	// An unavailable or invalid model answer is a normal fallback. Cache this
	// result under the same attempt and snapshot so delivery does not retry it.
	if err != nil || a == nil {
		return out, nil
	}
	switch a.PageKind {
	case "article":
		out.Layout = pagelayout.Layout{PageKind: a.PageKind, ArticleBody: a.ArticleBody, EditorialCover: a.EditorialCover, ExcludeBlocks: a.ExcludeBlocks}
	case "listing":
		content := a.ContentSelectors
		if len(content) > 16 {
			content = content[:16]
		}
		out.Layout = pagelayout.Layout{PageKind: a.PageKind, ContentSelectors: content}
	}
	return out, nil
}
