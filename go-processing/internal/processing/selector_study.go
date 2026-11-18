package processing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/url"
	"time"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

const maxStudyHTMLBytes = 2 * 1024 * 1024
const maxStudyWireBytes = 8 * 1024 * 1024

type extractionStudySnapshot struct {
	DocumentURL    string `json:"document_url"`
	HTML           string `json:"html"`
	SnapshotDigest string `json:"snapshot_digest"`
}

type extractionStudyReplay struct {
	extractionStudySnapshot
	Layout map[string]any `json:"layout"`
}

func (s extractionStudySnapshot) valid() bool {
	u, err := url.Parse(s.DocumentURL)
	if err != nil || len(s.DocumentURL) > 8192 || u.Hostname() == "" || u.User != nil ||
		(u.Scheme != "https" && u.Scheme != "http") || s.HTML == "" || len(s.HTML) > maxStudyHTMLBytes || !utf8.ValidString(s.HTML) {
		return false
	}
	digest := sha256.Sum256([]byte(s.HTML))
	return s.SnapshotDigest == hex.EncodeToString(digest[:])
}

func (p *Processor) applyExtractionStudy(ctx context.Context, fence store.Fence, article store.Article, attempt int, fingerprint string, original Extracted) (Extracted, bool) {
	snapshot := original.study
	if !p.Extractor.EnablePageStudy || snapshot == nil || ctx.Err() != nil {
		return original, false
	}
	studyCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	answer, err := p.AI.Call(studyCtx, fence, article, attempt, fingerprint, "extraction_study", map[string]any{
		"url": article.URL, "document_url": snapshot.DocumentURL, "html": snapshot.HTML, "snapshot_digest": snapshot.SnapshotDigest,
	})
	if err != nil || answer["snapshot_digest"] != snapshot.SnapshotDigest {
		return original, false
	}
	layout, ok := answer["layout"].(map[string]any)
	if !ok || (layout["page_kind"] != "article" && layout["page_kind"] != "listing") {
		return original, false
	}

	extractor := p.Extractor
	maxBytes := extractor.MaxBytes
	if maxBytes < 1 || maxBytes > 5*1024*1024 {
		maxBytes = 5 * 1024 * 1024
	}
	// The initial request may have used the browser's separate byte budget.
	maxBytes = max(maxBytes, len(snapshot.HTML))
	replay := &extractionStudyReplay{extractionStudySnapshot: *snapshot, Layout: layout}
	candidate, err := extractor.admittedAttempt(studyCtx, article.URL, article.Title, article.SourceType, "", "", p.textLimit(), false, maxBytes, replay)
	if err != nil || !candidate.applied {
		return original, false
	}
	switch layout["page_kind"] {
	case "listing":
		if candidate.Outcome == "rejected" && candidate.Terminal && candidate.Error == "unsupported_input" && candidate.failureReason == "non_article_listing" &&
			candidate.Content == "" && candidate.ImageURL == "" && len(candidate.MediaFiles) == 0 {
			candidate.Error = "non_article_listing"
			return candidate, true
		}
	case "article":
		if candidate.Error == "" && candidate.Outcome == "success" && candidate.MethodUsed == "ai_selectors" &&
			isGoodExtractionContent(candidate.Content, p.textLimit()) {
			return candidate, true
		}
	}
	return original, false
}
