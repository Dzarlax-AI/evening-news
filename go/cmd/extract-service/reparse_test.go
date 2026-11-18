package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go/pagelayout"
	"github.com/Dzarlax-AI/evening-news/go/service"
)

func reparseReq(html string, layout pagelayout.Layout) service.Request {
	h := sha256.Sum256([]byte(html))
	return service.Request{URL: "https://news.example/story", SourceType: "article", MaxBytes: service.MaxBytes, MaxChars: 100000, Reparse: &service.ReparseRequest{DocumentURL: "https://news.example/final", HTML: html, SnapshotDigest: hex.EncodeToString(h[:]), Layout: layout}}
}
func TestReparsePageStudySnapshotAndListing(t *testing.T) {
	html := `<main><article><div class="story"><p>The river research team measured water quality across several districts this spring. Their instruments recorded changes after rainfall and compared samples from nearby farms. Local authorities published the findings with a detailed explanation of the measurement process. Residents can now review the evidence and understand how upstream activity affects their drinking water.</p></div></article></main>`
	r := reparseReq(html, pagelayout.Layout{PageKind: "article", ArticleBody: ".story"})
	got := reparseArticle(context.Background(), r)
	if !got.Applied || got.Outcome != "success" || got.Result.URL != r.URL || got.FinalURL != r.Reparse.DocumentURL {
		t.Fatalf("article reparse failed: %+v", got)
	}
	r.Reparse.SnapshotDigest = strings.Repeat("0", 64)
	if got = reparseArticle(context.Background(), r); got.Applied {
		t.Fatal("mismatched snapshot applied")
	}
	var cards strings.Builder
	for i := 0; i < 12; i++ {
		fmt.Fprintf(&cards, `<li><a href="/story/%d">An individual headline about the city and its residents number %d</a></li>`, i, i)
	}
	list := `<main><ul>` + cards.String() + `</ul></main>`
	r = reparseReq(list, pagelayout.Layout{PageKind: "listing", ContentSelectors: []string{"li"}})
	got = reparseArticle(context.Background(), r)
	if !got.Applied || got.Outcome != "rejected" || got.Error == nil || got.Error.Reason != "non_article_listing" {
		t.Fatalf("listing not rejected: %+v", got)
	}
	r = reparseReq(html+list, pagelayout.Layout{PageKind: "listing", ContentSelectors: []string{"li"}})
	if got = reparseArticle(context.Background(), r); got.Applied {
		t.Fatal("article with recommendations rejected as listing")
	}
}
