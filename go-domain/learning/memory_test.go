package learning

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/callbacks"
)

func TestPatternLimitsAndRememberedContext(t *testing.T) {
	m := New(nil)
	m.loaded = true
	for d := 0; d < 550; d++ {
		domain := fmt.Sprintf("%03d.example.test", d)
		for i := 0; i < 12; i++ {
			m.patterns[domain] = append(m.patterns[domain], &pattern{domain: domain, selector: fmt.Sprintf("#%02d", i), method: "readability", success: i + 1})
		}
	}
	m.enforceLimits()
	total := 0
	for _, entries := range m.patterns {
		if len(entries) > 10 {
			t.Fatal("per-domain limit")
		}
		total += len(entries)
	}
	if len(m.patterns) > 500 || total > 2000 {
		t.Fatalf("domains=%d patterns=%d", len(m.patterns), total)
	}
	domain := "000.example.test"
	m.patterns[domain] = []*pattern{{domain: domain, selector: "main", method: "readability", success: 4, failure: 1}}
	input, _ := json.Marshal(map[string]string{"url": "https://" + domain + "/story"})
	r := callbacks.Request{RunID: "test", ArticleID: 1, ArticleAttempt: 1, InputFingerprint: "one", Input: input}
	got, err := m.Context(context.Background(), r, authority.Fence{})
	if err != nil || got.Selector == nil || *got.Selector != "main" {
		t.Fatalf("context=%v err=%v", got, err)
	}
	saved := m.take(r)
	if saved.Selector == nil || *saved.Selector != "main" {
		t.Fatal("missing remembered selector")
	}
	if m.take(r).Selector != nil {
		t.Fatal("context was not consumed")
	}
	for i := 0; i < 130; i++ {
		r.ArticleID = int64(i + 1)
		_, _ = m.Context(context.Background(), r, authority.Fence{})
	}
	if m.lru.Len() != 128 {
		t.Fatalf("contexts=%d", m.lru.Len())
	}
}
