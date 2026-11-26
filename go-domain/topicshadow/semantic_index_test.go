package topicshadow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type semanticRoundTrip func(*http.Request) (*http.Response, error)

func (f semanticRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestSemanticIndexWireAndRepeat(t *testing.T) {
	n := 0
	present := false
	var saved map[string]any
	client := &http.Client{Transport: semanticRoundTrip(func(r *http.Request) (*http.Response, error) {
		var b map[string]any
		if r.Body != nil {
			if e := json.NewDecoder(r.Body).Decode(&b); e != nil {
				t.Fatal(e)
			}
		}
		var response any = map[string]any{"result": []any{}}
		switch {
		case strings.HasSuffix(r.URL.Path, "/points/query"):
			if b["query"] == nil || b["filter"] == nil {
				t.Fatal("missing point query or temporal filter")
			}
			response = map[string]any{"result": map[string]any{"points": []any{}}}
		case strings.HasSuffix(r.URL.Path, "/embeddings"):
			n++
			if b["model"] != semanticModel || b["input"] != "Title\n\nSummary" {
				t.Fatal("incorrect embedding request")
			}
			response = map[string]any{"data": []any{map[string]any{"index": 0, "embedding": make([]float64, semanticDimensions)}}}
		case r.Method == http.MethodPut:
			points := b["points"].([]any)
			saved = points[0].(map[string]any)
			delete(saved, "vector")
			present = true
			response = map[string]any{"result": true}
		case strings.HasSuffix(r.URL.Path, "/points") && present:
			response = map[string]any{"result": []any{saved}}
		}
		data, _ := json.Marshal(response)
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(data))), Header: make(http.Header)}, nil
	})}
	s, e := NewSemanticIndex(SemanticIndexConfig{QdrantURL: "http://127.0.0.1:6333", OpenAIURL: "https://api.openai.com/v1", Collection: "news_topic_shadow_v1", OpenAIKey: "test", HTTPClient: client, ReserveEmbedding: func(context.Context, int) error { return nil }})
	if e != nil {
		t.Fatal(e)
	}
	now := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	a := IndexedArticle{ArticleID: 1, Claim: "claim", Title: "Title", Summary: "Summary", FetchedAt: now, AvailableAt: now}
	made, e := s.Index(context.Background(), a)
	if e != nil || !made {
		t.Fatalf("first: %v %v", made, e)
	}
	made, e = s.Index(context.Background(), a)
	if e != nil || made || n != 1 {
		t.Fatalf("repeat: %v %v calls %d", made, e, n)
	}
	if _, e = s.Neighbors(context.Background(), a, 32); e != nil || n != 1 {
		t.Fatalf("query: %v, embeddings %d", e, n)
	}
	a.Summary = "Changed"
	if semanticID(a) == semanticID(IndexedArticle{ArticleID: 1, Claim: "claim", Title: "Title", Summary: "Summary", FetchedAt: now, AvailableAt: now}) {
		t.Fatal("version IDs collide")
	}
}
