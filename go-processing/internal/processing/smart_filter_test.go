package processing

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type smartFilterGolden struct {
	Version int    `json:"version"`
	Case    string `json:"case"`
	Article struct {
		Title           string `json:"title"`
		Content         string `json:"content"`
		URL             string `json:"url"`
		SourceType      string `json:"source_type"`
		AllowExtraction bool   `json:"allow_extraction"`
	} `json:"article"`
	Expected struct {
		ShouldProcess bool   `json:"should_process"`
		Reason        string `json:"reason"`
	} `json:"expected"`
}

func TestDuplicateCacheMatchesLegacyOwnerAndExpiryRules(t *testing.T) {
	cache := NewDuplicateCache()
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	ownerID := int64(41)
	hash, duplicate, cacheHit := cache.Lookup("  Synthetic ARTICLE body  ", ownerID, now)
	if hash != contentHash("Synthetic ARTICLE body") || duplicate || cacheHit {
		t.Fatalf("first lookup = (%q, %v, %v)", hash, duplicate, cacheHit)
	}
	cache.Remember(hash, &ownerID, now)
	if _, duplicate, cacheHit = cache.Lookup("synthetic article BODY", ownerID, now.Add(time.Hour)); duplicate || !cacheHit {
		t.Fatalf("owner replay should be admitted from cache: duplicate=%v cacheHit=%v", duplicate, cacheHit)
	}
	if _, duplicate, cacheHit = cache.Lookup("synthetic article BODY", 42, now.Add(time.Hour)); !duplicate || !cacheHit {
		t.Fatalf("different article should be a duplicate: duplicate=%v cacheHit=%v", duplicate, cacheHit)
	}
	if _, duplicate, cacheHit = cache.Lookup("synthetic article BODY", 42, now.Add(24*time.Hour)); duplicate || cacheHit {
		t.Fatalf("entry exactly 24 hours old must expire: duplicate=%v cacheHit=%v", duplicate, cacheHit)
	}

	dbHitHash := contentHash("durable hit")
	cache.Remember(dbHitHash, nil, now)
	if _, duplicate, cacheHit = cache.Lookup("durable hit", ownerID, now.Add(time.Minute)); !duplicate || !cacheHit {
		t.Fatalf("durable hit remembered without owner must block replay: duplicate=%v cacheHit=%v", duplicate, cacheHit)
	}
}

func TestSmartFilterMatchesFrozenPythonCorpus(t *testing.T) {
	path := filepath.Join("..", "..", "..", "tests", "fixtures", "smart-filter-v1", "corpus.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []smartFilterGolden
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) < 20 {
		t.Fatalf("golden corpus unexpectedly small: %d", len(fixtures))
	}
	seen := map[string]bool{}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.Case, func(t *testing.T) {
			if fixture.Version != 1 || fixture.Case == "" || seen[fixture.Case] {
				t.Fatalf("invalid or duplicate v1 fixture case %q", fixture.Case)
			}
			seen[fixture.Case] = true
			got, reason := shouldProcess(fixture.Article.Title, fixture.Article.Content, fixture.Article.URL,
				fixture.Article.SourceType, fixture.Article.AllowExtraction)
			if got != fixture.Expected.ShouldProcess || reason != fixture.Expected.Reason {
				t.Fatalf("got (%v, %q), want (%v, %q)", got, reason, fixture.Expected.ShouldProcess, fixture.Expected.Reason)
			}
		})
	}
}

func TestSmartFilterUnicodeLengthAndExtractionReasons(t *testing.T) {
	title := "Synthetic Unicode title"
	pass, reason := shouldProcess(title, strings.Repeat("я", 50000), "https://example.test/item", "rss", false)
	if !pass || reason != "Passed all filters (quality: 0.65)" {
		t.Fatalf("50,000-codepoint body: got (%v, %q)", pass, reason)
	}
	pass, reason = shouldProcess(title, strings.Repeat("я", 50001), "https://example.test/item", "rss", false)
	if pass || reason != "Content too short (50001 chars < 10)" {
		t.Fatalf("50,001-codepoint body: got (%v, %q)", pass, reason)
	}
	for _, reason := range []string{
		"Content too short (0 chars < 10)",
		"Metadata/low-quality content detected (no extraction possible)",
		"Metadata/low-quality content detected (needs extraction)",
		"Content very short (20 chars, 4 words) - likely RSS summary (needs extraction)",
	} {
		if !reasonNeedsExtraction(reason) {
			t.Errorf("legacy extraction trigger not recognized: %q", reason)
		}
	}
	if reasonNeedsExtraction("Spam patterns detected") {
		t.Fatal("spam rejection must not trigger extraction")
	}
}

func TestSmartFilterUnicodeLoweringIsSafeAcrossWorkers(t *testing.T) {
	inputs := []string{
		"ΟΣ ΣΟ ΜΆΙΟΣ ẞ İSTANBUL",
		"МАШИНА ЁЖИК ИЗ СИНТЕТИЧЕСКОГО ТЕКСТА",
		"Straße Œuvre İ I\u0307 Σς",
		"ქართული ტექსტი და ქართული ასოები",
		"Հայերեն տեքստ և Unicode լեզուներ",
		"ＫＥＬＶＩＮ KELVIN ÅNGSTRÖM",
		"Synthetic ARTICLE with Ё, İ, Σ, and K characters",
	}
	want := make([]string, len(inputs))
	for i, input := range inputs {
		want[i] = lower(input)
	}

	const workers = 32 // Exceeds the processor's default worker count.
	const repeats = 200
	var wg sync.WaitGroup
	errors := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; n < repeats; n++ {
				i := (worker + n) % len(inputs)
				if got := lower(inputs[i]); got != want[i] {
					errors <- "Unicode lowercase output changed under concurrency"
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
