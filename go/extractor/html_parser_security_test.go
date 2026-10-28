package extractor

import (
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
	nethtml "golang.org/x/net/html"
)

func TestHTMLParserPreservesTextAroundIgnoredEndTags(t *testing.T) {
	const repetitions = 8192
	const text = "x"
	input := strings.Repeat(text+"</ignored>", repetitions)
	want := strings.Repeat(text, repetitions)
	documentInput := "<article>" + input + "</article>"
	fosterParentInput := "<table>" + input + "</table>"

	t.Run("document", func(t *testing.T) {
		doc := assertBoundedParserAllocation(t, func() *goquery.Document {
			return parse(documentInput)
		})
		article := doc.Find("article")
		if got := article.Text(); got != want {
			t.Fatalf("document parser text length = %d, want %d", len(got), len(want))
		}
	})

	t.Run("table foster-parent handling", func(t *testing.T) {
		doc := assertBoundedParserAllocation(t, func() *goquery.Document {
			return parse(fosterParentInput)
		})
		tables := doc.Find("table")
		if tables.Length() != 1 {
			t.Fatal("table element was not preserved")
		}
		if got := doc.Find("body").Text(); got != want {
			t.Fatalf("foster-parented body text length = %d, want %d", len(got), len(want))
		}
		if got := tables.Text(); got != "" {
			t.Fatalf("table should not contain foster-parented text, got length %d", len(got))
		}
		previous := tables.Nodes[0].PrevSibling
		if previous == nil || previous.Type != nethtml.TextNode || previous.Data != want {
			t.Fatal("repeated text was not preserved immediately before the table")
		}
	})

	t.Run("rss fragment", func(t *testing.T) {
		got := assertBoundedParserAllocation(t, func() string {
			return sanitizeRSSHTML(input, "https://publisher.example/story")
		})
		if got != want {
			t.Fatalf("RSS fragment output length = %d, want %d", len(got), len(want))
		}
	})
}

func assertBoundedParserAllocation[T any](t *testing.T, parse func() T) T {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	result := parse()
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	// A generous ceiling catches quadratic text coalescing without relying on time.
	if allocated > 5<<20 {
		t.Fatalf("parser allocated %d bytes, above the 5 MiB ceiling", allocated)
	}
	return result
}

func BenchmarkHTMLParserRepeatedTextIgnoredEndTags(b *testing.B) {
	for _, repetitions := range []int{512, 2048, 8192} {
		input := strings.Repeat("x</ignored>", repetitions)
		documentInput := "<article>" + input + "</article>"
		b.Run("document/"+strconv.Itoa(repetitions), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			for b.Loop() {
				_ = parse(documentInput)
			}
		})
		b.Run("rss-fragment/"+strconv.Itoa(repetitions), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(input)))
			for b.Loop() {
				_ = sanitizeRSSHTML(input, "https://publisher.example/story")
			}
		})
	}
}
