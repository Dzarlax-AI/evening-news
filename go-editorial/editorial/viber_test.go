package editorial

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func viberFixture(t *testing.T, locale string) (Snapshot, Edition) {
	t.Helper()
	text := []string{"Prvi potvrđeni tekst", "Drugi potvrđeni tekst", "Treći potvrđeni tekst"}
	if locale != SerbianLocale {
		text = []string{"Первый подтверждённый текст", "Второй подтверждённый текст", "Третий подтверждённый текст"}
	}
	s := Snapshot{
		Date: "2026-10-27", CutoffUTC: time.Date(2026, 10, 27, 18, 0, 0, 0, time.UTC),
		SiteBaseURL: "https://news.example", EditionURL: "https://news.example/digest/2026-10-27?edition=fixture",
	}
	for i := 0; i < 3; i++ {
		categoryID, category := "4", "Tehnologija"
		if locale != SerbianLocale {
			category = "Технологии"
		}
		if i == 2 {
			categoryID, category = "5", "Kultura"
			if locale != SerbianLocale {
				category = "Культура"
			}
		}
		s.Articles = append(s.Articles, Article{ID: fmt.Sprint(1001 + i), Title: "Naslov " + fmt.Sprint(i+1), Summary: text[i], Content: text[i], SourceName: "PRIVATE SOURCE LABEL " + fmt.Sprint(i+1), URL: fmt.Sprintf("https://source.example/%d", i+1), CategoryID: categoryID, CategoryName: category})
	}
	story := func(i int, kind string) Story {
		a := s.Articles[i]
		return Story{Kind: kind, Title: a.Title, Summary: a.Summary, PrimaryArticleID: a.ID, ArticleIDs: []string{a.ID}, Evidence: []Evidence{{ArticleID: a.ID, Quote: a.Summary, Locale: locale}}}
	}
	e, err := FinalizeEdition(s, locale, []Story{story(1, "brief"), story(0, "main"), story(2, "brief")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s, e
}

func TestRenderViberUsesRenderOrderAndOmitsSources(t *testing.T) {
	s, e := viberFixture(t, SerbianLocale)
	rendered, err := RenderViber(s, SerbianLocale, e, ViberRenderOptions{AuthToken: "private-token", Sender: "private-sender"})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []string{e.Stories[1].ID, e.Stories[0].ID, e.Stories[2].ID}
	if fmt.Sprint(rendered.StoryIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("story order = %v, want %v", rendered.StoryIDs, wantIDs)
	}
	joined := strings.Join(rendered.Parts, "\n")
	for _, want := range []string{"*Evening News · 27. oktobar 2026.*", "*Glavno*", "*Još ukratko*", "*Tehnologija*", "*Kultura*", "https://news.example/article/1001?locale=sr-Latn", "https://news.example/article/1002?locale=sr-Latn", "https://news.example/article/1003?locale=sr-Latn", "https://news.example/digest/2026-10-27?edition=fixture"} {
		if !strings.Contains(joined, want) {
			t.Errorf("render missing %q", want)
		}
	}
	if strings.Contains(joined, "PRIVATE SOURCE LABEL") || strings.Contains(joined, "source.example") || strings.Contains(joined, "<a ") {
		t.Fatalf("Viber rendering leaked a source label or used link markup: %s", joined)
	}
	for _, metric := range rendered.Metrics {
		if metric.UTF16Units > ViberHardUTF16Limit || metric.JSONBytes > ViberHardJSONByteLimit {
			t.Fatalf("part exceeds hard limit: %+v", metric)
		}
	}
}

func TestRenderViberPreservesMarkerTextAndIsDeterministic(t *testing.T) {
	s, e := viberFixture(t, RussianLocale)
	e.Stories[1].Title = "Заголовок *с маркером*"
	e, err := FinalizeEdition(s, RussianLocale, e.Stories, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := RenderViber(s, RussianLocale, e, ViberRenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := RenderViber(s, RussianLocale, e, ViberRenderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(first.Parts) != fmt.Sprint(second.Parts) || fmt.Sprint(first.StoryIDs) != fmt.Sprint(second.StoryIDs) {
		t.Fatal("same input produced different output")
	}
	if !strings.Contains(strings.Join(first.Parts, "\n"), "Заголовок *с маркером*") {
		t.Fatal("marker text was altered")
	}
	if strings.Contains(strings.Join(first.Parts, "\n"), "**Заголовок") {
		t.Fatal("text with a marker was wrapped in formatting markers")
	}
}

func TestRenderViberSplitsAtStoryBoundariesWithStableNumbering(t *testing.T) {
	s, _ := viberFixture(t, RussianLocale)
	stories := make([]Story, 0, 5)
	for i := range s.Articles {
		s.Articles[i].Summary = strings.Repeat("A", 1800)
		s.Articles[i].Content = s.Articles[i].Summary
		stories = append(stories, Story{Kind: "main", Title: "Заголовок", Summary: s.Articles[i].Summary, PrimaryArticleID: s.Articles[i].ID, ArticleIDs: []string{s.Articles[i].ID}, Evidence: []Evidence{{ArticleID: s.Articles[i].ID, Quote: s.Articles[i].Summary[:400], Locale: RussianLocale}}})
	}
	for i := 3; i < 5; i++ {
		a := Article{ID: fmt.Sprint(1001 + i), Title: "Заголовок", Summary: strings.Repeat("A", 1800), Content: strings.Repeat("A", 1800), SourceName: "Private source", URL: fmt.Sprintf("https://source.example/%d", i+1), CategoryID: "4", CategoryName: "Технологии"}
		s.Articles = append(s.Articles, a)
		stories = append(stories, Story{Kind: "main", Title: a.Title, Summary: a.Summary, PrimaryArticleID: a.ID, ArticleIDs: []string{a.ID}, Evidence: []Evidence{{ArticleID: a.ID, Quote: a.Summary[:400], Locale: RussianLocale}}})
	}
	e, err := FinalizeEdition(s, RussianLocale, stories, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := RenderViber(s, RussianLocale, e, ViberRenderOptions{AuthToken: "secret", Sender: "sender"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := RenderViber(s, RussianLocale, e, ViberRenderOptions{AuthToken: "secret", Sender: "sender"})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Parts) < 2 || fmt.Sprint(first.Parts) != fmt.Sprint(second.Parts) {
		t.Fatalf("expected deterministic multipart output, got %d parts", len(first.Parts))
	}
	for i, part := range first.Parts {
		if !strings.Contains(part, fmt.Sprintf("Part %d/%d", i+1, len(first.Parts))) {
			t.Errorf("part %d does not contain its final numbering", i+1)
		}
		if !strings.Contains(part, s.EditionURL) {
			t.Errorf("part %d is missing the edition URL", i+1)
		}
		if !first.Metrics[i].SoftUTF16 || !first.Metrics[i].SoftJSONByte {
			t.Errorf("part %d crossed a soft budget unexpectedly: %+v", i+1, first.Metrics[i])
		}
	}
	if len(first.StoryIDs) != 5 {
		t.Fatalf("story IDs = %d, want 5", len(first.StoryIDs))
	}
}

func TestRenderViberRejectsSingleStoryAboveSoftBudget(t *testing.T) {
	s, e := viberFixture(t, RussianLocale)
	main := e.Stories[1]
	s.Articles = []Article{s.Articles[0]}
	s.SiteBaseURL = "https://news.example/" + strings.Repeat("x", 1800)
	s.EditionURL = "https://news.example/digest/" + strings.Repeat("y", 1800)
	main.Title = strings.Repeat("T", 1500)
	main.Summary = strings.Repeat("S", 1500)
	e, err := FinalizeEdition(s, RussianLocale, []Story{main}, nil)
	if err != nil {
		t.Fatal(err)
	}
	dateLabel, _ := localizedDateLabel(e.Date, RussianLocale)
	labels := renderLabels(RussianLocale)
	body := strings.Join([]string{
		viberBold("Evening News · "+dateLabel) + "\n" + fmt.Sprintf(labels.materials, len(s.Articles)),
		viberBold(labels.main),
		viberStoryBlock(main, map[string]Article{s.Articles[0].ID: s.Articles[0]}, e.SiteBaseURL, RussianLocale, false).text,
	}, "\n\n")
	completeText := viberFinalizePart(body, 1, 1, e.EditionURL)
	units, jsonBytes := ViberPayloadSize(completeText, "", "")
	if units <= ViberSoftUTF16Limit || units > ViberHardUTF16Limit || jsonBytes > ViberHardJSONByteLimit {
		t.Fatalf("fixture must be above soft and within hard limits, got UTF-16=%d JSON=%d", units, jsonBytes)
	}
	sourceName := s.Articles[0].SourceName
	rendered, err := RenderViber(s, RussianLocale, e, ViberRenderOptions{})
	if err == nil || err.Error() != "viber_story_too_large" {
		t.Fatalf("expected finite soft-limit error, got %v", err)
	}
	if len(rendered.Parts) != 0 || s.Articles[0].SourceName != sourceName {
		t.Fatal("rejected render returned output or changed its source snapshot")
	}
}

func TestViberPayloadSizeUTF16Edges(t *testing.T) {
	for _, target := range []int{6999, 7000, 7001} {
		text := strings.Repeat("a", target)
		units, _ := ViberPayloadSize(text, "sender", "token")
		if units != target {
			t.Fatalf("UTF-16 size = %d, want %d", units, target)
		}
	}
	units, _ := ViberPayloadSize("a🙂b", "", "")
	if units != 4 {
		t.Fatalf("emoji UTF-16 size = %d, want 4", units)
	}
}

func TestViberPayloadSizeJSONEdgesAndASCIIEscaping(t *testing.T) {
	_, overhead := ViberPayloadSize("", "", "")
	for _, target := range []int{30719, 30720, 30721} {
		text := strings.Repeat("a", target-overhead)
		_, bytes := ViberPayloadSize(text, "", "")
		if bytes != target {
			t.Fatalf("JSON size = %d, want %d", bytes, target)
		}
	}
	_, asciiBytes := ViberPayloadSize("čир🙂", "S", "T")
	if asciiBytes < len("\\u010d\\u0438\\u0440\\ud83d\\ude42") {
		t.Fatalf("non-ASCII text was not escaped for wire sizing: %d", asciiBytes)
	}
	_, escapedBytes := ViberPayloadSize("<>&\"\\\n", "", "")
	_, plainBytes := ViberPayloadSize("<>&\"\\", "", "")
	if escapedBytes <= plainBytes {
		t.Fatal("JSON control characters were not counted as escaped bytes")
	}
}

func TestRenderViberRejectsOversizedPayloadWithoutSecrets(t *testing.T) {
	s, e := viberFixture(t, RussianLocale)
	secret := strings.Repeat("private-secret", 400)
	_, err := RenderViber(s, RussianLocale, e, ViberRenderOptions{AuthToken: secret, Sender: "sender"})
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("expected safe oversized-payload error, got %v", err)
	}
}
