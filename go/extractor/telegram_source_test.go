package extractor

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseTelegramChannelMatchesPythonBaseline(t *testing.T) {
	baselineBytes, err := os.ReadFile("../../tests/fixtures/telegram/python_baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	var baselines map[string]json.RawMessage
	if err := json.Unmarshal(baselineBytes, &baselines); err != nil {
		t.Fatal(err)
	}
	fallback := time.Date(2026, 10, 22, 12, 34, 56, 0, time.UTC)
	cases := map[string]struct{ file, pageURL string }{
		"channel":            {"channel.html", "https://t.me/s/testchannel"},
		"fallback_message":   {"fallback_message.html", "https://t.me/testchannel"},
		"fallback_data_post": {"fallback_data_post.html", "https://t.me/s/testchannel"},
		"malformed":          {"malformed.html", "https://t.me/s/testchannel"},
	}
	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			html, err := os.ReadFile("../../tests/fixtures/telegram/" + testCase.file)
			if err != nil {
				t.Fatal(err)
			}
			var want, got any
			if err := json.Unmarshal(baselines[name], &want); err != nil {
				t.Fatal(err)
			}
			articles, err := ParseTelegramChannel(string(html), testCase.pageURL, "Offline Telegram Fixture", 20, fallback)
			if err != nil {
				t.Fatal(err)
			}
			actualBytes, err := json.Marshal(articles)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(actualBytes, &got); err != nil {
				t.Fatal(err)
			}
			for _, article := range got.([]any) {
				row := article.(map[string]any)
				if raw, ok := row["published_at"].(string); ok && raw != "" {
					parsed, err := time.Parse(time.RFC3339Nano, raw)
					if err != nil {
						t.Fatalf("parse Go published_at %q: %v", raw, err)
					}
					row["published_at"] = telegramPythonDateString(parsed)
				}
				for _, media := range row["media_files"].([]any) {
					item := media.(map[string]any)
					kind, _ := item["type"].(string)
					url, _ := item["url"].(string)
					if (kind == "poll" || kind == "location" || kind == "contact") && strings.HasPrefix(url, "#"+kind+"_") {
						item["url"] = "#" + kind + "_<runtime-hash>"
					}
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Go Telegram output differs from Python baseline at %s", firstTelegramDifference(got, want, "$"))
			}
		})
	}
}

func telegramPythonDateString(value time.Time) string {
	value = value.UTC().Truncate(time.Microsecond)
	if value.Nanosecond() == 0 {
		return value.Format("2006-01-02T15:04:05Z")
	}
	return value.Format("2006-01-02T15:04:05.000000Z")
}

func firstTelegramDifference(got, want any, path string) string {
	gotMap, gok := got.(map[string]any)
	wantMap, wok := want.(map[string]any)
	if gok && wok {
		for key, expected := range wantMap {
			actual, exists := gotMap[key]
			if !exists {
				return fmt.Sprintf("%s.%s missing in Go", path, key)
			}
			if difference := firstTelegramDifference(actual, expected, path+"."+key); difference != "" {
				return difference
			}
		}
		return ""
	}
	gotList, gok := got.([]any)
	wantList, wok := want.([]any)
	if gok && wok {
		if len(gotList) != len(wantList) {
			return fmt.Sprintf("%s length Go=%d Python=%d", path, len(gotList), len(wantList))
		}
		for index, expected := range wantList {
			if difference := firstTelegramDifference(gotList[index], expected, fmt.Sprintf("%s[%d]", path, index)); difference != "" {
				return difference
			}
		}
		return ""
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Sprintf("%s Go=%v Python=%v", path, got, want)
	}
	return ""
}

const telegramSourceFixture = `<!doctype html><div class="tgme_widget_message" data-post="sample/10"><div class="tgme_widget_message_text">Достаточно длинный текст сообщения для локального парсера.</div><time datetime="2026-10-21T18:30:00+02:00"></time></div><div class="tgme_widget_message" data-post="sample/11"><div class="tgme_widget_message_text">Второе сообщение содержит достаточно длинный текст.</div></div>`

func TestParseTelegramChannelOrderLimitAndDates(t *testing.T) {
	fallback := time.Date(2026, 10, 22, 12, 0, 0, 0, time.FixedZone("+03", 3*60*60))
	articles, err := ParseTelegramChannel(telegramSourceFixture, "https://t.me/s/sample", "Sample Channel", 2, fallback)
	if err != nil {
		t.Fatal(err)
	}
	if len(articles) != 2 || articles[0].URL != "https://t.me/sample/10" || articles[1].URL != "https://t.me/sample/11" {
		t.Fatalf("unexpected order or identity: %#v", articles)
	}
	if got := articles[0].PublishedAt.Format(time.RFC3339); got != "2026-10-21T16:30:00Z" {
		t.Fatalf("timezone not normalized to UTC: %s", got)
	}
	if !articles[1].PublishedAt.Equal(fallback.UTC()) {
		t.Fatalf("fallback time = %s, want %s", articles[1].PublishedAt, fallback.UTC())
	}
	limited, err := ParseTelegramChannel(telegramSourceFixture, "https://t.me/s/sample", "Sample Channel", 1, fallback)
	if err != nil || len(limited) != 1 {
		t.Fatalf("limit result = %d, %v", len(limited), err)
	}
}

func TestParseTelegramChannelErrorsAreDistinct(t *testing.T) {
	fallback := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	if _, err := ParseTelegramChannel("<html></html>", "https://t.me/s/sample", "Sample", 1, fallback); !errors.Is(err, ErrTelegramEmpty) {
		t.Fatalf("empty page error = %v", err)
	}
	if _, err := ParseTelegramChannel(strings.Repeat("x", telegramSourceMaxBytes+1), "https://t.me/s/sample", "Sample", 1, fallback); !errors.Is(err, ErrTelegramByteLimit) {
		t.Fatalf("over-limit error = %v", err)
	}
	if _, err := ParseTelegramChannel(telegramSourceFixture, "https://t.me/s/sample", "Sample", 0, fallback); !errors.Is(err, ErrTelegramInvalidInput) {
		t.Fatalf("invalid limit error = %v", err)
	}
	if _, err := ParseTelegramChannel(telegramSourceFixture, "https://t.me/s/sample", "Sample", 1, time.Time{}); !errors.Is(err, ErrTelegramInvalidInput) {
		t.Fatalf("missing fixed fallback time error = %v", err)
	}
	if _, err := ParseTelegramChannel(telegramSourceFixture, "file:///tmp/channel", "Sample", 1, fallback); !errors.Is(err, ErrTelegramInvalidInput) {
		t.Fatalf("invalid page URL error = %v", err)
	}
	tooLong := `<div class="tgme_widget_message" data-post="sample/12"><div class="tgme_widget_message_text">` + strings.Repeat("x", telegramSourceMaxChars+1) + `</div></div>`
	if _, err := ParseTelegramChannel(tooLong, "https://t.me/s/sample", "Sample", 1, fallback); !errors.Is(err, ErrTelegramTextLimit) {
		t.Fatalf("over-limit post error = %v", err)
	}
}

func TestParseTelegramChannelReturnsValidPrefixOnOversizedPost(t *testing.T) {
	fallback := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
	valid := `<div class="tgme_widget_message" data-post="sample/20"><div class="tgme_widget_message_text">A valid post before oversized input must be returned.</div></div>`
	oversized := `<div class="tgme_widget_message" data-post="sample/21"><div class="tgme_widget_message_text">` + strings.Repeat("x", telegramSourceMaxChars+1) + `</div></div>`
	later := `<div class="tgme_widget_message" data-post="sample/22"><div class="tgme_widget_message_text">This later post must not be processed after a bounded error.</div></div>`
	articles, err := ParseTelegramChannel(valid+oversized+later, "https://t.me/s/sample", "Sample", 3, fallback)
	if !errors.Is(err, ErrTelegramTextLimit) {
		t.Fatalf("oversized post error = %v", err)
	}
	if len(articles) != 1 || articles[0].URL != "https://t.me/sample/20" {
		t.Fatalf("returned prefix = %#v, want only valid first post", articles)
	}
}
