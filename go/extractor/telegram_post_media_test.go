package extractor

import (
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func TestTelegramPostMediaPythonGolden(t *testing.T) {
	raw, e := os.ReadFile("testdata/python-post-media.json")
	if e != nil {
		t.Fatal(e)
	}
	var cases []struct {
		Name, HTML string
		Expected   []map[string]any
	}
	if e = json.Unmarshal(raw, &cases); e != nil {
		t.Fatal(e)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			got, found, err := ParseTelegramPostMedia(tc.HTML, "https://t.me/s/fixture/1", "fixture/1")
			if err == nil && found {
				again, _, e := ParseTelegramPostMedia(tc.HTML, "https://t.me/s/fixture/1", "fixture/1")
				if e != nil || !reflect.DeepEqual(got, again) {
					t.Fatal("media output is not deterministic")
				}
				for _, rows := range [][]map[string]any{got, tc.Expected} {
					for _, item := range rows {
						kind, _ := item["type"].(string)
						value, _ := item["url"].(string)
						if (kind == "poll" || kind == "location" || kind == "contact") && strings.HasPrefix(value, "#"+kind+"_") {
							item["url"] = "#" + kind + "_volatile"
						}
					}
				}
			}
			wire, _ := json.Marshal(got)
			var normalized []map[string]any
			_ = json.Unmarshal(wire, &normalized)
			if err != nil || !found || !reflect.DeepEqual(normalized, tc.Expected) {
				t.Fatalf("got=%v expected=%v found=%v err=%v", got, tc.Expected, found, err)
			}
		})
	}
	_, found, e := ParseTelegramPostMedia(`<div data-post="fixture/2"></div>`, "https://t.me/s/fixture/1", "fixture/1")
	if e != nil || found {
		t.Fatalf("missing post found=%v err=%v", found, e)
	}
}

func TestPostMediaStableIdentityAcrossProcesses(t *testing.T) {
	var results []string
	for i := 0; i < 2; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestPostMediaIdentityHelper$")
		cmd.Env = append(os.Environ(), "NEWS_MEDIA_ID_HELPER=1")
		out, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, string(out))
	}
	if results[0] == "" || results[0] != results[1] {
		t.Fatalf("unstable identifiers %q %q", results[0], results[1])
	}
}
func TestPostMediaIdentityHelper(t *testing.T) {
	if os.Getenv("NEWS_MEDIA_ID_HELPER") != "1" {
		return
	}
	html := `<div data-post="fixture/1"><div class="tgme_widget_message_poll"><div class="poll_question">Choose?</div><div class="poll_option">Yes</div></div><div class="tgme_widget_message_contact">Contact</div><div class="tgme_widget_message_location">Place</div></div>`
	items, found, err := ParseTelegramPostMedia(html, "https://t.me/s/fixture/1", "fixture/1")
	if err != nil || !found {
		os.Exit(2)
	}
	raw, err := json.Marshal(items)
	if err != nil {
		os.Exit(2)
	}
	_, _ = os.Stdout.Write(raw)
	os.Exit(0)
}
