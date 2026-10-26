package processing

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

type mediaGoldenCorpus struct {
	Version int `json:"version"`
	Cases   []struct {
		Case     string           `json:"case"`
		Content  string           `json:"content"`
		BaseURL  string           `json:"base_url"`
		Expected []map[string]any `json:"expected"`
	} `json:"cases"`
}

func TestMediaExtractorMatchesFrozenPythonCorpus(t *testing.T) {
	path := "../../../tests/fixtures/media-extraction-v1/corpus.json"
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var corpus mediaGoldenCorpus
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Version != 1 || len(corpus.Cases) == 0 {
		t.Fatalf("invalid media corpus header: version=%d cases=%d", corpus.Version, len(corpus.Cases))
	}
	for _, fixture := range corpus.Cases {
		t.Run(fixture.Case, func(t *testing.T) {
			got := extractMediaFiles(fixture.Content, fixture.BaseURL)
			if !reflect.DeepEqual(got, fixture.Expected) {
				gotJSON, _ := json.MarshalIndent(got, "", "  ")
				wantJSON, _ := json.MarshalIndent(fixture.Expected, "", "  ")
				t.Fatalf("media output differs from frozen Python reference\ngot:\n%s\nwant:\n%s", gotJSON, wantJSON)
			}
		})
	}
}

func TestMediaExtractorEmptyContentReturnsEmptyList(t *testing.T) {
	got := extractMediaFiles("", "https://publisher.synthetic.test/story")
	if len(got) != 0 {
		t.Fatalf("empty content produced media: %#v", got)
	}
}
