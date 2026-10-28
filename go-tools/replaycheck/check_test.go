package replaycheck

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestFrozenCorpusGoldens(t *testing.T) {
	cases, err := loadCorpus("../../tests/fixtures/go-parity/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = loadGoldens("../../docs/extraction-golden-cases.json", cases); err != nil {
		t.Fatal(err)
	}
}

func TestReplayGate(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, v any) string {
		t.Helper()
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, name)
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	c := obj{"id": "fixture", "html": "<p>safe</p>"}
	digest, err := hash(c)
	if err != nil {
		t.Fatal(err)
	}
	corpus := write("corpus", obj{"version": 1, "cases": []any{c}})
	golden := write("golden", obj{"version": 1, "cases": []any{obj{"id": "fixture", "case_sha256": digest, "evidence": "synthetic", "expected": obj{"state": "extracted", "contains": []any{"safe text"}, "not_contains": []any{"hidden"}, "fields": obj{"title": "title"}, "media_contains": []any{obj{"type": "image"}}}}}})
	record := obj{"case_id": "fixture", "cycle": 1, "passed": true, "failures": []any{}, "result": obj{"state": "extracted", "content": "safe\ntext", "title": "title", "media": []any{obj{"type": "image", "url": "fixture"}}}}
	replay := write("replay", record)
	report, err := Check(replay, corpus, golden, 1)
	if err != nil || report.Gate != "passed" {
		t.Fatalf("valid replay: %+v %v", report, err)
	}
	record["result"].(obj)["content"] = "hidden"
	replay = write("replay", record)
	report, err = Check(replay, corpus, golden, 1)
	if err != nil || report.Gate != "requires_review" || len(report.Failures) != 1 {
		t.Fatalf("changed content: %+v %v", report, err)
	}
	if _, err = Check(replay, corpus, golden, 2); err == nil {
		t.Fatal("missing cycle accepted")
	}
	bad := write("badgolden", obj{"version": 1, "cases": []any{obj{"id": "fixture", "case_sha256": "incorrect", "evidence": "synthetic", "expected": obj{"state": "failed"}}}})
	if _, err = Check(replay, corpus, bad, 1); err == nil {
		t.Fatal("altered fingerprint accepted")
	}
	if _, err = decode([]byte(`{} trailing`)); err == nil {
		t.Fatal("malformed trailing JSON accepted")
	}
}

func TestGoldenProjectionAndIndependentOutcomes(t *testing.T) {
	expected := obj{"state": "extracted", "contains": []any{"first second"}, "not_contains": []any{"private"}, "fields": obj{"metadata": obj{"source": "one", "nested": obj{"verified": true}}}, "media_contains": []any{obj{"type": "image", "id": "stable"}}}
	makeResult := func() obj {
		return obj{"state": "extracted", "content": "first\n second", "metadata": obj{"source": "one", "nested": obj{"verified": true, "extra": 1}}, "media": []any{obj{"type": "image", "id": "stable", "url": "synthetic"}}}
	}
	if !matches(expected, makeResult()) {
		t.Fatal("expected projection failed")
	}
	for _, mutate := range []func(obj){
		func(r obj) { r["state"] = "failed" },
		func(r obj) { r["content"] = "first second private" },
		func(r obj) { r["content"] = "first" },
		func(r obj) { r["metadata"].(obj)["nested"] = obj{"verified": false} },
		func(r obj) { r["media"] = []any{} },
		func(r obj) { r["media"] = []any{obj{"type": "image", "id": "other"}} },
	} {
		r := makeResult()
		mutate(r)
		if matches(expected, r) {
			t.Fatal("incorrect result accepted")
		}
	}
	if !matches(obj{"state": "resource_limit"}, obj{"state": "resource_limit"}) {
		t.Fatal("resource limit outcome rejected")
	}
	if matches(obj{"state": "failed"}, obj{"state": "resource_limit"}) {
		t.Fatal("failure states conflated")
	}
}
