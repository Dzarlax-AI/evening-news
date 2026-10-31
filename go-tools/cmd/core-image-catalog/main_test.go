package main

import (
	"encoding/json"
	"github.com/Dzarlax-AI/evening-news/go-tools/corerelease"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareAndNormalize(t *testing.T) {
	dir := t.TempDir()
	old := strings.Repeat("a", 40)
	head := strings.Repeat("b", 40)
	fp := strings.Repeat("c", 64)
	id := "sha256:" + strings.Repeat("d", 64)
	digest := "sha256:" + strings.Repeat("e", 64)
	legacy := map[string]any{"schemaVersion": 1, "revision": old, "platform": "linux/amd64", "images": map[string]any{}}
	imgs := legacy["images"].(map[string]any)
	for _, im := range corerelease.Images {
		imgs[im.Target] = map[string]string{"repository": im.Repository, "revision": old, "platform": "linux/amd64", "imageID": digest, "digest": digest}
	}
	write := func(name string, v any) string {
		p := filepath.Join(dir, name)
		b, _ := json.Marshal(v)
		if err := os.WriteFile(p, b, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	base := write("old", legacy)
	plan := corerelease.Impact{Version: 1, Base: old, Head: old, Targets: []string{}, Fingerprints: map[string]string{}}
	for _, im := range corerelease.Images {
		plan.Fingerprints[im.Target] = fp
	}
	orig := inspect
	defer func() { inspect = orig }()
	inspect = func(ref string) (string, string, string, error) {
		for i, im := range corerelease.Images {
			if strings.Contains(ref, im.Repository) {
				return "sha256:" + strings.Repeat(string(rune('1'+i)), 64), "linux/amd64", old, nil
			}
		}
		return id, "linux/amd64", head, nil
	}
	normalized := filepath.Join(dir, "normalized")
	if e := run([]string{"normalize-v1", "--baseline", base, "--impact-plan", write("plan0", plan), "--output", normalized}); e != nil {
		t.Fatal(e)
	}
	c, e := corerelease.ReadCatalog(normalized)
	if e != nil || c.Images["web"].ImageID == digest {
		t.Fatalf("%v %v", c, e)
	}
	// A v1 identity unrelated to both registry digest and inspected config is rejected.
	imgs["web"].(map[string]string)["imageID"] = "sha256:" + strings.Repeat("f", 64)
	badbase := write("bad-old", legacy)
	if e = run([]string{"normalize-v1", "--baseline", badbase, "--impact-plan", write("plan-bad", plan), "--output", filepath.Join(dir, "bad")}); e == nil {
		t.Fatal("arbitrary legacy identity accepted")
	}
	plan.Head = head
	plan.Targets = []string{"web"}
	inspect = func(ref string) (string, string, string, error) {
		for _, im := range corerelease.Images {
			if im.Local == ref {
				v := c.Images[im.Target]
				if im.Target == "web" {
					return id, "linux/amd64", head, nil
				}
				return v.ImageID, v.Platform, v.Revision, nil
			}
		}
		t.Fatal("unexpected reference")
		return "", "", "", nil
	}
	if e = run([]string{"prepare", "--baseline", normalized, "--impact-plan", write("plan1", plan), "--output", filepath.Join(dir, "candidate")}); e != nil {
		t.Fatal(e)
	}
}
