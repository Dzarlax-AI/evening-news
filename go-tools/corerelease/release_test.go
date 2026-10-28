package corerelease

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublishChecksAllBeforePush(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "third_image_mismatch"}[bad], func(t *testing.T) {
			dir := t.TempDir()
			revision := strings.Repeat("a", 40)
			id := "sha256:" + strings.Repeat("b", 64)
			receipt := Receipt{Result: "passed", IDs: map[string]string{}, Revisions: map[string]string{}, Tags: map[string]string{}}
			for _, im := range Images {
				receipt.IDs[im.Target] = id
				receipt.Revisions[im.Target] = revision
				receipt.Tags[im.Target] = im.Local
			}
			data, _ := json.Marshal(receipt)
			if e := os.WriteFile(filepath.Join(dir, "receipt.json"), data, 0600); e != nil {
				t.Fatal(e)
			}
			calls := []string{}
			run := func(args ...string) (string, error) {
				calls = append(calls, args[0])
				switch args[0] {
				case "image":
					if bad && args[len(args)-1] == Images[2].Local {
						return id + " linux/arm64 " + revision, nil
					}
					return id + " linux/amd64 " + revision, nil
				case "tag":
					return "", nil
				case "push":
					return "digest: sha256:" + strings.Repeat("c", 64), nil
				}
				return "", errors.New("unexpected")
			}
			output := filepath.Join(dir, "result", "manifest.json")
			manifest, e := Publish(revision, "linux/amd64", output, dir, run)
			if bad {
				if e == nil || e.Error() != "executor_platform_mismatch" {
					t.Fatal(e)
				}
				if strings.Join(calls, ",") != "image,image,image" {
					t.Fatalf("mutated before validation: %v", calls)
				}
				return
			}
			if e != nil || len(manifest.Images) != 3 {
				t.Fatalf("manifest=%v error=%v", manifest, e)
			}
			if strings.Join(calls, ",") != "image,image,image,tag,push,tag,push,tag,push" {
				t.Fatal(calls)
			}
			if _, e = os.Stat(output); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestMissingAcceptanceDoesNotPublish(t *testing.T) {
	called := false
	_, e := Publish(strings.Repeat("a", 40), "linux/amd64", filepath.Join(t.TempDir(), "out"), t.TempDir(), func(...string) (string, error) { called = true; return "", nil })
	if e == nil || called {
		t.Fatalf("error=%v called=%v", e, called)
	}
}
