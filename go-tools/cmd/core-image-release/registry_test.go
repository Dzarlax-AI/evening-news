package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/corerelease"
	"strings"
	"testing"
)

func TestRegistryReconcile(t *testing.T) {
	id := "sha256:" + strings.Repeat("c", 64)
	want := corerelease.CatalogImage{Repository: "dzarlax-ai/rss-web", Platform: "linux/amd64", ImageID: id}
	tag := "ghcr.io/" + want.Repository + ":sha-" + strings.Repeat("d", 40)
	hash := func(s string) string { return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(s))) }
	for _, mode := range []string{"single", "index", "wrong_config", "ambiguous", "changed", "registry_error", "bad_digest"} {
		t.Run(mode, func(t *testing.T) {
			config := id
			if mode == "wrong_config" {
				config = "sha256:" + strings.Repeat("a", 64)
			}
			single := `{"schemaVersion":2,"config":{"digest":"` + config + `"}}`
			child := hash(single)
			raw := single
			if mode == "index" || mode == "ambiguous" {
				n := 1
				if mode == "ambiguous" {
					n = 2
				}
				entries := []map[string]any{}
				for i := 0; i < n; i++ {
					entries = append(entries, map[string]any{"digest": child, "platform": map[string]string{"os": "linux", "architecture": "amd64"}})
				}
				b, _ := json.Marshal(map[string]any{"schemaVersion": 2, "manifests": entries})
				raw = string(b)
			}
			digest := hash(raw)
			if mode == "bad_digest" {
				digest = "sha256:" + strings.Repeat("a", 64)
			}
			r := registry{run: func(a ...string) (string, error) {
				ref := a[len(a)-1]
				if len(a) > 3 && a[3] == "--raw" {
					if mode == "registry_error" {
						return "sensitive", errors.New("error")
					}
					if strings.HasSuffix(ref, child) && raw != single {
						return single, nil
					}
					return raw, nil
				}
				d := digest
				if mode == "changed" {
					d = "sha256:" + strings.Repeat("9", 64)
				}
				return "Name: example\nDigest:    " + d + "\n", nil
			}}
			err := r.reconcile(tag, digest, want)
			if (mode == "single" || mode == "index") != (err == nil) {
				t.Fatalf("%s: %v", mode, err)
			}
		})
	}
}
func TestRegistryLookupFailsClosed(t *testing.T) {
	r := registry{run: func(...string) (string, error) { return "permission denied", errors.New("secret") }}
	if _, _, err := r.lookup("tag"); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal(err)
	}
}
