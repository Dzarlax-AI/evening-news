package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecordIsExclusiveAndContainsDigest(t *testing.T) {
	d := t.TempDir()
	archive := filepath.Join(d, "image.tar")
	output := filepath.Join(d, "metadata.json")
	if err := os.WriteFile(archive, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	id := "sha256:" + strings.Repeat("1", 64)
	sha := strings.Repeat("a", 40)
	if err := record("fixture", id, sha, archive, output); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil || !strings.Contains(string(data), "f16d05ec6b29248d2c61adb1e9263f78e4f7bace1b955014a2d17872cfe4064d") {
		t.Fatal("digest missing")
	}
	if err = record("fixture", id, sha, archive, output); err == nil {
		t.Fatal("overwrote metadata")
	}
}
