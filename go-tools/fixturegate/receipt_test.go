package fixturegate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReceiptRequiresDistinctImagesAndNeverOverwrites(t *testing.T) {
	d := t.TempDir()
	rev := strings.Repeat("a", 40)
	images := map[string]Image{"web": {"sha256:" + strings.Repeat("1", 64), rev}, "background": {"sha256:" + strings.Repeat("2", 64), rev}, "executor": {"sha256:" + strings.Repeat("3", 64), rev}}
	if err := WriteReceipt(d, images); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(d, "receipt.json")
	before, _ := os.ReadFile(path)
	if err := WriteReceipt(d, images); err == nil {
		t.Fatal("receipt overwritten")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("receipt changed")
	}
	images["executor"] = images["web"]
	if err := WriteReceipt(filepath.Join(d, "bad"), images); err == nil {
		t.Fatal("duplicate image accepted")
	}
}
