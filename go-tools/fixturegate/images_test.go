package fixturegate

import (
	"strings"
	"testing"
)

func TestImageIdentityAndRevision(t *testing.T) {
	refs := map[string]string{"web": "web", "background": "background", "executor": "executor"}
	rev := strings.Repeat("a", 40)
	ids := map[string]string{"web": "sha256:" + strings.Repeat("1", 64), "background": "sha256:" + strings.Repeat("2", 64), "executor": "sha256:" + strings.Repeat("3", 64)}
	run := func(args ...string) (string, error) { return ids[args[len(args)-1]] + " " + rev, nil }
	snapshot, e := CaptureImages(refs, run)
	if e != nil {
		t.Fatal(e)
	}
	if e = VerifyImages(refs, snapshot, run); e != nil {
		t.Fatal(e)
	}
	ids["executor"] = "sha256:" + strings.Repeat("4", 64)
	if e = VerifyImages(refs, snapshot, run); e == nil {
		t.Fatal("tag replacement accepted")
	}
	ids["executor"] = ids["web"]
	if _, e = CaptureImages(refs, run); e == nil {
		t.Fatal("duplicate image accepted")
	}
}
