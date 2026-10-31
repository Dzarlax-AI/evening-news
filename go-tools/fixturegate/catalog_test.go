package fixturegate

import (
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/corerelease"
	"strings"
	"testing"
)

func TestCatalogCaptureMixedAndReplacement(t *testing.T) {
	c := corerelease.Catalog{Version: 2, Revision: strings.Repeat("a", 40), BaselineRevision: strings.Repeat("b", 40), Platform: "linux/amd64", Images: map[string]corerelease.CatalogImage{}}
	refs := map[string]string{}
	for i, im := range corerelease.Images {
		r := strings.Repeat("b", 40)
		origin := "reused"
		digest := "sha256:" + strings.Repeat("e", 64)
		if i == 0 {
			r = c.Revision
			origin = "built"
			digest = ""
		}
		c.Images[im.Target] = corerelease.CatalogImage{Repository: im.Repository, Revision: r, Platform: c.Platform, ImageID: "sha256:" + strings.Repeat(fmt.Sprint(i+1), 64), Digest: digest, SourceFingerprint: strings.Repeat("f", 64), Origin: origin}
		refs[im.Target] = im.Target
	}
	changed := false
	run := func(args ...string) (string, error) {
		v := c.Images[args[len(args)-1]]
		if changed {
			return "invalid", nil
		}
		return v.ImageID + " " + v.Platform + " " + v.Revision, nil
	}
	images, err := CaptureCatalog(refs, c, run)
	if err != nil {
		t.Fatal(err)
	}
	if err = WriteCatalogReceipt(t.TempDir(), images, c); err != nil {
		t.Fatal(err)
	}
	changed = true
	if err = VerifyCatalog(refs, images, c, run); err == nil {
		t.Fatal("replacement accepted")
	}
}
