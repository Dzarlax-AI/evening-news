package corerelease

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func selectiveFixture(t *testing.T) SelectiveOptions {
	t.Helper()
	old := strings.Repeat("a", 40)
	head := strings.Repeat("b", 40)
	fp := strings.Repeat("c", 64)
	baseline := Catalog{Version: 2, Revision: old, Platform: "linux/amd64", BaselineRevision: strings.Repeat("d", 40), Images: map[string]CatalogImage{}}
	candidate := Catalog{Version: 2, Revision: head, Platform: "linux/amd64", BaselineRevision: old, Images: map[string]CatalogImage{}}
	for i, im := range Images {
		v := CatalogImage{Repository: im.Repository, Revision: old, Platform: "linux/amd64", ImageID: "sha256:" + strings.Repeat(fmt.Sprint(i+1), 64), Digest: "sha256:" + strings.Repeat("e", 64), SourceFingerprint: fp, Origin: "reused"}
		baseline.Images[im.Target] = v
		candidate.Images[im.Target] = v
	}
	v := candidate.Images["web"]
	v.Origin = "built"
	v.Revision = head
	v.Digest = ""
	v.ImageID = "sha256:" + strings.Repeat("f", 64)
	candidate.Images["web"] = v
	impact := Impact{Version: 1, Base: old, Head: head, Targets: []string{"web"}, Fingerprints: map[string]string{"web": fp, "background": fp, "executor": fp}}
	receipt := ReceiptV2{Version: 2, Result: "passed", Catalog: candidate, Targets: []string{"web"}, Tags: map[string]string{}}
	for _, im := range Images {
		receipt.Tags[im.Target] = im.Local
	}
	run := func(args ...string) (string, error) {
		switch args[0] {
		case "image":
			for _, im := range Images {
				if args[len(args)-1] == im.Local {
					v := candidate.Images[im.Target]
					return v.ImageID + " " + v.Platform + " " + v.Revision, nil
				}
			}
		case "tag":
			return "", nil
		case "push":
			return "digest: sha256:" + strings.Repeat("f", 64), nil
		}
		return "", errors.New("unexpected")
	}
	return SelectiveOptions{Catalog: candidate, ExpectedBaseline: baseline, Impact: impact, Receipt: receipt, Baseline: func() (Catalog, error) { return baseline, nil }, Lookup: func(string) (string, bool, error) { return "", false, nil }, Run: run, Output: filepath.Join(t.TempDir(), "manifest.json")}
}
func TestSelectiveMixedAndRefusals(t *testing.T) {
	for _, name := range []string{"success", "missing_baseline", "wrong_revision", "receipt_tamper", "unknown_schema", "tag_replaced", "stale_baseline", "existing_tag", "lookup_error", "push_error"} {
		t.Run(name, func(t *testing.T) {
			o := selectiveFixture(t)
			calls := []string{}
			run := o.Run
			o.Run = func(a ...string) (string, error) {
				calls = append(calls, a[0])
				if name == "tag_replaced" && a[0] == "image" {
					return "bad", nil
				}
				if name == "push_error" && a[0] == "push" {
					return "", errors.New("failure")
				}
				return run(a...)
			}
			switch name {
			case "missing_baseline":
				o.Baseline = func() (Catalog, error) { return Catalog{}, errors.New("missing") }
			case "wrong_revision":
				o.Impact.Head = strings.Repeat("9", 40)
			case "receipt_tamper":
				o.Receipt.Targets = []string{"executor"}
			case "unknown_schema":
				o.Catalog.Version = 3
			case "stale_baseline":
				o.ExpectedBaseline.Revision = strings.Repeat("9", 40)
			case "existing_tag":
				o.Lookup = func(string) (string, bool, error) { return "", true, nil }
			case "lookup_error":
				o.Lookup = func(string) (string, bool, error) { return "", false, errors.New("denied") }
			}
			result, e := PublishSelective(o)
			if name == "success" {
				if e != nil {
					t.Fatal(e)
				}
				if result.Images["background"].Digest != o.ExpectedBaseline.Images["background"].Digest || strings.Join(calls, ",") != "image,image,image,tag,push" {
					t.Fatalf("result %v calls %v", result, calls)
				}
			} else if e == nil {
				t.Fatal("expected failure")
			} else if name != "push_error" && (strings.Contains(strings.Join(calls, ","), "push") || strings.Contains(strings.Join(calls, ","), "tag")) {
				t.Fatalf("unexpected mutation %v", calls)
			}
		})
	}
}
func TestStaleAfterPushDoesNotWriteManifest(t *testing.T) {
	o := selectiveFixture(t)
	n := 0
	old := o.ExpectedBaseline
	o.Baseline = func() (Catalog, error) {
		n++
		if n == 3 {
			old.Revision = strings.Repeat("9", 40)
		}
		return old, nil
	}
	if _, err := PublishSelective(o); err == nil || err.Error() != "baseline_stale" {
		t.Fatal(err)
	}
}

func TestReconcileRequiresProof(t *testing.T) {
	for _, proof := range []bool{false, true} {
		t.Run(fmt.Sprint(proof), func(t *testing.T) {
			o := selectiveFixture(t)
			digest := "sha256:" + strings.Repeat("9", 64)
			o.Lookup = func(string) (string, bool, error) { return digest, true, nil }
			calls := []string{}
			run := o.Run
			o.Run = func(a ...string) (string, error) { calls = append(calls, a[0]); return run(a...) }
			if proof {
				o.Reconcile = func(_ string, d string, expected CatalogImage) error {
					if d != digest || expected.ImageID != o.Catalog.Images["web"].ImageID {
						return errors.New("bad proof")
					}
					return nil
				}
			}
			result, err := PublishSelective(o)
			if !proof && err == nil {
				t.Fatal("missing proof accepted")
			}
			if proof && (err != nil || result.Images["web"].Digest != digest || strings.Join(calls, ",") != "image,image,image") {
				t.Fatalf("result=%v err=%v calls=%v", result, err, calls)
			}
		})
	}
}

func TestNoopAndPreflightAllTags(t *testing.T) {
	t.Run("noop", func(t *testing.T) {
		o := selectiveFixture(t)
		for _, im := range Images {
			v := o.ExpectedBaseline.Images[im.Target]
			v.Origin = "reused"
			o.Catalog.Images[im.Target] = v
		}
		o.Impact.Targets = []string{}
		o.Receipt.Catalog = o.Catalog
		o.Receipt.Targets = []string{}
		o.Lookup = func(string) (string, bool, error) { t.Fatal("noop lookup"); return "", false, nil }
		run := o.Run
		o.Run = func(a ...string) (string, error) {
			if a[0] != "image" {
				t.Fatal("noop mutated")
			}
			for _, im := range Images {
				if im.Local == a[len(a)-1] {
					v := o.Catalog.Images[im.Target]
					return v.ImageID + " " + v.Platform + " " + v.Revision, nil
				}
			}
			return run(a...)
		}
		if _, err := PublishSelective(o); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("second tag exists", func(t *testing.T) {
		o := selectiveFixture(t)
		v := o.Catalog.Images["background"]
		v.Origin = "built"
		v.Revision = o.Catalog.Revision
		v.Digest = ""
		o.Catalog.Images["background"] = v
		o.Impact.Targets = []string{"background", "web"}
		o.Receipt.Catalog = o.Catalog
		o.Receipt.Targets = []string{"background", "web"}
		n := 0
		o.Lookup = func(string) (string, bool, error) { n++; return "", n == 2, nil }
		o.Run = func(a ...string) (string, error) {
			if a[0] != "image" {
				t.Fatal("mutation before preflight completed")
			}
			for _, im := range Images {
				if im.Local == a[len(a)-1] {
					v := o.Catalog.Images[im.Target]
					return v.ImageID + " " + v.Platform + " " + v.Revision, nil
				}
			}
			return "", errors.New("unknown")
		}
		if _, err := PublishSelective(o); err == nil || n != 2 {
			t.Fatalf("%v %d", err, n)
		}
	})
}

func TestImpactFingerprintsIncludeExtractor(t *testing.T) {
	o := selectiveFixture(t)
	o.Impact.Fingerprints["extractor"] = strings.Repeat("a", 64)
	if e := ValidateInputs(o.Catalog, o.ExpectedBaseline, o.Impact); e != nil {
		t.Fatal(e)
	}
	o.Impact.Fingerprints["unknown"] = strings.Repeat("a", 64)
	if e := ValidateInputs(o.Catalog, o.ExpectedBaseline, o.Impact); e == nil {
		t.Fatal("unknown fingerprint accepted")
	}
	delete(o.Impact.Fingerprints, "unknown")
	o.Impact.Fingerprints["extractor"] = "bad"
	if e := ValidateInputs(o.Catalog, o.ExpectedBaseline, o.Impact); e == nil {
		t.Fatal("invalid extractor fingerprint accepted")
	}
}
