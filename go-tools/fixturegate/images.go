// Package fixturegate holds reusable checks for the full image acceptance gate.
// These helpers do not issue an acceptance receipt on their own.
package fixturegate

import (
	"errors"

	"github.com/Dzarlax-AI/evening-news/go-tools/corerelease"
	"regexp"
	"strings"
)

type Image struct{ ID, Revision string }
type Runner func(args ...string) (string, error)

var imageID = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var revision = regexp.MustCompile(`^[0-9a-f]{40}$`)
var targets = []string{"web", "background", "executor"}

// CaptureImages snapshots all three existing image IDs and their source revision.
func CaptureImages(refs map[string]string, run Runner) (map[string]Image, error) {
	out := map[string]Image{}
	distinct := map[string]bool{}
	common := ""
	for _, target := range targets {
		ref := refs[target]
		if ref == "" {
			return nil, errors.New("fixture_image_ref_missing")
		}
		v, e := run("image", "inspect", "--format", `{{.Id}} {{index .Config.Labels "org.opencontainers.image.revision"}}`, ref)
		f := strings.Fields(v)
		if e != nil || len(f) != 2 || !imageID.MatchString(f[0]) || !revision.MatchString(f[1]) {
			return nil, errors.New("fixture_image_inspect_failed")
		}
		if common != "" && common != f[1] {
			return nil, errors.New("fixture_image_revision_mismatch")
		}
		common = f[1]
		if distinct[f[0]] {
			return nil, errors.New("fixture_image_ids_not_distinct")
		}
		distinct[f[0]] = true
		out[target] = Image{f[0], f[1]}
	}
	return out, nil
}

// VerifyImages detects tag replacement during acceptance, before any release.
func VerifyImages(refs map[string]string, initial map[string]Image, run Runner) error {
	final, e := CaptureImages(refs, run)
	if e != nil {
		return e
	}
	for _, target := range targets {
		if final[target] != initial[target] {
			return errors.New("fixture_image_changed")
		}
	}
	return nil
}

// CaptureCatalog checks all components against a validated complete catalog.
func CaptureCatalog(refs map[string]string, catalog corerelease.Catalog, run Runner) (map[string]Image, error) {
	if err := catalog.Validate(); err != nil {
		return nil, err
	}
	out := make(map[string]Image, len(targets))
	for _, target := range targets {
		if refs[target] == "" {
			return nil, errors.New("fixture_image_ref_missing")
		}
		v, err := run("image", "inspect", "--format", `{{.Id}} {{.Os}}/{{.Architecture}} {{index .Config.Labels "org.opencontainers.image.revision"}}`, refs[target])
		f := strings.Fields(v)
		want := catalog.Images[target]
		if err != nil || len(f) != 3 || f[0] != want.ImageID || f[1] != want.Platform || f[2] != want.Revision {
			return nil, errors.New("fixture_catalog_image_mismatch")
		}
		out[target] = Image{f[0], f[2]}
	}
	return out, nil
}
func VerifyCatalog(refs map[string]string, initial map[string]Image, catalog corerelease.Catalog, run Runner) error {
	final, err := CaptureCatalog(refs, catalog, run)
	if err != nil {
		return err
	}
	for _, target := range targets {
		if final[target] != initial[target] {
			return errors.New("fixture_image_changed")
		}
	}
	return nil
}
