// Package fixturegate holds reusable checks for the full image acceptance gate.
// These helpers do not issue an acceptance receipt on their own.
package fixturegate

import (
	"errors"
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
