package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-tools/corerelease"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "core image catalog failed ("+err.Error()+")")
		os.Exit(1)
	}
}
func read(path string, v any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}

var inspect = inspectDocker

func inspectDocker(ref string) (string, string, string, error) {
	out, e := corerelease.Docker("image", "inspect", `--format={{.Id}} {{.Os}}/{{.Architecture}} {{index .Config.Labels "org.opencontainers.image.revision"}}`, ref)
	f := strings.Fields(out)
	if e != nil || len(f) != 3 {
		return "", "", "", errors.New("image_inspect_failed")
	}
	return f[0], f[1], f[2], nil
}
func run(args []string) error {
	if len(args) == 0 {
		return errors.New("command_required")
	}
	cmd := args[0]
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	baselinePath := f.String("baseline", "", "")
	impactPath := f.String("impact-plan", "", "")
	output := f.String("output", "", "")
	if e := f.Parse(args[1:]); e != nil {
		return e
	}
	if *baselinePath == "" || *impactPath == "" || *output == "" || f.NArg() != 0 {
		return errors.New("arguments_required")
	}
	var plan corerelease.Impact
	if e := read(*impactPath, &plan); e != nil {
		return e
	}
	var c corerelease.Catalog
	switch cmd {
	case "prepare":
		baseline, e := corerelease.ReadCatalog(*baselinePath)
		if e != nil {
			return e
		}
		c = corerelease.Catalog{Version: 2, Revision: plan.Head, Platform: baseline.Platform, BaselineRevision: baseline.Revision, Images: map[string]corerelease.CatalogImage{}}
		selected := map[string]bool{}
		for _, t := range plan.Targets {
			if selected[t] {
				return errors.New("duplicate_target")
			}
			selected[t] = true
		}
		for _, im := range corerelease.Images {
			old := baseline.Images[im.Target]
			id, platform, rev, e := inspect(im.Local)
			if e != nil {
				return e
			}
			v := old
			if selected[im.Target] {
				v = corerelease.CatalogImage{Repository: im.Repository, Revision: rev, Platform: platform, ImageID: id, SourceFingerprint: plan.Fingerprints[im.Target], Origin: "built"}
			} else {
				if id != old.ImageID || platform != old.Platform || rev != old.Revision {
					return errors.New("reused_identity_mismatch")
				}
				v.Origin = "reused"
			}
			c.Images[im.Target] = v
		}
		if e = corerelease.ValidateInputs(c, baseline, plan); e != nil {
			return e
		}
	case "normalize-v1":
		var old struct {
			Version  int    `json:"schemaVersion"`
			Revision string `json:"revision"`
			Platform string `json:"platform"`
			Images   map[string]struct {
				Repository string `json:"repository"`
				Revision   string `json:"revision"`
				Platform   string `json:"platform"`
				ImageID    string `json:"imageID"`
				Digest     string `json:"digest"`
			} `json:"images"`
		}
		if e := read(*baselinePath, &old); e != nil {
			return e
		}
		if old.Version != 1 || len(old.Images) != 3 || plan.Version != 1 || plan.Base != old.Revision || plan.Head != old.Revision || len(plan.Targets) != 0 || corerelease.ValidateFingerprints(plan.Fingerprints) != nil {
			return errors.New("v1_baseline_invalid")
		}
		c = corerelease.Catalog{Version: 2, Revision: old.Revision, Platform: old.Platform, BaselineRevision: old.Revision, Images: map[string]corerelease.CatalogImage{}}
		for _, im := range corerelease.Images {
			v, ok := old.Images[im.Target]
			if !ok || v.Repository != im.Repository || v.Revision != old.Revision || v.Platform != old.Platform || !strings.HasPrefix(v.Digest, "sha256:") {
				return errors.New("v1_image_invalid")
			}
			id, platform, rev, e := inspect("ghcr.io/" + im.Repository + "@" + v.Digest)
			if e != nil {
				return e
			}
			if platform != v.Platform || rev != v.Revision || (v.ImageID != v.Digest && v.ImageID != id) {
				return errors.New("v1_image_identity_mismatch")
			}
			c.Images[im.Target] = corerelease.CatalogImage{Repository: v.Repository, Revision: rev, Platform: platform, ImageID: id, Digest: v.Digest, SourceFingerprint: plan.Fingerprints[im.Target], Origin: "reused"}
		}
		if e := c.Validate(); e != nil {
			return e
		}
	default:
		return errors.New("unknown_command")
	}
	file, e := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if e != nil {
		return e
	}
	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	e = enc.Encode(c)
	closeErr := file.Close()
	if e != nil {
		_ = os.Remove(*output)
		return e
	}
	return closeErr
}
