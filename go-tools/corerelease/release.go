// Package corerelease publishes only images verified by an acceptance receipt.
package corerelease

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

type Image struct{ Target, Local, Repository string }

var Images = []Image{{"web", "evening-news-web:consolidation-fixture", "dzarlax-ai/rss-web"}, {"background", "evening-news-background:consolidation-fixture", "dzarlax-ai/rss-background"}, {"executor", "evening-news-executor:consolidation-fixture", "dzarlax-ai/rss-summarizer"}}
var revisionPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var idPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var digestPattern = regexp.MustCompile(`digest:\s*(sha256:[0-9a-f]{64})\b`)

type Runner func(args ...string) (string, error)

func Docker(args ...string) (string, error) {
	out, e := exec.Command("docker", args...).CombinedOutput()
	return string(out), e
}

type Receipt struct {
	Result    string            `json:"result"`
	IDs       map[string]string `json:"image_ids"`
	Revisions map[string]string `json:"image_revisions"`
	Tags      map[string]string `json:"image_tags"`
}
type imageRecord struct {
	Repository string `json:"repository"`
	Revision   string `json:"revision"`
	Platform   string `json:"platform"`
	ImageID    string `json:"imageID"`
	Digest     string `json:"digest"`
}
type Manifest struct {
	Version  int                    `json:"schemaVersion"`
	Revision string                 `json:"revision"`
	Platform string                 `json:"platform"`
	Images   map[string]imageRecord `json:"images"`
}

func Publish(revision, platform, output, acceptanceDir string, run Runner) (Manifest, error) {
	var manifest Manifest
	fail := func(code string) (Manifest, error) { return manifest, errors.New(code) }
	if !revisionPattern.MatchString(revision) {
		return fail("revision_invalid")
	}
	if platform != "linux/amd64" {
		return fail("platform_invalid")
	}
	if _, e := os.Lstat(output); e == nil {
		return fail("manifest_already_exists")
	} else if !os.IsNotExist(e) {
		return fail("manifest_output_unavailable")
	}
	reports, e := filepath.Glob(filepath.Join(acceptanceDir, "*.json"))
	if e != nil || len(reports) != 1 {
		return fail("acceptance_report_missing_or_ambiguous")
	}
	data, e := os.ReadFile(reports[0])
	if e != nil {
		return fail("acceptance_report_invalid")
	}
	var receipt Receipt
	if json.Unmarshal(data, &receipt) != nil {
		return fail("acceptance_report_invalid")
	}
	if receipt.Result != "passed" {
		return fail("acceptance_not_passed")
	}
	if receipt.IDs == nil || receipt.Revisions == nil || receipt.Tags == nil {
		return fail("acceptance_report_invalid")
	}
	for _, image := range Images {
		if receipt.Tags[image.Target] != image.Local {
			return fail(image.Target + "_acceptance_tag_mismatch")
		}
		if receipt.Revisions[image.Target] != revision {
			return fail(image.Target + "_acceptance_revision_mismatch")
		}
		if !idPattern.MatchString(receipt.IDs[image.Target]) {
			return fail(image.Target + "_acceptance_image_id_invalid")
		}
	}
	for _, image := range Images {
		out, e := run("image", "inspect", `--format={{.Id}} {{.Os}}/{{.Architecture}} {{index .Config.Labels "org.opencontainers.image.revision"}}`, image.Local)
		fields := strings.Fields(out)
		if e != nil || len(fields) != 3 || !idPattern.MatchString(fields[0]) {
			return fail(image.Target + "_image_unavailable")
		}
		if fields[1] != platform {
			return fail(image.Target + "_platform_mismatch")
		}
		if fields[2] != revision {
			return fail(image.Target + "_revision_mismatch")
		}
		if fields[0] != receipt.IDs[image.Target] {
			return fail(image.Target + "_acceptance_image_id_mismatch")
		}
	}
	manifest = Manifest{1, revision, platform, map[string]imageRecord{}}
	for _, image := range Images {
		tag := "ghcr.io/" + image.Repository + ":sha-" + revision
		id := receipt.IDs[image.Target]
		if _, e := run("tag", id, tag); e != nil {
			return fail(image.Target + "_tag_failed")
		}
		out, e := run("push", tag)
		if e != nil {
			return fail(image.Target + "_push_failed")
		}
		found := digestPattern.FindAllStringSubmatch(out, -1)
		if len(found) == 0 {
			return fail(image.Target + "_digest_missing")
		}
		manifest.Images[image.Target] = imageRecord{image.Repository, revision, platform, id, found[len(found)-1][1]}
	}
	if e := os.MkdirAll(filepath.Dir(output), 0755); e != nil {
		return fail("manifest_write_failed")
	}
	f, e := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if e != nil {
		if os.IsExist(e) {
			return fail("manifest_already_exists")
		}
		return fail("manifest_write_failed")
	}
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	e = encoder.Encode(manifest)
	closeErr := f.Close()
	if e != nil || closeErr != nil {
		return fail("manifest_write_failed")
	}
	return manifest, nil
}
