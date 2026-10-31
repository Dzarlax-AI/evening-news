package main

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-tools/corerelease"
)

var manifestDigest = regexp.MustCompile(`(?m)^Digest:[ \t]*(sha256:[0-9a-f]{64})[ \t]*\r?$`)
var shaDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type registry struct{ run corerelease.Runner }

func (r registry) lookup(tag string) (string, bool, error) {
	out, err := r.run("buildx", "imagetools", "inspect", tag)
	if err != nil {
		if missingRegistryReference(out, tag) {
			return "", false, nil
		}
		return "", false, errors.New("registry_lookup_failed")
	}
	matches := manifestDigest.FindAllStringSubmatch(out, -1)
	if len(matches) != 1 {
		return "", false, errors.New("registry_digest_invalid")
	}
	return matches[0][1], true, nil
}

// A missing tag is safe only when the output is a complete known absence
// response. A different reference or a transport error must fail closed.
func missingRegistryReference(output, tag string) bool {
	if tag == "" || strings.ContainsAny(tag, "\r\n") {
		return false
	}
	message := strings.TrimSpace(output)
	message = strings.TrimPrefix(message, "ERROR: ")
	message = strings.TrimPrefix(message, "failed to solve: ")
	if strings.ContainsAny(message, "\r\n") {
		return false
	}
	if strings.EqualFold(message, "manifest unknown") || strings.EqualFold(message, "no such manifest") {
		return true
	}
	for _, candidate := range []string{
		"reference " + tag + " not found", tag + ": not found",
		tag + ": manifest unknown", tag + ": no such manifest",
	} {
		if message == candidate {
			return true
		}
	}
	return false
}
func (r registry) reconcile(tag, digest string, want corerelease.CatalogImage) error {
	if !shaDigest.MatchString(digest) || !strings.HasPrefix(tag, "ghcr.io/"+want.Repository+":sha-") {
		return errors.New("registry_proof_invalid")
	}
	ref := "ghcr.io/" + want.Repository + "@" + digest
	type descriptor struct {
		Digest   string `json:"digest"`
		Platform struct {
			OS           string `json:"os"`
			Architecture string `json:"architecture"`
			Variant      string `json:"variant"`
		} `json:"platform"`
	}
	type manifest struct {
		SchemaVersion int          `json:"schemaVersion"`
		MediaType     string       `json:"mediaType"`
		Config        descriptor   `json:"config"`
		Manifests     []descriptor `json:"manifests"`
	}
	raw, err := r.run("buildx", "imagetools", "inspect", "--raw", ref)
	if err != nil {
		return errors.New("registry_proof_unavailable")
	}
	if fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(raw))) != digest {
		return errors.New("registry_digest_mismatch")
	}
	var m manifest
	if json.Unmarshal([]byte(raw), &m) != nil || m.SchemaVersion != 2 {
		return errors.New("registry_manifest_invalid")
	}
	if len(m.Manifests) > 0 {
		var child string
		count := 0
		for _, d := range m.Manifests {
			if d.Platform.OS == "linux" && d.Platform.Architecture == "amd64" && d.Platform.Variant == "" {
				count++
				child = d.Digest
			}
		}
		if count != 1 || !shaDigest.MatchString(child) {
			return errors.New("registry_platform_ambiguous")
		}
		raw, err = r.run("buildx", "imagetools", "inspect", "--raw", "ghcr.io/"+want.Repository+"@"+child)
		if err != nil {
			return errors.New("registry_proof_unavailable")
		}
		if fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(raw))) != child {
			return errors.New("registry_digest_mismatch")
		}
		m = manifest{}
		if json.Unmarshal([]byte(raw), &m) != nil || m.SchemaVersion != 2 || len(m.Manifests) != 0 {
			return errors.New("registry_manifest_invalid")
		}
	}
	if want.Platform != "linux/amd64" || m.Config.Digest != want.ImageID {
		return errors.New("registry_identity_mismatch")
	}
	// PublishSelective already inspected the exact accepted local config ID and
	// checked its platform and revision; the manifest config digest binds it.
	latest, exists, err := r.lookup(tag)
	if err != nil || !exists || latest != digest {
		return errors.New("registry_tag_changed")
	}
	return nil
}
