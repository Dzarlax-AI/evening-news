package corerelease

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

var fingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Catalog identifies the complete set accepted by the integration gate. Built
// entries have no registry digest until publication; reused entries must have one.
type Catalog struct {
	Version          int                     `json:"schemaVersion"`
	Revision         string                  `json:"revision"`
	Platform         string                  `json:"platform"`
	BaselineRevision string                  `json:"baselineRevision"`
	Images           map[string]CatalogImage `json:"images"`
}
type CatalogImage struct {
	Repository        string `json:"repository"`
	Revision          string `json:"revision"`
	Platform          string `json:"platform"`
	ImageID           string `json:"imageID"`
	Digest            string `json:"digest"`
	SourceFingerprint string `json:"sourceFingerprint"`
	Origin            string `json:"origin"`
}
type Impact struct {
	Version      int               `json:"schemaVersion"`
	Base         string            `json:"base"`
	Head         string            `json:"head"`
	Targets      []string          `json:"targets"`
	Fingerprints map[string]string `json:"source_fingerprints"`
}
type ReceiptV2 struct {
	Version int               `json:"schemaVersion"`
	Result  string            `json:"result"`
	Catalog Catalog           `json:"catalog"`
	Targets []string          `json:"targets"`
	Tags    map[string]string `json:"image_tags"`
}

func ReadCatalog(path string) (Catalog, error) {
	var c Catalog
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	if e != nil {
		return c, e
	}
	return c, c.Validate()
}
func (c Catalog) Validate() error {
	if c.Version != 2 || !revisionPattern.MatchString(c.Revision) || !revisionPattern.MatchString(c.BaselineRevision) || c.Platform != "linux/amd64" || len(c.Images) != len(Images) {
		return errors.New("catalog_invalid")
	}
	seen := map[string]bool{}
	for _, im := range Images {
		v, ok := c.Images[im.Target]
		if !ok || v.Repository != im.Repository || v.Platform != c.Platform || !revisionPattern.MatchString(v.Revision) || !idPattern.MatchString(v.ImageID) || !fingerprintPattern.MatchString(v.SourceFingerprint) || seen[v.ImageID] {
			return fmt.Errorf("%s_catalog_invalid", im.Target)
		}
		seen[v.ImageID] = true
		switch v.Origin {
		case "built":
			if v.Revision != c.Revision || (v.Digest != "" && !idPattern.MatchString(v.Digest)) {
				return errors.New("built_catalog_invalid")
			}
		case "reused":
			if !idPattern.MatchString(v.Digest) {
				return errors.New("reused_catalog_invalid")
			}
		default:
			return errors.New("catalog_origin_invalid")
		}
	}
	return nil
}
func (c Catalog) Targets() []string {
	out := []string{}
	for _, im := range Images {
		if c.Images[im.Target].Origin == "built" {
			out = append(out, im.Target)
		}
	}
	sort.Strings(out)
	return out
}

// ValidateFingerprints accepts the three core targets and the optional extractor fixture.
func ValidateFingerprints(fingerprints map[string]string) error {
	for _, im := range Images {
		if !fingerprintPattern.MatchString(fingerprints[im.Target]) {
			return errors.New("fingerprints_invalid")
		}
	}
	for key, value := range fingerprints {
		if key != "web" && key != "background" && key != "executor" && key != "extractor" {
			return errors.New("fingerprints_invalid")
		}
		if !fingerprintPattern.MatchString(value) {
			return errors.New("fingerprints_invalid")
		}
	}
	return nil
}
func ValidateInputs(c, baseline Catalog, impact Impact) error {
	if e := c.Validate(); e != nil {
		return e
	}
	if e := baseline.Validate(); e != nil {
		return errors.New("baseline_invalid")
	}
	if baseline.Revision != c.BaselineRevision || impact.Version != 1 || impact.Base != baseline.Revision || impact.Head != c.Revision {
		return errors.New("baseline_or_impact_mismatch")
	}
	targets := append([]string{}, impact.Targets...)
	sort.Strings(targets)
	if !reflect.DeepEqual(targets, c.Targets()) {
		return errors.New("selection_mismatch")
	}
	if err := ValidateFingerprints(impact.Fingerprints); err != nil {
		return errors.New("fingerprints_invalid")
	}
	for _, im := range Images {
		v := c.Images[im.Target]
		old := baseline.Images[im.Target]
		if impact.Fingerprints[im.Target] != v.SourceFingerprint {
			return errors.New("fingerprint_mismatch")
		}
		if v.Origin == "reused" && (v.Repository != old.Repository || v.Revision != old.Revision || v.Platform != old.Platform || v.ImageID != old.ImageID || v.Digest != old.Digest || v.SourceFingerprint != old.SourceFingerprint) {
			return errors.New("reused_baseline_mismatch")
		}
		if v.Origin == "built" && v.Digest != "" {
			return errors.New("candidate_digest_must_be_empty")
		}
		if !idPattern.MatchString(old.Digest) {
			return errors.New("baseline_digest_missing")
		}
	}
	return nil
}

// SelectiveOptions callbacks are trusted adapters: Baseline must resolve the
// authoritative latest published catalog on each call; Lookup must distinguish
// absent tags from registry failures. Neither callback may infer absence on error.
type SelectiveOptions struct {
	Catalog          Catalog
	ExpectedBaseline Catalog
	Impact           Impact
	Receipt          ReceiptV2
	Baseline         func() (Catalog, error)
	Lookup           func(tag string) (digest string, exists bool, err error)
	// Reconcile proves the existing registry digest resolves to the accepted config ID and platform.
	Reconcile func(tag, digest string, expected CatalogImage) error
	Run       Runner
	Output    string
}

func PublishSelective(o SelectiveOptions) (Catalog, error) {
	var none Catalog
	if o.Baseline == nil || o.Lookup == nil || o.Run == nil || o.Output == "" {
		return none, errors.New("release_options_invalid")
	}
	baseline, e := o.Baseline()
	if e != nil {
		return none, fmt.Errorf("baseline_unavailable: %w", e)
	}
	if !reflect.DeepEqual(baseline, o.ExpectedBaseline) {
		return none, errors.New("baseline_stale")
	}
	if e = ValidateInputs(o.Catalog, baseline, o.Impact); e != nil {
		return none, e
	}
	if o.Receipt.Version != 2 || o.Receipt.Result != "passed" || !reflect.DeepEqual(o.Receipt.Catalog, o.Catalog) || !reflect.DeepEqual(o.Receipt.Targets, o.Catalog.Targets()) || len(o.Receipt.Tags) != len(Images) {
		return none, errors.New("receipt_mismatch")
	}
	if _, e = os.Lstat(o.Output); e == nil {
		return none, errors.New("manifest_already_exists")
	} else if !os.IsNotExist(e) {
		return none, e
	}
	for _, im := range Images {
		v := o.Catalog.Images[im.Target]
		if o.Receipt.Tags[im.Target] != im.Local {
			return none, errors.New("receipt_tag_mismatch")
		}
		out, err := o.Run("image", "inspect", `--format={{.Id}} {{.Os}}/{{.Architecture}} {{index .Config.Labels "org.opencontainers.image.revision"}}`, im.Local)
		f := strings.Fields(out)
		if err != nil || len(f) != 3 || f[0] != v.ImageID || f[1] != v.Platform || f[2] != v.Revision {
			return none, fmt.Errorf("%s_image_mismatch", im.Target)
		}
	}
	// Check every target before any mutation. Existing tags require explicit proof.
	reconciled := map[string]string{}
	for _, im := range Images {
		if o.Catalog.Images[im.Target].Origin != "built" {
			continue
		}
		tag := "ghcr.io/" + im.Repository + ":sha-" + o.Catalog.Revision
		digest, exists, err := o.Lookup(tag)
		if err != nil {
			return none, fmt.Errorf("tag_lookup_failed: %w", err)
		}
		if exists {
			if !idPattern.MatchString(digest) || o.Reconcile == nil {
				return none, errors.New("tag_already_exists")
			}
			if err := o.Reconcile(tag, digest, o.Catalog.Images[im.Target]); err != nil {
				return none, fmt.Errorf("tag_reconciliation_failed: %w", err)
			}
			reconciled[im.Target] = digest
		}
	}
	fresh := func() error {
		v, err := o.Baseline()
		if err != nil {
			return fmt.Errorf("baseline_unavailable: %w", err)
		}
		if !reflect.DeepEqual(v, baseline) {
			return errors.New("baseline_stale")
		}
		return nil
	}
	if e = fresh(); e != nil {
		return none, e
	}
	result := o.Catalog
	result.Images = make(map[string]CatalogImage, len(o.Catalog.Images))
	for k, v := range o.Catalog.Images {
		result.Images[k] = v
	}
	for _, im := range Images {
		v := result.Images[im.Target]
		if v.Origin != "built" {
			continue
		}
		tag := "ghcr.io/" + im.Repository + ":sha-" + o.Catalog.Revision
		if digest, ok := reconciled[im.Target]; ok {
			v.Digest = digest
			result.Images[im.Target] = v
			continue
		}
		if _, e = o.Run("tag", v.ImageID, tag); e != nil {
			return none, fmt.Errorf("tag_failed: %w", e)
		}
		out, err := o.Run("push", tag)
		if err != nil {
			return none, fmt.Errorf("push_failed: %w", err)
		}
		found := digestPattern.FindAllStringSubmatch(out, -1)
		if len(found) == 0 {
			return none, errors.New("digest_missing")
		}
		v.Digest = found[len(found)-1][1]
		result.Images[im.Target] = v
	}
	if e = fresh(); e != nil {
		return none, e
	}
	if e = os.MkdirAll(filepath.Dir(o.Output), 0755); e != nil {
		return none, e
	}
	f, e := os.OpenFile(o.Output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if e != nil {
		return none, e
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	e = enc.Encode(result)
	closeErr := f.Close()
	if e != nil {
		return none, e
	}
	if closeErr != nil {
		return none, closeErr
	}
	return result, nil
}
