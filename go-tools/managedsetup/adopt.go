package managedsetup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
)

const maxAdoptionConfig = 1 << 20

// RenderedCompose is the deliberately narrow input supported by adopt. Values
// are secrets and must stay in-process; callers should only print Keys().
type RenderedCompose struct {
	Services map[string]map[string]string
	Digest   string
}

type composeDocument struct {
	Services map[string]json.RawMessage `json:"services"`
}

// ReadRenderedCompose accepts only a private rendered Compose JSON file. It
// does not evaluate dotenv syntax, interpolation, shell commands, or YAML.
func ReadRenderedCompose(path string) (RenderedCompose, error) {
	var out RenderedCompose
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || info.Size() <= 0 || info.Size() > maxAdoptionConfig {
		return out, errors.New("adoption config must be a private regular rendered Compose JSON file")
	}
	f, err := os.Open(path)
	if err != nil {
		return out, errors.New("adoption config unavailable")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return out, errors.New("adoption config changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxAdoptionConfig+1))
	if err != nil || len(raw) == 0 || len(raw) > maxAdoptionConfig {
		return out, errors.New("adoption config unreadable or too large")
	}
	if err = rejectDuplicateJSON(raw); err != nil {
		return out, errors.New("adoption config contains ambiguous JSON")
	}
	var doc composeDocument
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if err = dec.Decode(&doc); err != nil || doc.Services == nil || dec.Decode(new(any)) != io.EOF {
		return out, errors.New("unsupported rendered Compose JSON shape")
	}
	out.Services = map[string]map[string]string{}
	for service, spec := range doc.Services {
		var serviceFields map[string]json.RawMessage
		if json.Unmarshal(spec, &serviceFields) != nil {
			return RenderedCompose{}, errors.New("unsupported rendered Compose service")
		}
		envRaw, exists := serviceFields["environment"]
		if !exists {
			continue
		}
		var env map[string]json.RawMessage
		if json.Unmarshal(envRaw, &env) != nil {
			return RenderedCompose{}, errors.New("unsupported Compose environment shape")
		}
		values := map[string]string{}
		for key, value := range env {
			var text string
			trimmed := strings.TrimSpace(string(value))
			if len(trimmed) < 2 || trimmed[0] != '"' {
				return RenderedCompose{}, errors.New("unsupported Compose environment value")
			}
			if err := json.Unmarshal(value, &text); err != nil {
				return RenderedCompose{}, errors.New("unsupported Compose environment value")
			}
			if strings.TrimSpace(key) == "" || strings.Contains(key, "=") {
				return RenderedCompose{}, errors.New("unsupported Compose environment key")
			}
			values[key] = text
		}
		out.Services[service] = values
	}
	if len(out.Services["web"]) == 0 || len(out.Services["executor"]) == 0 || len(out.Services["background"]) == 0 || len(out.Services["extractor"]) == 0 {
		return RenderedCompose{}, errors.New("unsupported Compose service set")
	}
	h := sha256.Sum256(raw)
	out.Digest = hex.EncodeToString(h[:])
	return out, nil
}

func rejectDuplicateJSON(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	var walk func() error
	walk = func() error {
		t, err := d.Token()
		if err != nil {
			return err
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				if !ok || seen[name] {
					return errors.New("duplicate key")
				}
				seen[name] = true
				if err = walk(); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("invalid object")
			}
		case '[':
			for d.More() {
				if err = walk(); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("invalid array")
			}
		default:
			return errors.New("invalid JSON delimiter")
		}
		return nil
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

// Keys returns only service and key names, sorted for stable, secret-free
// dry-run output.
func (c RenderedCompose) Keys() map[string][]string {
	out := make(map[string][]string, len(c.Services))
	for service, values := range c.Services {
		for key := range values {
			out[service] = append(out[service], key)
		}
		sort.Strings(out[service])
	}
	return out
}

type AdoptionBundle struct {
	Version        int               `json:"version"`
	InstallationID string            `json:"installation_id"`
	Revision       int64             `json:"revision"`
	SourceDigest   string            `json:"source_digest"`
	ManifestSHA256 map[string]string `json:"manifest_sha256"`
	ArtifactSHA256 map[string]string `json:"artifact_sha256"`
}

// StageAdoption creates immutable, private service manifests in setup staging.
// It never activates the files or writes the legacy configuration values into
// shared state. Credential fields must already have been removed by the caller.
func StageAdoption(setup string, id string, sourceDigest string, roles map[string]map[string]string, policies map[string]struct{ Allowed, Required []string }, extra map[string][]byte) (string, AdoptionBundle, error) {
	var bundle AdoptionBundle
	if len(id) != 32 || len(sourceDigest) != 64 || len(roles) != 4 {
		return "", bundle, errors.New("invalid adoption identity")
	}
	if _, err := hex.DecodeString(id); err != nil {
		return "", bundle, errors.New("invalid adoption identity")
	}
	if _, err := hex.DecodeString(sourceDigest); err != nil {
		return "", bundle, errors.New("invalid adoption source digest")
	}
	setupInfo, err := os.Lstat(setup)
	if err != nil || !setupInfo.IsDir() || setupInfo.Mode()&os.ModeSymlink != 0 || setupInfo.Mode().Perm()&0077 != 0 {
		return "", bundle, errors.New("adoption state directory must be private")
	}
	dir := filepath.Join(setup, "adoptions", id)
	if err := os.MkdirAll(filepath.Dir(dir), 0700); err != nil {
		return "", bundle, errors.New("adoption staging unavailable")
	}
	if err := os.Chmod(filepath.Dir(dir), 0700); err != nil {
		return "", bundle, errors.New("adoption staging unavailable")
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		if !os.IsExist(err) {
			return "", bundle, errors.New("adoption staging unavailable")
		}
		return verifyStagedAdoption(dir, id, sourceDigest, roles, policies)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(dir)
		}
	}()
	bundle = AdoptionBundle{Version: 1, InstallationID: id, Revision: 1, SourceDigest: sourceDigest, ManifestSHA256: map[string]string{}, ArtifactSHA256: map[string]string{}}
	for role, values := range roles {
		policy, ok := policies[role]
		if !ok {
			return "", AdoptionBundle{}, errors.New("unsupported adoption role")
		}
		manifest, err := json.MarshalIndent(runtimeconfig.Manifest{Version: runtimeconfig.Version, Service: role, InstallationID: id, Values: values}, "", "  ")
		if err != nil {
			return "", AdoptionBundle{}, errors.New("adoption manifest encoding failed")
		}
		if _, err = runtimeconfig.Decode(manifest, role, policy.Allowed, policy.Required); err != nil {
			return "", AdoptionBundle{}, errors.New("adoption runtime parity validation failed")
		}
		name := role + ".json"
		if err = writeNew(filepath.Join(dir, name), manifest, -1); err != nil {
			return "", AdoptionBundle{}, errors.New("adoption staging failed")
		}
		h := sha256.Sum256(manifest)
		bundle.ManifestSHA256[role] = hex.EncodeToString(h[:])
	}
	for name, raw := range extra {
		if filepath.Base(name) != name || name == "" || name == "bundle.json" || len(raw) > maxAdoptionConfig {
			return "", AdoptionBundle{}, errors.New("invalid adoption staging artifact")
		}
		if err := writeNew(filepath.Join(dir, name), raw, -1); err != nil {
			return "", AdoptionBundle{}, errors.New("adoption staging failed")
		}
		h := sha256.Sum256(raw)
		bundle.ArtifactSHA256[name] = hex.EncodeToString(h[:])
	}
	index, err := json.Marshal(bundle)
	if err != nil {
		return "", AdoptionBundle{}, errors.New("adoption staging failed")
	}
	if err = writeNew(filepath.Join(dir, "bundle.json"), index, -1); err != nil {
		return "", AdoptionBundle{}, errors.New("adoption staging failed")
	}
	cleanup = false
	return dir, bundle, nil
}

func verifyStagedAdoption(dir, id, sourceDigest string, roles map[string]map[string]string, policies map[string]struct{ Allowed, Required []string }) (string, AdoptionBundle, error) {
	var bundle AdoptionBundle
	raw, err := os.ReadFile(filepath.Join(dir, "bundle.json"))
	if err != nil || json.Unmarshal(raw, &bundle) != nil || bundle.Version != 1 || bundle.InstallationID != id || bundle.Revision != 1 || bundle.SourceDigest != sourceDigest {
		return "", AdoptionBundle{}, errors.New("existing adoption stage is ambiguous")
	}
	if len(bundle.ManifestSHA256) != len(roles) {
		return "", AdoptionBundle{}, errors.New("existing adoption stage is ambiguous")
	}
	for name, expected := range bundle.ArtifactSHA256 {
		if filepath.Base(name) != name || name == "bundle.json" {
			return "", AdoptionBundle{}, errors.New("existing adoption artifact is ambiguous")
		}
		got, e := os.ReadFile(filepath.Join(dir, name))
		h := sha256.Sum256(got)
		if e != nil || hex.EncodeToString(h[:]) != expected {
			return "", AdoptionBundle{}, errors.New("existing adoption artifact is ambiguous")
		}
	}
	for role, values := range roles {
		policy, ok := policies[role]
		if !ok {
			return "", AdoptionBundle{}, errors.New("unsupported adoption role")
		}
		manifest, e := json.MarshalIndent(runtimeconfig.Manifest{Version: runtimeconfig.Version, Service: role, InstallationID: id, Values: values}, "", "  ")
		if e != nil {
			return "", AdoptionBundle{}, errors.New("adoption manifest encoding failed")
		}
		if _, e = runtimeconfig.Decode(manifest, role, policy.Allowed, policy.Required); e != nil {
			return "", AdoptionBundle{}, errors.New("adoption runtime parity validation failed")
		}
		got, e := os.ReadFile(filepath.Join(dir, role+".json"))
		h := sha256.Sum256(manifest)
		if e != nil || string(got) != string(manifest) || bundle.ManifestSHA256[role] != hex.EncodeToString(h[:]) {
			return "", AdoptionBundle{}, errors.New("existing adoption stage is ambiguous")
		}
	}
	return dir, bundle, nil
}

// ActivateAdoption copies validated staged manifests into runtime volumes only
// after the caller has durably committed the database import. Existing targets
// are accepted only when byte-identical; the completion marker is written last.
func ActivateAdoption(l Layout, stage string, id, composeOutput string, policies map[string]struct{ Allowed, Required []string }) error {
	if composeOutput == "" {
		return errors.New("adoption compose output path is required")
	}
	var bundle AdoptionBundle
	raw, err := os.ReadFile(filepath.Join(stage, "bundle.json"))
	if err != nil || json.Unmarshal(raw, &bundle) != nil || bundle.Version != 1 || bundle.InstallationID != id || bundle.Revision != 1 {
		return errors.New("adoption bundle invalid")
	}
	if _, ok := bundle.ArtifactSHA256["import.json"]; !ok {
		return errors.New("adoption import artifact unavailable")
	}
	for name, expected := range bundle.ArtifactSHA256 {
		if filepath.Base(name) != name || name == "bundle.json" {
			return errors.New("adoption artifact invalid")
		}
		artifact, e := os.ReadFile(filepath.Join(stage, name))
		h := sha256.Sum256(artifact)
		if e != nil || hex.EncodeToString(h[:]) != expected {
			return errors.New("adoption artifact integrity failed")
		}
	}
	roles := []struct {
		name, path string
		uid        int
	}{{"web", l.Web, l.WebUID}, {"domain", l.Domain, l.DomainUID}, {"background", l.Background, l.BackgroundUID}, {"extractor", l.Extractor, l.ExtractorUID}}
	for _, role := range roles {
		manifest, e := os.ReadFile(filepath.Join(stage, role.name+".json"))
		if e != nil {
			return errors.New("adoption manifest unavailable")
		}
		h := sha256.Sum256(manifest)
		if bundle.ManifestSHA256[role.name] != hex.EncodeToString(h[:]) {
			return errors.New("adoption manifest integrity failed")
		}
		policy, ok := policies[role.name]
		if !ok {
			return errors.New("unsupported adoption role")
		}
		if _, e = runtimeconfig.Decode(manifest, role.name, policy.Allowed, policy.Required); e != nil {
			return errors.New("adoption manifest invalid")
		}
		info, e := os.Lstat(role.path)
		if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
			return errors.New("runtime state volume unavailable")
		}
	}
	// Validate all conflicts before the first target write.
	for _, role := range roles {
		want, _ := os.ReadFile(filepath.Join(stage, role.name+".json"))
		got, e := os.ReadFile(filepath.Join(role.path, "runtime.json"))
		if e == nil && string(got) != string(want) {
			return errors.New("existing runtime state conflicts with adoption")
		}
		if e != nil && !os.IsNotExist(e) {
			return errors.New("runtime state unavailable")
		}
	}
	for _, role := range roles {
		want, _ := os.ReadFile(filepath.Join(stage, role.name+".json"))
		target := filepath.Join(role.path, "runtime.json")
		if _, e := os.Stat(target); os.IsNotExist(e) {
			if e = writeNew(target, want, role.uid); e != nil {
				return errors.New("runtime adoption staging failed")
			}
		} else if e != nil {
			return errors.New("runtime state unavailable")
		}
	}
	if err = writeOrVerify(composeOutput, []byte(adoptionComposeOverride)); err != nil {
		return errors.New("adoption Compose override output unavailable")
	}
	marker := map[string]any{"installation_id": id, "source_digest": bundle.SourceDigest, "revision": bundle.Revision, "runtime_files_activated": true}
	markerBytes, _ := json.Marshal(marker)
	markerPath := filepath.Join(l.Setup, "adoption-committed.json")
	if got, e := os.ReadFile(markerPath); e == nil {
		var prior map[string]any
		if json.Unmarshal(got, &prior) != nil || prior["installation_id"] != id || prior["source_digest"] != bundle.SourceDigest {
			return errors.New("existing adoption marker conflicts")
		}
		return nil
	} else if !os.IsNotExist(e) {
		return errors.New("adoption completion marker unavailable")
	}
	if err = writeNew(markerPath, markerBytes, -1); err != nil {
		return errors.New("adoption completion marker unavailable")
	}
	return nil
}

func writeOrVerify(path string, data []byte) error {
	info, err := os.Lstat(path)
	if err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("invalid output target")
		}
		old, e := os.ReadFile(path)
		if e != nil || string(old) != string(data) {
			return errors.New("output target conflicts")
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	return writeNew(path, data, -1)
}

const adoptionComposeOverride = `services:
  web:
    environment:
      NEWS_RUNTIME_FILE: /run/news/runtime.json
    volumes:
      - managed_web:/run/news:ro
  executor:
    environment:
      NEWS_RUNTIME_FILE: /run/news/runtime.json
    volumes:
      - managed_domain:/run/news:ro
  background:
    environment:
      NEWS_RUNTIME_FILE: /run/news/runtime.json
    volumes:
      - managed_background:/run/news:ro
  extractor:
    environment:
      NEWS_RUNTIME_FILE: /run/news/runtime.json
    volumes:
      - managed_extractor:/run/news:ro
  migrate:
    entrypoint: ["/bin/true"]
volumes:
  managed_web: {}
  managed_domain: {}
  managed_background: {}
  managed_extractor: {}
`
