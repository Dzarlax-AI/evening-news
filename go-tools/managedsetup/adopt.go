package managedsetup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
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
	Services       map[string]map[string]string
	ServiceNames   map[string]bool
	PublishedPorts map[string][]PublishedPort
	Digest         string
	raw            []byte
}

// PublishedPort contains the only host-publish fields needed to validate the
// explicit loopback-only local HTTP adoption profile.
type PublishedPort struct {
	HostIP string `json:"host_ip"`
	Mode   string `json:"mode"`
}

func (c RenderedCompose) HasLoopbackPublishedPorts(service string) bool {
	ports := c.PublishedPorts[service]
	if len(ports) == 0 {
		return false
	}
	for _, port := range ports {
		ip, err := netip.ParseAddr(port.HostIP)
		if err != nil || !ip.IsLoopback() || strings.EqualFold(port.Mode, "host") {
			return false
		}
	}
	return true
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
	out.ServiceNames = make(map[string]bool, len(doc.Services))
	out.PublishedPorts = make(map[string][]PublishedPort, len(doc.Services))
	for service, spec := range doc.Services {
		out.ServiceNames[service] = true
		var serviceFields map[string]json.RawMessage
		if json.Unmarshal(spec, &serviceFields) != nil {
			return RenderedCompose{}, errors.New("unsupported rendered Compose service")
		}
		if portsRaw, exists := serviceFields["ports"]; exists {
			var ports []PublishedPort
			if json.Unmarshal(portsRaw, &ports) != nil {
				return RenderedCompose{}, errors.New("unsupported rendered Compose ports")
			}
			out.PublishedPorts[service] = ports
		}
		envRaw, exists := serviceFields["environment"]
		if !exists {
			out.Services[service] = map[string]string{}
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
	hasApp, hasWeb := out.ServiceNames["app"], out.ServiceNames["web"]
	if hasApp == hasWeb || !out.ServiceNames["executor"] || !out.ServiceNames["background"] || !out.ServiceNames["extractor"] || len(out.Services["app"])+len(out.Services["web"]) == 0 || len(out.Services["executor"]) == 0 || len(out.Services["background"]) == 0 || len(out.Services["extractor"]) == 0 {
		return RenderedCompose{}, errors.New("unsupported Compose service set")
	}
	h := sha256.Sum256(raw)
	out.Digest = hex.EncodeToString(h[:])
	out.raw = append([]byte(nil), raw...)
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
	if len(id) != 32 || len(sourceDigest) != 64 || (len(roles) != 4 && len(roles) != 5) {
		return "", bundle, errors.New("invalid adoption identity")
	}
	if _, err := hex.DecodeString(id); err != nil {
		return "", bundle, errors.New("invalid adoption identity")
	}
	if _, err := hex.DecodeString(sourceDigest); err != nil {
		return "", bundle, errors.New("invalid adoption source digest")
	}
	setupInfo, err := os.Lstat(setup)
	if err != nil || !setupInfo.IsDir() || setupInfo.Mode()&os.ModeSymlink != 0 {
		return "", bundle, errors.New("adoption state directory must be private")
	}
	if setupInfo.Mode().Perm()&0077 != 0 {
		entries, readErr := os.ReadDir(setup)
		if readErr != nil || len(entries) != 0 {
			return "", bundle, errors.New("adoption state directory must be private")
		}
		if err = os.Chmod(setup, 0700); err != nil {
			return "", bundle, errors.New("adoption state directory must be private")
		}
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
	if _, ok := bundle.ArtifactSHA256["compose-override.yml"]; !ok {
		return errors.New("adoption Compose override unavailable")
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
	if _, hasEgress := bundle.ManifestSHA256["egress"]; hasEgress {
		if l.Egress == "" {
			return errors.New("egress runtime state volume unavailable")
		}
		roles = append(roles, struct {
			name, path string
			uid        int
		}{"egress", l.Egress, l.EgressUID})
	}
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
		if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("runtime state volume unavailable")
		}
		entries, e := os.ReadDir(role.path)
		if e != nil || len(entries) > 1 {
			return errors.New("runtime state volume contains unexpected files")
		}
		for _, entry := range entries {
			if entry.Name() != "runtime.json" {
				return errors.New("runtime state volume contains unexpected files")
			}
		}
		target := filepath.Join(role.path, "runtime.json")
		if targetInfo, statErr := os.Lstat(target); statErr == nil {
			if !targetInfo.Mode().IsRegular() || targetInfo.Mode()&os.ModeSymlink != 0 {
				return errors.New("existing runtime state conflicts with adoption")
			}
			got, readErr := os.ReadFile(target)
			if readErr != nil || string(got) != string(manifest) {
				return errors.New("existing runtime state conflicts with adoption")
			}
		} else if !os.IsNotExist(statErr) {
			return errors.New("runtime state unavailable")
		}
	}
	override, err := os.ReadFile(filepath.Join(stage, "compose-override.yml"))
	if err != nil || len(override) == 0 || len(override) > maxAdoptionConfig {
		return errors.New("adoption Compose override unavailable")
	}
	if err = preflightWriteOrVerify(composeOutput, override); err != nil {
		return errors.New("adoption Compose override output unavailable")
	}
	marker := map[string]any{"installation_id": id, "source_digest": bundle.SourceDigest, "revision": bundle.Revision, "runtime_files_activated": true}
	markerBytes, _ := json.Marshal(marker)
	markerPath := filepath.Join(l.Setup, "adoption-committed.json")
	markerExists := true
	if err = preflightWriteOrVerify(markerPath, markerBytes); err != nil {
		return errors.New("adoption completion marker unavailable")
	} else if _, statErr := os.Lstat(markerPath); os.IsNotExist(statErr) {
		markerExists = false
	}
	// Validate every volume and output conflict before changing ownership or
	// writing any runtime file. Docker named volumes may start root-owned 0755.
	for _, role := range roles {
		if err = os.Chmod(role.path, 0700); err != nil {
			return errors.New("runtime state volume ownership unavailable")
		}
		if role.uid >= 0 && os.Chown(role.path, role.uid, role.uid) != nil {
			return errors.New("runtime state volume ownership unavailable")
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
		} else {
			if e = os.Chmod(target, 0600); e != nil || (role.uid >= 0 && os.Chown(target, role.uid, role.uid) != nil) {
				return errors.New("runtime state volume ownership unavailable")
			}
		}
	}
	if err = writeOrVerify(composeOutput, override); err != nil {
		return errors.New("adoption Compose override output unavailable")
	}
	if markerExists {
		return nil
	}
	if err = writeNew(markerPath, markerBytes, -1); err != nil {
		return errors.New("adoption completion marker unavailable")
	}
	return nil
}

func preflightWriteOrVerify(path string, data []byte) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("invalid output target")
	}
	old, err := os.ReadFile(path)
	if err != nil || string(old) != string(data) {
		return errors.New("output target conflicts")
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

// AdoptionComposeOverride emits only News service overlays. Service names come
// from the canonical local Compose configuration; browser and Readeck service
// definitions and all existing network topology remain owned by their sources.
func AdoptionComposeOverride(services map[string]string) ([]byte, error) {
	return AdoptionComposeOverrideWithRuntime(services, nil)
}

// AdoptionComposeOverrideWithRuntime preserves only Go runtime process
// settings that must be present before the managed application starts.
func AdoptionComposeOverrideWithRuntime(services map[string]string, processEnv map[string]string) ([]byte, error) {
	for key := range processEnv {
		if key != "GOMAXPROCS" && key != "GOMEMLIMIT" {
			return nil, errors.New("unsupported adoption process environment key")
		}
	}
	allowed := map[string]map[string]bool{"web": {"app": true, "web": true}, "domain": {"executor": true}, "background": {"background": true}, "extractor": {"extractor": true}, "egress": {"egress": true}}
	for role, name := range services {
		if role == "migrate" && name == "migrate" {
			continue
		}
		if !allowed[role][name] {
			return nil, errors.New("unsupported adoption Compose service name")
		}
	}
	for _, role := range []string{"web", "domain", "background", "extractor"} {
		if services[role] == "" {
			return nil, errors.New("adoption Compose service set incomplete")
		}
	}
	if name := services["egress"]; name != "" && !allowed["egress"][name] {
		return nil, errors.New("unsupported adoption Compose service name")
	}
	var b strings.Builder
	b.WriteString("services:\n")
	items := []struct{ role, volume string }{{"web", "managed_web"}, {"domain", "managed_domain"}, {"background", "managed_background"}, {"extractor", "managed_extractor"}}
	if services["egress"] != "" {
		items = append(items, struct{ role, volume string }{"egress", "managed_egress"})
	}
	for _, item := range items {
		fmt.Fprintf(&b, "  %s:\n    env_file: !override []\n    environment: !override\n      NEWS_RUNTIME_FILE: /run/news/runtime.json\n", services[item.role])
		if item.role == "background" {
			for _, key := range []string{"GOMAXPROCS", "GOMEMLIMIT"} {
				if value, ok := processEnv[key]; ok {
					quoted, _ := json.Marshal(value)
					fmt.Fprintf(&b, "      %s: %s\n", key, quoted)
				}
			}
		}
		fmt.Fprintf(&b, "    volumes:\n      - %s:/run/news:ro\n", item.volume)
		if item.role == "egress" {
			fmt.Fprintf(&b, "    profiles: !override []\n")
		}
	}
	if services["migrate"] == "migrate" {
		b.WriteString("  migrate:\n    entrypoint: !override [\"/bin/true\"]\n")
	}
	b.WriteString("volumes:\n")
	volumes := []string{"managed_web", "managed_domain", "managed_background", "managed_extractor"}
	if services["egress"] != "" {
		volumes = append(volumes, "managed_egress")
	}
	for _, v := range volumes {
		fmt.Fprintf(&b, "  %s: {}\n", v)
	}
	return []byte(b.String()), nil
}
