package managedsetup

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
)

var externalDSNKeys = map[string]string{
	"web": "DATABASE_URL", "domain": "DATABASE_URL", "worker": "WORKER_DATABASE_URL",
	"execution": "EXECUTION_DATABASE_URL", "ingestion": "INGESTION_DATABASE_URL",
	"processing": "PROCESSING_DATABASE_URL", "scheduler": "SCHEDULER_DATABASE_URL",
}

// NewExternalState starts from the same generated purpose tokens and encryption
// material as a local install, then replaces only the seven DB connection
// values with a pre-provisioned role bundle. No arbitrary environment map is
// accepted by this constructor.
func NewExternalState(site string, options Options, roleDSNs map[string]string) (State, map[string][]byte, error) {
	var zero State
	if len(roleDSNs) != len(externalDSNKeys) {
		return zero, nil, errors.New("external database role bundle is incomplete")
	}
	for role := range externalDSNKeys {
		if strings.TrimSpace(roleDSNs[role]) == "" {
			return zero, nil, errors.New("external database role bundle is incomplete")
		}
	}
	s, err := NewState(site, options)
	if err != nil {
		return zero, nil, err
	}
	generated, err := Build(s)
	if err != nil {
		return zero, nil, errors.New("external runtime generation failed")
	}
	values := map[string]map[string]string{}
	roles := []string{"web", "domain", "background", "extractor"}
	if s.Options.NativeBrowser != nil {
		roles = append(roles, "egress")
	}
	for _, role := range roles {
		var m runtimeconfig.Manifest
		if json.Unmarshal(generated[role], &m) != nil || m.Service != role || m.InstallationID != s.InstallationID || m.Values == nil {
			return zero, nil, errors.New("generated runtime state invalid")
		}
		values[role] = m.Values
	}
	for role, key := range externalDSNKeys {
		service := role
		if role == "worker" || role == "execution" || role == "ingestion" || role == "processing" || role == "scheduler" {
			service = "background"
		}
		values[service][key] = roleDSNs[role]
	}
	s.ExternalDB = true
	s.RuntimeValues = values
	manifests, err := Build(s)
	if err != nil {
		return zero, nil, errors.New("external runtime validation failed")
	}
	return s, manifests, nil
}

// InstallExternalState activates manifests after the caller has completed the
// external database migrations and identity-row commit. The private local
// bootstrap DSN is installer-only and is never written to a runtime manifest.
// installation.json is written last and acts as the local state commit marker.
func InstallExternalState(l Layout, s State, manifests map[string][]byte, bootstrapDSN string) error {
	wantManifests := 4
	if s.Options.NativeBrowser != nil {
		wantManifests++
	}
	if !s.ExternalDB || bootstrapDSN == "" || s.InstallationID == "" || len(manifests) != wantManifests {
		return errors.New("external installation state invalid")
	}
	setupInfo, err := os.Lstat(l.Setup)
	if err != nil || !setupInfo.IsDir() || setupInfo.Mode()&os.ModeSymlink != 0 || setupInfo.Mode().Perm()&0077 != 0 {
		return errors.New("external setup directory must be private")
	}
	roleDirs := []struct {
		name, path string
		uid        int
	}{{"web", l.Web, l.WebUID}, {"domain", l.Domain, l.DomainUID}, {"background", l.Background, l.BackgroundUID}, {"extractor", l.Extractor, l.ExtractorUID}}
	if s.Options.NativeBrowser != nil {
		roleDirs = append(roleDirs, struct {
			name, path string
			uid        int
		}{"egress", l.Egress, l.EgressUID})
	}
	for _, role := range roleDirs {
		var m runtimeconfig.Manifest
		if json.Unmarshal(manifests[role.name], &m) != nil || m.Version != runtimeconfig.Version || m.Service != role.name || m.InstallationID != s.InstallationID {
			return errors.New("external runtime manifest identity invalid")
		}
		info, e := os.Lstat(role.path)
		if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
			return errors.New("external runtime volume unavailable")
		}
	}
	statePath := filepath.Join(l.Setup, "installation.json")
	stateBytes, err := json.Marshal(s)
	if err != nil {
		return errors.New("external setup state invalid")
	}
	bootstrapPath := filepath.Join(l.Setup, "external-bootstrap.json")
	for _, target := range []struct {
		path string
		data []byte
	}{{bootstrapPath, []byte(bootstrapDSN)}, {statePath, stateBytes}} {
		if got, e := os.ReadFile(target.path); e == nil {
			if string(got) != string(target.data) {
				return errors.New("existing external installer state conflicts")
			}
		} else if !os.IsNotExist(e) {
			return errors.New("external installer state unavailable")
		}
	}
	for _, role := range roleDirs {
		path := filepath.Join(role.path, "runtime.json")
		if got, e := os.ReadFile(path); e == nil {
			if string(got) != string(manifests[role.name]) {
				return fmt.Errorf("existing %s runtime state conflicts", role.name)
			}
		} else if !os.IsNotExist(e) {
			return errors.New("external runtime state unavailable")
		}
	}
	if _, e := os.Stat(bootstrapPath); os.IsNotExist(e) {
		if err = writeNew(bootstrapPath, []byte(bootstrapDSN), -1); err != nil {
			return errors.New("external bootstrap state unavailable")
		}
	} else if e != nil {
		return errors.New("external bootstrap state unavailable")
	}
	for _, role := range roleDirs {
		path := filepath.Join(role.path, "runtime.json")
		if _, e := os.Stat(path); os.IsNotExist(e) {
			if err = writeNew(path, manifests[role.name], role.uid); err != nil {
				return errors.New("external runtime state copy failed")
			}
		} else if e != nil {
			return errors.New("external runtime state unavailable")
		}
	}
	if _, e := os.Stat(statePath); os.IsNotExist(e) {
		if err = writeNew(statePath, stateBytes, -1); err != nil {
			return errors.New("external setup state commit failed")
		}
	} else if e != nil {
		return errors.New("external setup state unavailable")
	}
	return nil
}

// VerifyExternalState checks that installer-only external DB state and every
// role manifest still match the committed installation record.
func VerifyExternalState(l Layout, s State) error {
	wantRoles := 4
	if s.Options.NativeBrowser != nil {
		wantRoles++
	}
	if !s.ExternalDB || len(s.RuntimeValues) != wantRoles {
		return errors.New("external installation state invalid")
	}
	setupInfo, err := os.Lstat(l.Setup)
	if err != nil || !setupInfo.IsDir() || setupInfo.Mode()&os.ModeSymlink != 0 || setupInfo.Mode().Perm()&0077 != 0 {
		return errors.New("external setup directory must be private")
	}
	bootstrapPath := filepath.Join(l.Setup, "external-bootstrap.json")
	info, err := os.Lstat(bootstrapPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || info.Size() > 8192 {
		return errors.New("external bootstrap state unavailable")
	}
	bootstrap, err := os.ReadFile(bootstrapPath)
	if err != nil || strings.TrimSpace(string(bootstrap)) == "" {
		return errors.New("external bootstrap state unavailable")
	}
	manifests, err := Build(s)
	if err != nil {
		return errors.New("external installation state invalid")
	}
	roles := []struct{ name, path string }{{"web", l.Web}, {"domain", l.Domain}, {"background", l.Background}, {"extractor", l.Extractor}}
	if s.Options.NativeBrowser != nil {
		roles = append(roles, struct{ name, path string }{"egress", l.Egress})
	}
	for _, role := range roles {
		info, e := os.Lstat(role.path)
		if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
			return errors.New("external runtime volume unavailable")
		}
		actual, e := os.ReadFile(filepath.Join(role.path, "runtime.json"))
		if e != nil || string(actual) != string(manifests[role.name]) {
			return fmt.Errorf("%s external runtime state missing or changed", role.name)
		}
	}
	return nil
}
