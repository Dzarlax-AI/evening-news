// Package managedsetup creates private per-service runtime manifests for a new installation.
package managedsetup

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
)

// Layout contains mount points that must be private to the one-shot installer.
type Layout struct {
	Setup, Web, Domain, Background, Extractor, Egress, Postgres, PostgresSecrets string
	WebUID, DomainUID, BackgroundUID, ExtractorUID, EgressUID                    int
}

var roleNames = []string{"web", "domain", "worker", "execution", "ingestion", "processing", "scheduler"}

type State struct {
	Version         int                          `json:"version"`
	Options         Options                      `json:"options,omitempty"`
	ExternalDB      bool                         `json:"external_db,omitempty"`
	RuntimeValues   map[string]map[string]string `json:"runtime_values,omitempty"`
	InstallationID  string                       `json:"installation_id"`
	Site            string                       `json:"site"`
	Passwords       map[string]string            `json:"passwords"`
	Tokens          map[string]string            `json:"tokens"`
	CredentialKey   string                       `json:"credential_key"`
	CredentialKeyID string                       `json:"credential_key_id"`
}

func random(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
func writeNew(path string, data []byte, uid int) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(path)
		}
	}()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if uid >= 0 {
		if err = f.Chown(uid, uid); err != nil {
			return err
		}
	}
	if err = f.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}
func privateDir(path string, uid int) error {
	if path == "" {
		return errors.New("missing state volume")
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return err
	}
	if err := os.Chmod(path, 0700); err != nil {
		return err
	}
	if uid >= 0 {
		return os.Chown(path, uid, uid)
	}
	return nil
}
func dsn(role, password, database string) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword(role, password), Host: "postgres:5432", Path: database}
	q := u.Query()
	q.Set("sslmode", "disable")
	if database == "newsdb" {
		q.Set("search_path", "news")
	}
	u.RawQuery = q.Encode()
	return u.String()
}
func manifest(id, service string, values map[string]string) ([]byte, error) {
	return json.MarshalIndent(runtimeconfig.Manifest{Version: 1, Service: service, InstallationID: id, Values: values}, "", "  ")
}

// Prepare fails closed on incomplete state or an initialized PostgreSQL volume.
// Existing files are validated, never overwritten or automatically regenerated.
func Prepare(l Layout, site string) (bool, error) { return PrepareWithOptions(l, site, Options{}) }

func PrepareWithOptions(l Layout, site string, options Options) (bool, error) {
	if err := options.Validate(); err != nil {
		return false, err
	}
	u, err := url.Parse(site)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return false, errors.New("invalid SITE_BASE_URL")
	}
	site = strings.TrimSuffix(site, "/")
	dirs := []struct {
		path string
		uid  int
	}{{l.Setup, -1}, {l.Web, l.WebUID}, {l.Domain, l.DomainUID}, {l.Background, l.BackgroundUID}, {l.Extractor, l.ExtractorUID}, {l.Egress, l.EgressUID}, {l.PostgresSecrets, -1}}
	for _, d := range dirs {
		if d.path == "" {
			continue
		}
		if err = privateDir(d.path, d.uid); err != nil {
			return false, errors.New("state directory unavailable")
		}
	}
	pgInfo, err := os.Stat(l.Postgres)
	if err != nil || !pgInfo.IsDir() {
		return false, errors.New("database volume unavailable")
	}
	statePath := filepath.Join(l.Setup, "installation.json")
	if _, err = os.Stat(statePath); err == nil {
		existing, err := ReadState(l.Setup)
		if err != nil {
			return false, errors.New("installation options changed; explicit maintenance required")
		}
		if options.NativeBrowser == nil && existing.Options.NativeBrowser != nil {
			options.NativeBrowser = existing.Options.NativeBrowser
		}
		if len(options.TrustedProxyCIDRs) > 0 || options.NativeBrowser != nil {
			if !sameOptions(existing.Options, options) {
				return false, errors.New("installation options changed; explicit maintenance required")
			}
		}
		return verify(l, site)
	} else if !os.IsNotExist(err) {
		return false, errors.New("state unavailable")
	}
	if _, err = os.Stat(filepath.Join(l.Postgres, "PG_VERSION")); err == nil {
		return false, errors.New("database exists without installation state")
	} else if !os.IsNotExist(err) {
		return false, errors.New("database state unavailable")
	}
	// Any partial output is ambiguous: do not overwrite or invent replacement keys.
	partialPaths := []string{filepath.Join(l.Web, "runtime.json"), filepath.Join(l.Domain, "runtime.json"), filepath.Join(l.Background, "runtime.json"), filepath.Join(l.Extractor, "runtime.json"), filepath.Join(l.PostgresSecrets, "postgres-password")}
	if l.Egress != "" {
		partialPaths = append(partialPaths, filepath.Join(l.Egress, "runtime.json"))
	}
	for _, p := range partialPaths {
		if _, err = os.Lstat(p); err == nil {
			return false, errors.New("incomplete state requires recovery")
		} else if !os.IsNotExist(err) {
			return false, errors.New("state unavailable")
		}
	}
	s, err := NewState(site, options)
	if err != nil {
		return false, err
	}
	outputs, err := build(s, u)
	if err != nil {
		return false, err
	}
	for _, o := range []struct {
		path string
		data []byte
		uid  int
	}{{filepath.Join(l.PostgresSecrets, "postgres-password"), []byte(s.Passwords["owner"]), -1}, {filepath.Join(l.Web, "runtime.json"), outputs["web"], l.WebUID}, {filepath.Join(l.Domain, "runtime.json"), outputs["domain"], l.DomainUID}, {filepath.Join(l.Background, "runtime.json"), outputs["background"], l.BackgroundUID}, {filepath.Join(l.Extractor, "runtime.json"), outputs["extractor"], l.ExtractorUID}} {
		if err = writeNew(o.path, o.data, o.uid); err != nil {
			return false, errors.New("could not write private state")
		}
	}
	if s.Options.NativeBrowser != nil {
		if err = writeNew(filepath.Join(l.Egress, "runtime.json"), outputs["egress"], l.EgressUID); err != nil {
			return false, errors.New("could not write private state")
		}
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return false, err
	}
	if err = writeNew(statePath, raw, -1); err != nil {
		return false, errors.New("could not finalize private state")
	}
	return true, nil
}
func build(s State, u *url.URL) (map[string][]byte, error) {
	if err := s.Options.Validate(); err != nil {
		return nil, err
	}
	if s.ExternalDB {
		if err := validateExternalValues(s); err != nil {
			return nil, err
		}
		result := map[string][]byte{}
		names := []string{"web", "domain", "background", "extractor"}
		if s.Options.NativeBrowser != nil {
			names = append(names, "egress")
		}
		for _, name := range names {
			values := s.RuntimeValues[name]
			if len(values) == 0 {
				return nil, errors.New("external runtime state incomplete")
			}
			b, err := manifest(s.InstallationID, name, values)
			if err != nil {
				return nil, err
			}
			result[name] = b
		}
		return result, nil
	}
	p, t := s.Passwords, s.Tokens
	web := map[string]string{"DATABASE_URL": dsn("news_web_runtime", p["web"], "newsdb"), "JWT_SECRET": t["jwt"], "SCHEDULED_JOBS_TOKEN": t["scheduled"], "WEB_BACKEND_URL": "http://executor:8100", "WEB_BACKEND_TOKEN": t["web_backend"], "SCHEDULER_API_URL": "http://background:8080", "SCHEDULER_API_TOKEN": t["scheduler"], "SCHEDULER_API_ALLOW_HTTP": "true", "SITE_BASE_URL": s.Site, "ALLOWED_ORIGINS": s.Site, "TRUSTED_HOSTS": u.Hostname() + ",localhost,127.0.0.1,web"}
	if len(s.Options.TrustedProxyCIDRs) > 0 {
		web["TRUSTED_PROXY_CIDRS"] = strings.Join(s.Options.TrustedProxyCIDRs, ",")
	}
	if u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1") {
		web["LOCAL_HTTP_SETUP"] = "true"
	}
	domain := map[string]string{"DATABASE_URL": dsn("news_domain_runtime", p["domain"], "newsdb"), "JOB_EXECUTOR_TOKEN": t["job"], "ARTICLE_AI_TOKEN": t["article_ai"], "EXECUTION_SERVICE_URL": "http://background:8101", "EXTRACTION_SERVICE_URL": "http://extractor:8080", "EXTRACTION_SERVICE_TOKEN": t["extractor"], "WEB_BACKEND_TOKEN": t["web_backend"], "SITE_BASE_URL": s.Site, "NEWS_CREDENTIAL_KEY": s.CredentialKey, "NEWS_CREDENTIAL_KEY_ID": s.CredentialKeyID, "ALLOW_CREATE_ALL": "false"}
	nativeBrowser := s.Options.NativeBrowser != nil
	nativeMode := "false"
	if nativeBrowser {
		nativeMode = "true"
	}
	bg := map[string]string{"WORKER_DATABASE_URL": dsn("news_worker_runtime", p["worker"], "newsdb"), "EXECUTION_DATABASE_URL": dsn("news_execution_runtime", p["execution"], "newsdb"), "INGESTION_DATABASE_URL": dsn("news_ingestion_runtime", p["ingestion"], "newsdb"), "PROCESSING_DATABASE_URL": dsn("news_processing_runtime", p["processing"], "newsdb"), "SCHEDULER_DATABASE_URL": dsn("scheduler_runtime", p["scheduler"], "schedulerdb"), "JOB_EXECUTOR_TOKEN": t["job"], "ARTICLE_AI_TOKEN": t["article_ai"], "ARTICLE_INGESTION_TOKEN": t["ingestion"], "ARTICLE_PROCESSING_TOKEN": t["processing"], "SCHEDULER_API_TOKEN": t["scheduler"], "SCHEDULER_TARGET_NEWS_WORKER_TOKEN": t["scheduled"], "EXTRACTION_SERVICE_TOKEN": t["extractor"], "SCHEDULER_DB_SCHEMA": "scheduler_service", "SCHEDULER_START_PAUSED": "false", "SCHEDULER_TARGET_ALIASES": "news-worker", "SCHEDULER_TARGET_NEWS_WORKER_URL": "http://web:8000/api/v1/internal/scheduled-jobs", "SCHEDULER_ALLOW_HTTP_TARGETS": "true", "EXTRACTION_SERVICE_URL": "http://extractor:8080", "ARTICLE_AI_URL": "http://executor:8100", "SELECTOR_SERVICE_URL": "http://executor:8100", "DOMAIN_SERVICE_URL": "http://executor:8100", "JOB_EXECUTOR_URL": "http://background:8101", "EXTRACTION_SERVICE_NATIVE_BROWSER": nativeMode}
	extract := map[string]string{"EXTRACTOR_SERVICE_TOKEN": t["extractor"], "EXTRACTOR_HTTP_ONLY": "true"}
	var egress map[string]string
	if nativeBrowser {
		for key, value := range s.Options.NativeBrowser.ExtractorValues {
			extract[key] = value
		}
		extract["EXTRACTOR_SERVICE_TOKEN"] = t["extractor"]
		egress = s.Options.NativeBrowser.EgressValues
	}
	result := map[string][]byte{}
	for name, values := range map[string]map[string]string{"web": web, "domain": domain, "background": bg, "extractor": extract} {
		b, err := manifest(s.InstallationID, name, values)
		if err != nil {
			return nil, err
		}
		result[name] = b
	}
	if nativeBrowser {
		b, err := manifest(s.InstallationID, "egress", egress)
		if err != nil {
			return nil, err
		}
		result["egress"] = b
	}
	return result, nil
}
func verify(l Layout, site string) (bool, error) {
	s, err := ReadState(l.Setup)
	if err != nil || s.Site != site {
		return false, errors.New("installation state invalid or site changed")
	}
	u, _ := url.Parse(site)
	expected, err := build(s, u)
	if err != nil {
		return false, errors.New("installation state invalid")
	}
	paths := map[string]string{"web": l.Web, "domain": l.Domain, "background": l.Background, "extractor": l.Extractor}
	if s.Options.NativeBrowser != nil {
		paths["egress"] = l.Egress
	}
	for name, path := range paths {
		actual, err := os.ReadFile(filepath.Join(path, "runtime.json"))
		if err != nil || string(actual) != string(expected[name]) {
			return false, fmt.Errorf("%s runtime state missing or changed", name)
		}
	}
	if s.ExternalDB {
		return false, VerifyExternalState(l, s)
	}
	password, err := os.ReadFile(filepath.Join(l.PostgresSecrets, "postgres-password"))
	if err != nil || string(password) != s.Passwords["owner"] || len(password) == 0 {
		return false, errors.New("postgres state missing or changed")
	}
	return false, nil
}

// ReadState returns installer-only secrets. Callers must never log the result.
func ReadState(path string) (State, error) {
	var s State
	file := filepath.Join(path, "installation.json")
	info, err := os.Lstat(file)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return s, errors.New("installation state must be a private regular file")
	}
	raw, err := os.ReadFile(file)
	if err != nil {
		return s, errors.New("installation state unavailable")
	}
	if len(raw) > 65536 || runtimeconfig.DecodeStrict(raw, &s) != nil || s.Version != 1 || len(s.InstallationID) != 32 || s.Site == "" {
		return State{}, errors.New("installation state invalid")
	}
	if _, err := hex.DecodeString(s.InstallationID); err != nil || s.Options.Validate() != nil {
		return State{}, errors.New("installation state invalid")
	}
	for _, name := range append(append([]string{}, roleNames...), "owner") {
		if !validRandom(s.Passwords[name], 32) {
			return State{}, errors.New("installation state invalid")
		}
	}
	for _, name := range []string{"jwt", "scheduled", "scheduler", "web_backend", "job", "article_ai", "processing", "ingestion", "extractor"} {
		if !validRandom(s.Tokens[name], 32) {
			return State{}, errors.New("installation state invalid")
		}
	}
	key, err := base64.RawStdEncoding.DecodeString(s.CredentialKey)
	if err != nil || len(key) != 32 || s.CredentialKeyID == "" {
		return State{}, errors.New("installation state invalid")
	}
	if s.ExternalDB {
		if err := validateExternalValues(s); err != nil {
			return State{}, err
		}
	} else if len(s.RuntimeValues) != 0 {
		return State{}, errors.New("unexpected external runtime state")
	}
	return s, nil
}

func validRandom(v string, n int) bool {
	b, err := base64.RawURLEncoding.DecodeString(v)
	return err == nil && len(b) == n
}

// NewState generates independent installation secrets without writing files.
func NewState(site string, options Options) (State, error) {
	if err := options.Validate(); err != nil {
		return State{}, err
	}
	u, err := url.Parse(site)
	if err != nil || u.User != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return State{}, errors.New("invalid SITE_BASE_URL")
	}
	if u.Scheme == "http" && u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" {
		return State{}, errors.New("public installation requires HTTPS")
	}
	site = strings.TrimSuffix(site, "/")
	idbytes := make([]byte, 16)
	if _, err = rand.Read(idbytes); err != nil {
		return State{}, err
	}
	s := State{Options: options, Version: 1, InstallationID: hex.EncodeToString(idbytes), Site: site, Passwords: map[string]string{}, Tokens: map[string]string{}}
	for _, r := range append(append([]string{}, roleNames...), "owner") {
		s.Passwords[r], err = random(32)
		if err != nil {
			return State{}, err
		}
	}
	for _, r := range []string{"jwt", "scheduled", "scheduler", "web_backend", "job", "article_ai", "processing", "ingestion", "extractor"} {
		s.Tokens[r], err = random(32)
		if err != nil {
			return State{}, err
		}
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		return State{}, err
	}
	s.CredentialKey = base64.RawStdEncoding.EncodeToString(key)
	s.CredentialKeyID, err = random(16)
	if err != nil {
		return State{}, err
	}
	return s, nil
}

// Build renders private role manifests from validated installation state.
func Build(s State) (map[string][]byte, error) {
	u, err := url.Parse(s.Site)
	if err != nil {
		return nil, errors.New("invalid installation site")
	}
	return build(s, u)
}

// External installations may substitute only their pre-provisioned role DSNs.
// Every other value retains the generated purpose-secret and default contract.
func validateExternalValues(s State) error {
	wantRoles := 4
	if s.Options.NativeBrowser != nil {
		wantRoles++
	}
	if len(s.RuntimeValues) != wantRoles {
		return errors.New("external runtime state incomplete")
	}
	generated := s
	generated.ExternalDB = false
	generated.RuntimeValues = nil
	u, err := url.Parse(s.Site)
	if err != nil {
		return errors.New("external site invalid")
	}
	canonical, err := build(generated, u)
	if err != nil {
		return err
	}
	databases := map[string]map[string]bool{"web": {"DATABASE_URL": true}, "domain": {"DATABASE_URL": true}, "background": {"WORKER_DATABASE_URL": true, "EXECUTION_DATABASE_URL": true, "INGESTION_DATABASE_URL": true, "PROCESSING_DATABASE_URL": true, "SCHEDULER_DATABASE_URL": true}, "extractor": {}}
	for role, raw := range canonical {
		var m runtimeconfig.Manifest
		if json.Unmarshal(raw, &m) != nil {
			return errors.New("external state invalid")
		}
		values := s.RuntimeValues[role]
		if len(values) != len(m.Values) {
			return errors.New("external role contract changed")
		}
		for key, want := range m.Values {
			actual, ok := values[key]
			if !ok {
				return errors.New("external role contract incomplete")
			}
			if databases[role][key] {
				d, e := url.Parse(actual)
				if e != nil || (d.Scheme != "postgres" && d.Scheme != "postgresql") || d.User == nil || d.Hostname() == "" || d.Path == "" {
					return errors.New("external database URL invalid")
				}
			} else if actual != want {
				return errors.New("external purpose or runtime value changed")
			}
		}
	}
	return nil
}
