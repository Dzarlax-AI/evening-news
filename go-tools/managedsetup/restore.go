package managedsetup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
)

// RestoreState stages authenticated bundle contents only into completely empty
// installation volumes. The final identity file is written last; a partial
// copy cannot be mistaken for a newly generated installation.
func RestoreState(l Layout, staging string) error {
	s, err := ReadState(staging)
	if err != nil {
		return err
	}
	u, err := url.Parse(s.Site)
	if err != nil {
		return errors.New("invalid recovery site")
	}
	expected, err := build(s, u)
	if err != nil {
		return errors.New("invalid recovery state")
	}
	for role, raw := range expected {
		actual, e := os.ReadFile(filepath.Join(staging, role+".json"))
		if e != nil || string(actual) != string(raw) {
			return errors.New("recovery role state mismatch")
		}
	}
	password, err := os.ReadFile(filepath.Join(staging, "postgres-password"))
	if err != nil || string(password) != s.Passwords["owner"] {
		return errors.New("recovery database state mismatch")
	}
	dirs := []struct {
		path string
		uid  int
	}{{l.Setup, -1}, {l.Web, l.WebUID}, {l.Domain, l.DomainUID}, {l.Background, l.BackgroundUID}, {l.Extractor, l.ExtractorUID}, {l.PostgresSecrets, -1}, {l.Postgres, -1}}
	for _, d := range dirs {
		if d.path == "" {
			return errors.New("missing restore volume")
		}
		info, e := os.Lstat(d.path)
		if e == nil && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
			return errors.New("invalid restore volume")
		}
		if e != nil && !os.IsNotExist(e) {
			return errors.New("restore volume unavailable")
		}
		if e == nil {
			items, e := os.ReadDir(d.path)
			if e != nil || len(items) > 0 {
				return errors.New("restore requires empty state and database volumes")
			}
		}
	}
	// All rejection checks precede the first target mutation.
	for _, d := range dirs {
		if err = privateDir(d.path, d.uid); err != nil {
			return errors.New("restore volume unavailable")
		}
	}
	for _, role := range []struct {
		name, path string
		uid        int
	}{{"web", l.Web, l.WebUID}, {"domain", l.Domain, l.DomainUID}, {"background", l.Background, l.BackgroundUID}, {"extractor", l.Extractor, l.ExtractorUID}} {
		if err = writeNew(filepath.Join(role.path, "runtime.json"), expected[role.name], role.uid); err != nil {
			return errors.New("restore state copy failed")
		}
	}
	if err = writeNew(filepath.Join(l.PostgresSecrets, "postgres-password"), password, -1); err != nil {
		return errors.New("restore state copy failed")
	}
	pending := map[string]string{"installation_id": s.InstallationID}
	recoveryDir := filepath.Join(l.Setup, "recovery")
	if err = os.Mkdir(recoveryDir, 0700); err != nil {
		return errors.New("restore staging failed")
	}
	for _, file := range []string{"news.sql", "scheduler.sql"} {
		in, e := os.Open(filepath.Join(staging, file))
		if e != nil {
			return errors.New("restore dump unavailable")
		}
		out, e := os.OpenFile(filepath.Join(recoveryDir, file), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			in.Close()
			return errors.New("restore dump copy failed")
		}
		h := sha256.New()
		_, e = io.Copy(io.MultiWriter(out, h), in)
		in.Close()
		ce := out.Close()
		if e != nil || ce != nil {
			return errors.New("restore dump copy failed")
		}
		key := "news_sha256"
		if file == "scheduler.sql" {
			key = "scheduler_sha256"
		}
		pending[key] = hex.EncodeToString(h.Sum(nil))
	}
	marker, err := json.Marshal(pending)
	if err != nil {
		return errors.New("restore staging failed")
	}
	if err = writeNew(filepath.Join(l.Setup, "recovery-pending.json"), marker, -1); err != nil {
		return errors.New("restore staging failed")
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return errors.New("restore state invalid")
	}
	return writeNew(filepath.Join(l.Setup, "installation.json"), raw, -1)
}
