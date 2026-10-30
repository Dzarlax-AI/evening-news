package recovery_test

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
)

// Rehearse the published standard tar/age operator steps with synthetic data.
// This is deliberately not a new newsctl command or production archive reader.
func operatorEnvelopeRoundTrip(t *testing.T, dir string, installation string, key []byte) {
	t.Helper()
	for _, role := range []string{"web", "domain", "extractor", "egress"} {
		values := map[string]string{"FIXTURE": "synthetic-only"}
		if role == "domain" {
			values["NEWS_CREDENTIAL_KEY"] = base64.RawStdEncoding.EncodeToString(key)
			values["NEWS_CREDENTIAL_KEY_ID"] = "fixture-key"
		}
		b, _ := json.Marshal(map[string]any{"version": 1, "service": role, "installation_id": installation, "values": values})
		if os.WriteFile(filepath.Join(dir, role+".json"), b, 0600) != nil {
			t.Fatal("role fixture failed")
		}
	}
	names := []string{"manifest.json", "settings.json", "schedules.json", "web.json", "domain.json", "background.json", "extractor.json", "egress.json", "adoption-committed.json", "deployment.json", "roles.json", "RESTORE.md"}
	type entry struct {
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	}
	type envelopeManifest struct {
		InstallationID string           `json:"installation_id"`
		Profile        string           `json:"profile"`
		SchemaVersion  int              `json:"schema_version"`
		Format         string           `json:"format"`
		Entries        map[string]entry `json:"entries"`
	}
	manifest := envelopeManifest{InstallationID: installation, Profile: "adopted-native-external", SchemaVersion: 16, Format: "settings-recovery-v1", Entries: map[string]entry{}}
	for _, name := range names[1:] {
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if os.WriteFile(path, []byte("synthetic-only-"+name), 0600) != nil {
				t.Fatal("envelope fixture creation failed")
			}
		}
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal("envelope fixture read failed")
		}
		h := sha256.Sum256(b)
		manifest.Entries[name] = entry{int64(len(b)), hex.EncodeToString(h[:])}
	}
	b, _ := json.Marshal(manifest)
	if os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600) != nil {
		t.Fatal("manifest fixture failed")
	}
	args := append([]string{"-cf", "payload.tar"}, names...)
	cmd := exec.Command("tar", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "COPYFILE_DISABLE=1")
	if cmd.Run() != nil {
		t.Fatal("documented tar command failed")
	}
	cmd = exec.Command("age-keygen", "-o", filepath.Join(dir, "identity"))
	if cmd.Run() != nil {
		t.Fatal("age-keygen required for isolated envelope rehearsal")
	}
	pub, err := exec.Command("age-keygen", "-y", filepath.Join(dir, "identity")).Output()
	if err != nil {
		t.Fatal("public recipient failed")
	}
	encrypted := filepath.Join(dir, "packet.age")
	f, err := os.OpenFile(encrypted, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd = exec.Command("age", "-r", string(bytes.TrimSpace(pub)), filepath.Join(dir, "payload.tar"))
	cmd.Stdout = f
	err = cmd.Run()
	f.Close()
	if err != nil {
		t.Fatal("documented age encryption failed")
	}
	plain, err := exec.Command("age", "-d", "-i", filepath.Join(dir, "identity"), encrypted).Output()
	if err != nil {
		t.Fatal("age authentication failed")
	}
	original, _ := os.ReadFile(filepath.Join(dir, "payload.tar"))
	if !bytes.Equal(plain, original) {
		t.Fatal("tar roundtrip mismatch")
	}
	validate := func(payload []byte) error {
		// These are the exact checks required before extraction in the operator guide.
		reader := tar.NewReader(bytes.NewReader(payload))
		files := map[string][]byte{}
		total := int64(0)
		allowed := map[string]bool{}
		for _, n := range names {
			allowed[n] = true
		}
		for {
			h, e := reader.Next()
			if e == io.EOF {
				break
			}
			if e != nil {
				return e
			}
			if !allowed[h.Name] || (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA) || h.Size < 0 || h.Size > 16<<20 {
				return fmt.Errorf("entry rejected")
			}
			if _, ok := files[h.Name]; ok {
				return fmt.Errorf("duplicate entry")
			}
			total += h.Size
			if total > 32<<20 {
				return fmt.Errorf("size budget")
			}
			data, e := io.ReadAll(io.LimitReader(reader, 16<<20+1))
			if e != nil || int64(len(data)) != h.Size {
				return fmt.Errorf("truncated entry")
			}
			files[h.Name] = data
		}
		if len(files) != len(names) {
			return fmt.Errorf("missing entry")
		}
		var m envelopeManifest
		if json.Unmarshal(files["manifest.json"], &m) != nil || m.Format != "settings-recovery-v1" || m.InstallationID != installation || m.Profile != "adopted-native-external" || m.SchemaVersion != 16 || len(m.Entries) != len(names)-1 {
			return fmt.Errorf("manifest rejected")
		}
		for _, n := range names[1:] {
			h := sha256.Sum256(files[n])
			e, ok := m.Entries[n]
			if !ok || e.Size != int64(len(files[n])) || e.SHA256 != hex.EncodeToString(h[:]) {
				return fmt.Errorf("checksum rejected")
			}
		}
		var settings struct {
			Identity struct {
				InstallationID string `json:"installation_id"`
			} `json:"identity"`
			Credentials []struct {
				Provider   string  `json:"provider"`
				KeyID      *string `json:"key_id"`
				Version    *int    `json:"envelope_version"`
				Nonce      *string `json:"nonce"`
				Ciphertext *string `json:"ciphertext"`
			} `json:"integration_credentials"`
		}
		if json.Unmarshal(files["settings.json"], &settings) != nil || settings.Identity.InstallationID != installation {
			return fmt.Errorf("installation mismatch")
		}
		for _, role := range []string{"web", "domain", "background", "extractor", "egress"} {
			var v struct {
				Service        string            `json:"service"`
				InstallationID string            `json:"installation_id"`
				Values         map[string]string `json:"values"`
			}
			if json.Unmarshal(files[role+".json"], &v) != nil || v.Service != role || v.InstallationID != installation {
				return fmt.Errorf("role identity mismatch")
			}
			if role == "domain" {
				k, e := base64.RawStdEncoding.DecodeString(v.Values["NEWS_CREDENTIAL_KEY"])
				if e != nil || len(k) != 32 {
					return fmt.Errorf("credential key missing")
				}
				for _, c := range settings.Credentials {
					// A deleted provider key retains its revision with a null envelope.
					if c.Version == nil && c.KeyID == nil && c.Nonce == nil && c.Ciphertext == nil {
						continue
					}
					if c.Version == nil || c.KeyID == nil || c.Nonce == nil || c.Ciphertext == nil {
						return fmt.Errorf("partial credential envelope")
					}
					nonce, e := hex.DecodeString(strings.TrimPrefix(*c.Nonce, `\x`))
					if e != nil {
						return e
					}
					cipher, e := hex.DecodeString(strings.TrimPrefix(*c.Ciphertext, `\x`))
					if e != nil {
						return e
					}
					if _, e = credentialstore.Open(k, v.Values["NEWS_CREDENTIAL_KEY_ID"], installation, c.Provider, credentialstore.Envelope{Version: *c.Version, KeyID: *c.KeyID, Nonce: nonce, Ciphertext: cipher}); e != nil {
						return fmt.Errorf("credential authentication failed")
					}
				}
			}
		}
		return nil
	}
	if err = validate(plain); err != nil {
		t.Fatalf("documented envelope checks failed: %v", err)
	}
	// Neither tampered bytes nor a different recovery identity authenticate.
	cipher, _ := os.ReadFile(encrypted)
	cipher[len(cipher)/2] ^= 1
	bad := filepath.Join(dir, "bad.age")
	_ = os.WriteFile(bad, cipher, 0600)
	if _, err = exec.Command("age", "-d", "-i", filepath.Join(dir, "identity"), bad).Output(); err == nil {
		t.Fatal("tampered ciphertext accepted")
	}
	cmd = exec.Command("age-keygen", "-o", filepath.Join(dir, "other-identity"))
	if cmd.Run() != nil {
		t.Fatal("second fixture key failed")
	}
	if _, err = exec.Command("age", "-d", "-i", filepath.Join(dir, "other-identity"), encrypted).Output(); err == nil {
		t.Fatal("wrong recovery key accepted")
	}
	for _, tc := range []struct {
		name string
		typ  byte
	}{{"../escape", tar.TypeReg}, {"domain.json", tar.TypeSymlink}} {
		var buf bytes.Buffer
		w := tar.NewWriter(&buf)
		_ = w.WriteHeader(&tar.Header{Name: tc.name, Typeflag: tc.typ, Linkname: "../escape"})
		_ = w.Close()
		if validate(buf.Bytes()) == nil {
			t.Fatal("unsafe tar accepted")
		}
	}
	// A structurally sound tar with a modified key file fails its manifest hash.
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	r := tar.NewReader(bytes.NewReader(plain))
	for {
		h, e := r.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal("fixture tar failed")
		}
		data, _ := io.ReadAll(r)
		if h.Name == "domain.json" {
			data = append(data, 'x')
			h.Size = int64(len(data))
		}
		_ = w.WriteHeader(h)
		_, _ = w.Write(data)
	}
	_ = w.Close()
	if validate(buf.Bytes()) == nil {
		t.Fatal("mismatched key file hash accepted")
	}
}
