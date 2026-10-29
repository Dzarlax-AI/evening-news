// Package recovery handles offline, passphrase-encrypted installation bundles.
// No cleartext secrets are written outside a private, disposable staging directory.
package recovery

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"

	"filippo.io/age"
)

const maxBundle = 512 << 20
const maxEntry = 256 << 20

var ErrInvalid = errors.New("invalid or unreadable recovery bundle")

type Entry struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}
type Manifest struct {
	Version        int              `json:"version"`
	InstallationID string           `json:"installation_id"`
	Entries        map[string]Entry `json:"entries"`
}

var names = map[string]bool{"installation.json": true, "web.json": true, "domain.json": true, "background.json": true, "extractor.json": true, "postgres-password": true, "news.sql": true, "scheduler.sql": true}

// Write encrypts the complete tar/gzip stream using the interoperable age
// passphrase format. It refuses overwrite and removes incomplete output.
func Write(output, installationID, staging, passphrase string) (returnedErr error) {
	if len(passphrase) < 12 || len(passphrase) > 1024 || installationID == "" {
		return ErrInvalid
	}
	manifest := Manifest{Version: 1, InstallationID: installationID, Entries: map[string]Entry{}}
	keys := make([]string, 0, len(names))
	total := int64(0)
	for name := range names {
		p := filepath.Join(staging, name)
		info, err := os.Lstat(p)
		if err != nil || !info.Mode().IsRegular() || info.Size() > maxEntry {
			return ErrInvalid
		}
		f, err := os.Open(p)
		if err != nil {
			return ErrInvalid
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return ErrInvalid
		}
		total += info.Size()
		if total > maxBundle {
			return ErrInvalid
		}
		manifest.Entries[name] = Entry{info.Size(), hex.EncodeToString(h.Sum(nil))}
		keys = append(keys, name)
	}
	sort.Strings(keys)
	raw, err := json.Marshal(manifest)
	if err != nil {
		return ErrInvalid
	}
	if err = os.WriteFile(filepath.Join(staging, "bundle.json"), raw, 0600); err != nil {
		return ErrInvalid
	}
	keys = append([]string{"bundle.json"}, keys...)
	recipient, err := age.NewScryptRecipient(passphrase)
	if err != nil {
		return ErrInvalid
	}
	out, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return ErrInvalid
	}
	defer func() {
		out.Close()
		if returnedErr != nil {
			os.Remove(output)
		}
	}()
	encrypted, err := age.Encrypt(out, recipient)
	if err != nil {
		return ErrInvalid
	}
	gz := gzip.NewWriter(encrypted)
	archive := tar.NewWriter(gz)
	for _, name := range keys {
		info, err := os.Stat(filepath.Join(staging, name))
		if err != nil {
			return ErrInvalid
		}
		if err = archive.WriteHeader(&tar.Header{Name: name, Mode: 0600, Size: info.Size(), Typeflag: tar.TypeReg}); err != nil {
			return ErrInvalid
		}
		f, err := os.Open(filepath.Join(staging, name))
		if err != nil {
			return ErrInvalid
		}
		_, err = io.Copy(archive, f)
		f.Close()
		if err != nil {
			return ErrInvalid
		}
	}
	for _, close := range []func() error{archive.Close, gz.Close, encrypted.Close, out.Sync, out.Close} {
		if err = close(); err != nil {
			return ErrInvalid
		}
	}
	return nil
}

// Read authenticates the entire encrypted stream before extracting anything.
// The caller owns the returned private directory and must remove it afterward.
func Read(input, passphrase, parent string) (dir string, manifest Manifest, returnedErr error) {
	if len(passphrase) < 12 || len(passphrase) > 1024 {
		return "", manifest, ErrInvalid
	}
	dir, err := os.MkdirTemp(parent, "news-recovery-")
	if err != nil {
		return "", manifest, ErrInvalid
	}
	defer func() {
		if returnedErr != nil {
			os.RemoveAll(dir)
		}
	}()
	if err = os.Chmod(dir, 0700); err != nil {
		return dir, manifest, ErrInvalid
	}
	in, err := os.Open(input)
	if err != nil {
		return dir, manifest, ErrInvalid
	}
	defer in.Close()
	identity, err := age.NewScryptIdentity(passphrase)
	if err != nil {
		return dir, manifest, ErrInvalid
	}
	identity.SetMaxWorkFactor(18)
	plain, err := age.Decrypt(io.LimitReader(in, maxBundle+1), identity)
	if err != nil {
		return dir, manifest, ErrInvalid
	}
	temp, err := os.OpenFile(filepath.Join(dir, "authenticated.gz"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return dir, manifest, ErrInvalid
	}
	defer temp.Close()
	n, err := io.Copy(temp, io.LimitReader(plain, maxBundle+1))
	if err != nil || n > maxBundle {
		return dir, manifest, ErrInvalid
	}
	if _, err = temp.Seek(0, 0); err != nil {
		return dir, manifest, ErrInvalid
	}
	gz, err := gzip.NewReader(temp)
	if err != nil {
		return dir, manifest, ErrInvalid
	}
	defer gz.Close()
	archive := tar.NewReader(gz)
	found := map[string]Entry{}
	total := int64(0)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return dir, manifest, ErrInvalid
		}
		if header.Typeflag != tar.TypeReg || (!names[header.Name] && header.Name != "bundle.json") || header.Size < 0 || header.Size > maxEntry {
			return dir, manifest, ErrInvalid
		}
		if _, ok := found[header.Name]; ok {
			return dir, manifest, ErrInvalid
		}
		total += header.Size
		if total > maxBundle {
			return dir, manifest, ErrInvalid
		}
		if header.Name == "bundle.json" && header.Size > 64<<10 {
			return dir, manifest, ErrInvalid
		}
		f, err := os.OpenFile(filepath.Join(dir, header.Name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return dir, manifest, ErrInvalid
		}
		h := sha256.New()
		n, err := io.Copy(io.MultiWriter(f, h), archive)
		ce := f.Close()
		if err != nil || ce != nil || n != header.Size {
			return dir, manifest, ErrInvalid
		}
		found[header.Name] = Entry{n, hex.EncodeToString(h.Sum(nil))}
	}
	// Finish the compression stream and validate its checksum/trailer too.
	if _, err = io.Copy(io.Discard, gz); err != nil {
		return dir, manifest, ErrInvalid
	}
	raw, err := os.ReadFile(filepath.Join(dir, "bundle.json"))
	if err != nil || json.Unmarshal(raw, &manifest) != nil || manifest.Version != 1 || manifest.InstallationID == "" || len(manifest.Entries) != len(names) || len(found) != len(names)+1 {
		return dir, manifest, ErrInvalid
	}
	for name := range names {
		if expected, ok := manifest.Entries[name]; !ok || expected != found[name] {
			return dir, manifest, ErrInvalid
		}
	}
	if err = temp.Close(); err != nil {
		return dir, manifest, ErrInvalid
	}
	os.Remove(filepath.Join(dir, "authenticated.gz"))
	return dir, manifest, nil
}
