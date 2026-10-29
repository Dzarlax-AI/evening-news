package recovery

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestBundleRoundTripAndFailures(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	if err := os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	secret := []byte("synthetic-private-installation-state")
	for name := range names {
		if err := os.WriteFile(filepath.Join(stage, name), secret, 0600); err != nil {
			t.Fatal(err)
		}
	}
	out := filepath.Join(root, "fixture.age")
	pass := "synthetic-recovery-passphrase"
	if err := Write(out, "fixture-installation", stage, pass); err != nil {
		t.Fatal(err)
	}
	encrypted, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encrypted, secret) {
		t.Fatal("plaintext in encrypted bundle")
	}
	dir, m, err := Read(out, pass, root)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	if m.InstallationID != "fixture-installation" {
		t.Fatal("identity changed")
	}
	for name := range names {
		got, e := os.ReadFile(filepath.Join(dir, name))
		if e != nil || !bytes.Equal(got, secret) {
			t.Fatal("file did not round trip")
		}
	}
	if err := Write(out, "fixture-installation", stage, pass); err == nil {
		t.Fatal("overwrote existing bundle")
	}
	for _, tc := range []struct {
		name string
		data []byte
		pass string
	}{
		{"wrong password", encrypted, "another-synthetic-passphrase"},
		{"truncation", encrypted[:len(encrypted)-1], pass},
		{"tamper", append(append([]byte{}, encrypted[:len(encrypted)-1]...), encrypted[len(encrypted)-1]^1), pass},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(root, tc.name+".age")
			if e := os.WriteFile(p, tc.data, 0600); e != nil {
				t.Fatal(e)
			}
			d, _, e := Read(p, tc.pass, root)
			if e == nil {
				t.Fatal("accepted corrupt bundle")
			}
			if _, e = os.Stat(d); !os.IsNotExist(e) {
				t.Fatal("left decrypted staging after error")
			}
		})
	}
}
