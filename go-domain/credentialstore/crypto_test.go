package credentialstore

import (
	"bytes"
	"testing"
)

func TestSealOpenAndBindings(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	secret := []byte("test-only-secret")
	a, err := Seal(key, "key-1", "install-1", "gemini", secret)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Seal(key, "key-1", "install-1", "gemini", secret)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(a.Nonce, b.Nonce) || bytes.Contains(a.Ciphertext, secret) {
		t.Fatal("nonce reused or plaintext exposed")
	}
	p, err := Open(key, "key-1", "install-1", "gemini", a)
	if err != nil || !bytes.Equal(p, secret) {
		t.Fatal("roundtrip failed")
	}
	for _, tc := range []struct {
		k                          []byte
		id, installation, provider string
		env                        Envelope
	}{
		{bytes.Repeat([]byte{8}, 32), "key-1", "install-1", "gemini", a},
		{key, "key-2", "install-1", "gemini", a}, {key, "key-1", "install-2", "gemini", a}, {key, "key-1", "install-1", "openai", a},
	} {
		if _, err := Open(tc.k, tc.id, tc.installation, tc.provider, tc.env); err == nil {
			t.Fatal("accepted incorrect binding")
		}
	}
	a.Ciphertext[0] ^= 1
	if _, err := Open(key, "key-1", "install-1", "gemini", a); err == nil {
		t.Fatal("accepted altered ciphertext")
	}
}
func TestRejectInvalidKeyAndNonce(t *testing.T) {
	if _, err := Seal([]byte("short"), "key", "installation", "provider", nil); err == nil {
		t.Fatal("accepted key")
	}
	if _, err := Open(make([]byte, 32), "key", "installation", "provider", Envelope{Version: 1, KeyID: "key", Nonce: []byte{1}}); err == nil {
		t.Fatal("accepted nonce")
	}
}

func TestViberProviderAllowed(t *testing.T) {
	if !validProvider("viber") || validProvider("viber-other") {
		t.Fatal("provider allowlist")
	}
}
