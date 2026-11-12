package credentialstore

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"strings"
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

func TestAPNsCredentialRequiresStrictP256Object(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	priv := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	valid, _ := json.Marshal(map[string]string{"key_id": "ABCDEFGHIJ", "team_id": "1234567890", "private_key": priv})
	if !validAPNsCredential(valid) {
		t.Fatal("valid P-256 key rejected")
	}
	bad := []string{strings.Replace(string(valid), `"key_id":"ABCDEFGHIJ"`, `"key_id":"ABCDEFGHIJ","key_id":"KLMNOPQRST"`, 1), strings.Replace(string(valid), `"team_id":"1234567890"`, `"team_id":null`, 1), strings.TrimSuffix(string(valid), "}") + `,"extra":true}`, string(valid) + " trailing", strings.Replace(string(valid), "ABCDEFGHIJ", "invalid-id", 1)}
	for _, raw := range bad {
		if validAPNsCredential([]byte(raw)) {
			t.Fatalf("accepted invalid APNs key envelope")
		}
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
