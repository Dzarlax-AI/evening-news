// Package credentialstore protects integration credentials at rest.
package credentialstore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
)

const EnvelopeVersion = 1

// Envelope contains no plaintext. KeyID identifies the key needed to decrypt it.
type Envelope struct {
	Version    int
	KeyID      string
	Nonce      []byte
	Ciphertext []byte
}

// Seal binds ciphertext to an installation, provider and key version.
func Seal(key []byte, keyID, installationID, provider string, plaintext []byte) (Envelope, error) {
	var zero Envelope
	aead, err := newAEAD(key, keyID, installationID, provider)
	if err != nil {
		return zero, err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return zero, errors.New("credential nonce generation failed")
	}
	return Envelope{Version: EnvelopeVersion, KeyID: keyID, Nonce: nonce, Ciphertext: aead.Seal(nil, nonce, plaintext, aad(installationID, provider, keyID))}, nil
}

// Open authenticates every binding and refuses incompatible envelope versions.
func Open(key []byte, keyID, installationID, provider string, envelope Envelope) ([]byte, error) {
	if envelope.Version != EnvelopeVersion || envelope.KeyID != keyID {
		return nil, errors.New("credential envelope identity mismatch")
	}
	aead, err := newAEAD(key, keyID, installationID, provider)
	if err != nil {
		return nil, err
	}
	if len(envelope.Nonce) != aead.NonceSize() {
		return nil, errors.New("invalid credential envelope")
	}
	plaintext, err := aead.Open(nil, envelope.Nonce, envelope.Ciphertext, aad(installationID, provider, keyID))
	if err != nil {
		return nil, errors.New("credential authentication failed")
	}
	return plaintext, nil
}
func newAEAD(key []byte, keyID, installationID, provider string) (cipher.AEAD, error) {
	if len(key) != 32 || keyID == "" || installationID == "" || provider == "" {
		return nil, errors.New("invalid credential encryption configuration")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("invalid credential encryption key")
	}
	return cipher.NewGCM(block)
}
func aad(parts ...string) []byte {
	out := []byte("evening-news-credential-v1")
	for _, s := range parts {
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(s)))
		out = append(out, n[:]...)
		out = append(out, s...)
	}
	return out
}
