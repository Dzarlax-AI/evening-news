package apnspush

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

const Topic = "com.dzarlax.NewsFeed"

var ErrCredential = errors.New("invalid APNs credential")

type Credential struct {
	KeyID      string `json:"key_id"`
	TeamID     string `json:"team_id"`
	PrivateKey string `json:"private_key"`
}

func ParseCredential(raw []byte) (Credential, *ecdsa.PrivateKey, error) {
	var c Credential
	if len(raw) == 0 || len(raw) > 8192 {
		return c, nil, ErrCredential
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil || hasDuplicateJSONKey(raw) || len(object) != 3 {
		return c, nil, ErrCredential
	}
	for _, key := range []string{"key_id", "team_id", "private_key"} {
		v, ok := object[key]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return c, nil, ErrCredential
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF || !validAppleID(c.KeyID) || !validAppleID(c.TeamID) {
		return Credential{}, nil, ErrCredential
	}
	b, rest := pem.Decode([]byte(c.PrivateKey))
	if b == nil || b.Type != "PRIVATE KEY" || len(b.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 {
		return Credential{}, nil, ErrCredential
	}
	k, err := x509ParsePKCS8(b.Bytes)
	if err != nil || k.Curve != elliptic.P256() || k.D == nil || k.D.Sign() <= 0 || k.D.Cmp(k.Params().N) >= 0 {
		return Credential{}, nil, ErrCredential
	}
	return c, k, nil
}

// Sign creates a short lived APNs ES256 provider token. Apple's JWT uses the
// fixed-width raw R||S signature representation.
func Sign(c Credential, k *ecdsa.PrivateKey, now time.Time) (string, error) {
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]string{"alg": "ES256", "kid": c.KeyID})
	h := enc.EncodeToString(header)
	p, _ := json.Marshal(map[string]any{"iss": c.TeamID, "iat": now.Unix()})
	input := h + "." + enc.EncodeToString(p)
	sum := sha256.Sum256([]byte(input))
	r, s, err := ecdsa.Sign(rand.Reader, k, sum[:])
	if err != nil {
		return "", err
	}
	wide := 32
	sig := make([]byte, wide*2)
	r.FillBytes(sig[:wide])
	s.FillBytes(sig[wide:])
	return input + "." + enc.EncodeToString(sig), nil
}

func validAppleID(v string) bool {
	if len(v) != 10 {
		return false
	}
	for _, c := range v {
		if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

type Sender struct {
	mu         sync.Mutex
	Client     *http.Client
	Credential Credential
	Key        *ecdsa.PrivateKey
	Now        func() time.Time
	jwt        string
	jwtAt      time.Time
}

func (s *Sender) CloseIdleConnections() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Client != nil {
		s.Client.CloseIdleConnections()
	}
}

type Result struct {
	Status     int
	APNsID     string
	RetryAfter time.Duration
	Reason     string
}

func (s *Sender) Send(environment, token, requestID, pushType, collapse string, priority int, expiration time.Time, payload []byte) (Result, error) {
	var out Result
	if s == nil || s.Key == nil || (environment != "sandbox" && environment != "production") || !validToken(token) {
		return out, ErrCredential
	}
	base := "https://api.push.apple.com"
	if environment == "sandbox" {
		base = "https://api.sandbox.push.apple.com"
	}
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	client, jwt, err := s.transportAndJWT(now)
	if err != nil {
		return out, err
	}
	req, err := http.NewRequest(http.MethodPost, base+"/3/device/"+token, bytes.NewReader(payload))
	if err != nil {
		return out, errors.New("APNs request unavailable")
	}
	req.Header.Set("authorization", "bearer "+jwt)
	req.Header.Set("apns-topic", Topic)
	req.Header.Set("apns-push-type", pushType)
	req.Header.Set("apns-priority", strconv.Itoa(priority))
	req.Header.Set("apns-id", requestID)
	if collapse != "" {
		req.Header.Set("apns-collapse-id", collapse)
	}
	if !expiration.IsZero() {
		req.Header.Set("apns-expiration", strconv.FormatInt(expiration.Unix(), 10))
	}
	req.Header.Set("content-type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return out, errors.New("APNs transport outcome unknown")
	}
	defer resp.Body.Close()
	out.Status = resp.StatusCode
	out.APNsID = resp.Header.Get("apns-id")
	if v := resp.Header.Get("retry-after"); v != "" {
		if sec, e := strconv.Atoi(v); e == nil && sec >= 0 && sec <= 3600 {
			out.RetryAfter = time.Duration(sec) * time.Second
		}
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	var reason struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(body, &reason)
	out.Reason = reason.Reason
	return out, nil
}

func (s *Sender) transportAndJWT(now time.Time) (*http.Client, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Client == nil {
		s.Client = &http.Client{Transport: &http2.Transport{IdleConnTimeout: 90 * time.Second}, Timeout: 15 * time.Second}
	}
	if s.jwt == "" || now.Sub(s.jwtAt) >= 40*time.Minute || now.Before(s.jwtAt) {
		token, err := Sign(s.Credential, s.Key, now)
		if err != nil {
			return nil, "", err
		}
		s.jwt = token
		s.jwtAt = now
	}
	return s.Client, s.jwt, nil
}
func validToken(s string) bool {
	if len(s) < 2 || len(s) > 1024 || len(s)%2 != 0 {
		return false
	}
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// x509ParsePKCS8 is kept local so callers receive one generic validation error.
func x509ParsePKCS8(der []byte) (*ecdsa.PrivateKey, error) {
	k, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, err
	}
	e, ok := k.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("not EC")
	}
	return e, nil
}
