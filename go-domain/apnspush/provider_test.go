package apnspush

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func fixtureCredential(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890", PrivateKey: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestSenderReusesHTTP2ClientAndCachesJWT(t *testing.T) {
	raw := fixtureCredential(t)
	c, k, e := ParseCredential(raw)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Unix(1700000000, 0)
	tokens := []string{}
	calls := 0
	client := &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		tokens = append(tokens, r.Header.Get("authorization"))
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}
	s := &Sender{Client: client, Credential: c, Key: k, Now: func() time.Time { return now }}
	for _, delta := range []time.Duration{0, 20 * time.Minute, 41 * time.Minute} {
		now = time.Unix(1700000000, 0).Add(delta)
		if _, e = s.Send(context.Background(), "sandbox", "aabb", "00000000-0000-4000-8000-000000000001", "background", "", 5, now.Add(time.Minute), []byte(`{}`)); e != nil {
			t.Fatal(e)
		}
	}
	if calls != 3 || tokens[0] != tokens[1] || tokens[1] == tokens[2] {
		t.Fatalf("JWT cache policy mismatch: calls=%d", calls)
	}
}
func TestCredentialStrictAndP256(t *testing.T) {
	raw := fixtureCredential(t)
	_, k, err := ParseCredential(raw)
	if err != nil || k.Curve != elliptic.P256() {
		t.Fatalf("valid P-256 rejected: %v", err)
	}
	bad := append(raw[:len(raw)-1], []byte(`,"other":1}`)...)
	if _, _, err = ParseCredential(bad); err == nil {
		t.Fatal("unknown property accepted")
	}
}

func TestSenderPropagatesRequestCancellation(t *testing.T) {
	c, key, err := ParseCredential(fixtureCredential(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := false
	s := &Sender{Credential: c, Key: key, Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		cancel()
		select {
		case <-r.Context().Done():
			observed = true
			return nil, r.Context().Err()
		default:
			return nil, errors.New("request context did not cancel")
		}
	})}}
	if _, err = s.Send(ctx, "sandbox", "aabb", "00000000-0000-4000-8000-000000000001", "alert", "", 5, time.Now().Add(time.Hour), []byte(`{}`)); err == nil || !observed {
		t.Fatalf("transport did not observe caller cancellation: observed=%v err=%v", observed, err)
	}
}
func TestProviderTokenSignature(t *testing.T) {
	raw := fixtureCredential(t)
	c, k, err := ParseCredential(raw)
	if err != nil {
		t.Fatal(err)
	}
	token, err := Sign(c, k, time.Unix(1700000000, 0))
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("malformed JWT")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 64 {
		t.Fatal("bad ES256 signature encoding")
	}
	hash := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(&k.PublicKey, hash[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		t.Fatal("JWT signature invalid")
	}
}

func TestSenderReturnsBoundedAPNsStatusAndTransportAmbiguity(t *testing.T) {
	raw := fixtureCredential(t)
	c, key, err := ParseCredential(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []int{200, 400, 410, 429, 500, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			client := &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
				if r.URL.Host != "api.sandbox.push.apple.com" || r.Header.Get("apns-topic") != Topic || r.Header.Get("apns-id") == "" {
					t.Fatal("incorrect APNs request routing/headers")
				}
				h := make(http.Header)
				h.Set("apns-id", "00000000-0000-4000-8000-000000000001")
				if status == 429 {
					h.Set("retry-after", "3")
				}
				return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(`{"reason":"fixture"}`)), Request: r}, nil
			})}
			s := &Sender{Client: client, Credential: c, Key: key}
			result, err := s.Send(context.Background(), "sandbox", "aabb", "00000000-0000-4000-8000-000000000001", "alert", "", 5, time.Now().Add(time.Hour), []byte(`{}`))
			if err != nil || result.Status != status || result.APNsID == "" || result.Reason != "fixture" {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if status == 429 && result.RetryAfter != 3*time.Second {
				t.Fatalf("retry-after=%v", result.RetryAfter)
			}
		})
	}
	s := &Sender{Client: &http.Client{Transport: rtFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("timeout") })}, Credential: c, Key: key}
	if _, err = s.Send(context.Background(), "sandbox", "aabb", "00000000-0000-4000-8000-000000000001", "alert", "", 5, time.Now().Add(time.Hour), []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("transport ambiguity not reported: %v", err)
	}
}
