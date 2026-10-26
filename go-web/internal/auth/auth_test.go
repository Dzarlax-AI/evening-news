package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2030, 3, 17, 12, 0, 0, 0, time.UTC)

func testService() *Service {
	s := New(Config{Username: "админ", Password: "пароль", JWTSecret: "test-secret", Development: true})
	s.now = func() time.Time { return testNow }
	return s
}
func req(method, target string, headers map[string]string) *http.Request {
	r := httptest.NewRequest(method, target, nil)
	r.RemoteAddr = "192.0.2.9:1234"
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}
func basic(user, pass string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
}

func TestPythonMintedJWTWorksInCookieAndBearer(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "tests", "fixtures", "web-contract-v1", "auth-python-jwt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct{ Secret, Token string }
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	s := New(Config{JWTSecret: fixture.Secret})
	s.now = func() time.Time { return time.Unix(1899999500, 0).UTC() }
	for name, r := range map[string]*http.Request{"cookie": req("GET", "/", map[string]string{"Cookie": AdminCookie + "=" + fixture.Token}), "bearer": req("GET", "/", map[string]string{"Authorization": "Bearer " + fixture.Token})} {
		p, err := s.API(r)
		if err != nil || p.Username != "fixture-admin" || p.Level != LevelAdmin {
			t.Errorf("%s principal=%+v err=%v", name, p, err)
		}
	}
}

func TestJWTRejectsMalformedWrongAlgorithmInvalidClaimsAndExpired(t *testing.T) {
	s := testService()
	tests := map[string]string{
		"malformed":     "not.a.jwt",
		"wrong-alg":     jwtFor(t, s, "none", map[string]any{"sub": "u", "level": "admin", "exp": testNow.Add(time.Hour).Unix()}),
		"missing-sub":   signed(t, s, map[string]any{"level": "admin", "exp": testNow.Add(time.Hour).Unix()}),
		"missing-level": signed(t, s, map[string]any{"sub": "u", "exp": testNow.Add(time.Hour).Unix()}),
		"missing-exp":   signed(t, s, map[string]any{"sub": "u", "level": "admin"}),
		"expired":       signed(t, s, map[string]any{"sub": "u", "level": "admin", "exp": testNow.Unix() - 1}),
		"future-iat":    signed(t, s, map[string]any{"sub": "u", "level": "admin", "iat": testNow.Add(time.Second).Unix(), "exp": testNow.Add(time.Hour).Unix()}),
		"future-nbf":    signed(t, s, map[string]any{"sub": "u", "level": "admin", "nbf": testNow.Add(time.Second).Unix(), "exp": testNow.Add(time.Hour).Unix()}),
	}
	for name, token := range tests {
		t.Run(name, func(t *testing.T) {
			if _, ok := s.verify(token); ok {
				t.Fatal("accepted invalid token")
			}
		})
	}
	if _, ok := s.verify(strings.Repeat("x", maxTokenSize+1)); ok {
		t.Fatal("accepted oversized token")
	}
}

func signed(t *testing.T, s *Service, claims map[string]any) string {
	t.Helper()
	token, err := s.sign(claims)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func jwtFor(t *testing.T, s *Service, alg string, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	head, _ := json.Marshal(map[string]string{"alg": alg, "typ": "JWT"})
	enc := base64.RawURLEncoding.EncodeToString
	input := enc(head) + "." + enc(payload)
	mac := hmacFor([]byte("test-secret"), input)
	return input + "." + enc(mac)
}
func hmacFor(secret []byte, message string) []byte {
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(message))
	return mac.Sum(nil)
}

func TestAPIAuthSourcesAndAdminRoleBoundaries(t *testing.T) {
	s := testService()
	admin, _ := s.Mint("root", LevelAdmin)
	reader, _ := s.Mint("reader", LevelAPIRead)
	tests := []struct {
		name string
		r    *http.Request
		ok   bool
		user string
	}{
		{"basic-unicode", req("GET", "/", map[string]string{"Authorization": basic("админ", "пароль")}), true, "админ"},
		{"bearer", req("GET", "/", map[string]string{"Authorization": "Bearer " + admin}), true, "root"},
		{"cookie", req("GET", "/", map[string]string{"Cookie": AdminCookie + "=" + admin}), true, "root"},
		{"invalid-bearer-falls-back-cookie", req("GET", "/", map[string]string{"Authorization": "Bearer nope", "Cookie": AdminCookie + "=" + admin}), true, "root"},
		{"reader-authenticates", req("GET", "/", map[string]string{"Authorization": "Bearer " + reader}), true, "reader"},
		{"forward-header-ignored", req("GET", "/", map[string]string{"X-authentik-username": "proxy-user"}), false, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := s.API(tc.r)
			if (err == nil) != tc.ok {
				t.Fatalf("principal=%+v err=%v", p, err)
			}
			if tc.ok && p.Username != tc.user {
				t.Fatalf("user=%q", p.Username)
			}
		})
	}
	if p, err := s.API(req("GET", "/", map[string]string{"Authorization": "Bearer " + reader})); err != nil || p.Level == LevelAdmin {
		t.Fatalf("reader unexpectedly admin: %+v %v", p, err)
	}
}

func TestAdminPageTrustsOnlyConfiguredForwardAuthOrBasic(t *testing.T) {
	s := testService()
	tok, _ := s.Mint("admin", LevelAdmin)
	if _, err := s.AdminPage(req("GET", "/", map[string]string{"Cookie": AdminCookie + "=" + tok})); err == nil {
		t.Fatal("admin page accepted JWT cookie")
	}
	if _, err := s.AdminPage(req("GET", "/", map[string]string{"Authorization": basic("админ", "пароль")})); err != nil {
		t.Fatal(err)
	}
	forward := New(Config{TrustForwardAuth: true})
	p, err := forward.AdminPage(req("GET", "/", map[string]string{"X-authentik-username": "proxy-user"}))
	if err != nil || p.Username != "proxy-user" {
		t.Fatalf("principal=%+v err=%v", p, err)
	}
	if _, err = New(Config{}).AdminPage(req("GET", "/", nil)); err == nil || err.(*AuthError).Status != 501 {
		t.Fatalf("expected unconfigured error, got %v", err)
	}
	if _, err = New(Config{Username: "", Password: ""}).API(req("GET", "/", map[string]string{"Authorization": basic("", "")})); err == nil {
		t.Fatal("accepted Basic credentials with empty configuration")
	}
}

func TestPublicAdminRequiresAdminCookieAndTrustedHeader(t *testing.T) {
	s := testService()
	reader, _ := s.Mint("reader", LevelAPIRead)
	admin, _ := s.Mint("root", LevelAdmin)
	for _, tc := range []struct {
		name, token string
		want        bool
	}{{"admin", admin, true}, {"reader", reader, false}} {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := s.PublicAdmin(req("GET", "/", map[string]string{"Cookie": AdminCookie + "=" + tc.token}))
			if ok != tc.want {
				t.Fatalf("principal=%+v ok=%v", p, ok)
			}
		})
	}
}

func TestCookieAttributesAndFallbackSigningSecret(t *testing.T) {
	s := New(Config{Password: "pw", Development: true})
	s.now = func() time.Time { return testNow }
	c, err := s.AdminCookie("u")
	if err != nil {
		t.Fatal(err)
	}
	if c.Name != AdminCookie || !c.HttpOnly || c.Secure || c.SameSite != http.SameSiteLaxMode || c.Path != "/" || c.MaxAge != 86400 {
		t.Fatalf("cookie=%+v", c)
	}
	if p, ok := s.verify(c.Value); !ok || p.Username != "u" {
		t.Fatalf("principal=%+v ok=%v", p, ok)
	}
	prod, _ := New(Config{Password: "pw"}).AdminCookie("u")
	if !prod.Secure {
		t.Fatal("production cookie is not secure")
	}
}

func TestAuthHTTPLoginStatusAndLockout(t *testing.T) {
	s := testService()
	mux := http.NewServeMux()
	s.Register(mux)
	call := func(headers map[string]string) (int, http.Header, []byte) {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req("POST", "/auth/login", headers))
		return w.Code, w.Header(), w.Body.Bytes()
	}
	status, headers, body := call(nil)
	if status != 401 || headers.Get("WWW-Authenticate") != "Basic" || !strings.Contains(string(body), "Not authenticated") {
		t.Fatalf("%d %v %s", status, headers, body)
	}
	status, _, body = call(map[string]string{"Authorization": basic("админ", "bad")})
	if status != 401 || !strings.Contains(string(body), "Invalid credentials") {
		t.Fatalf("%d %s", status, body)
	}
	status, _, body = call(map[string]string{"Authorization": basic("админ", "пароль")})
	if status != 200 {
		t.Fatalf("valid login status=%d body=%s", status, body)
	}
	var token tokenResponse
	if err := json.Unmarshal(body, &token); err != nil || token.Level != LevelAdmin || token.ExpiresIn != 86400 {
		t.Fatalf("response=%s err=%v", body, err)
	}
	meRequest := req("GET", "/auth/me", map[string]string{"Authorization": basic("админ", "пароль")})
	meResponse := httptest.NewRecorder()
	mux.ServeHTTP(meResponse, meRequest)
	var me map[string]string
	if meResponse.Code != 200 || json.Unmarshal(meResponse.Body.Bytes(), &me) != nil || !strings.HasSuffix(me["expires_at"], "+00:00") {
		t.Fatalf("basic /auth/me response=%d %s", meResponse.Code, meResponse.Body.String())
	}
	call(nil)
	call(nil)
	status, _, _ = call(nil)
	if status != 429 {
		t.Fatalf("rate-limit status=%d", status)
	}
	locked := testService()
	locked.mu.Lock()
	locked.failed["192.0.2.9"] = lockThreshold
	locked.locked["192.0.2.9"] = testNow.Add(lockDuration)
	locked.mu.Unlock()
	w := httptest.NewRecorder()
	lockedMux := http.NewServeMux()
	locked.Register(lockedMux)
	lockedMux.ServeHTTP(w, req("POST", "/auth/login", nil))
	if w.Code != 429 {
		t.Fatalf("lockout status=%d", w.Code)
	}
}

func TestRateAndLockoutMapsStayBounded(t *testing.T) {
	s := testService()
	s.mu.Lock()
	for i := 0; i < maxTrackedIPs+4; i++ {
		ip := fmt.Sprintf("192.0.2.%d", i)
		s.rates[ip] = rateEntry{start: testNow}
		s.failed[ip] = 1
	}
	s.mu.Unlock()
	s.mu.Lock()
	s.prune(testNow)
	size := len(s.rates) + len(s.failed) + len(s.locked)
	s.mu.Unlock()
	if len(s.rates) > maxTrackedIPs || len(s.failed) > maxTrackedIPs || len(s.locked) > maxTrackedIPs {
		t.Fatalf("maps not bounded: %d", size)
	}
}
