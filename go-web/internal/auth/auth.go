package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/transport"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	LevelPublic   = "public"
	LevelAPIRead  = "api_read"
	LevelAPIWrite = "api_write"
	LevelAdmin    = "admin"
	TokenTTL      = 24 * time.Hour
	AdminCookie   = "admin_token"
	MaxAge        = 86400
	maxTokenSize  = 8 * 1024
	lockThreshold = 5
	lockDuration  = 5 * time.Minute
	rateWindow    = time.Minute
	maxTrackedIPs = 4096
)

type Config struct {
	Username, Password, JWTSecret string
	Development, TrustForwardAuth bool
}

type Principal struct {
	Username  string
	Level     string
	ExpiresAt time.Time
}

type AuthError struct {
	Status            int
	Detail, Challenge string
}

func (e *AuthError) Error() string { return e.Detail }

type Service struct {
	managed *ManagedStore
	config  Config
	secret  []byte
	now     func() time.Time
	mu      sync.Mutex
	failed  map[string]int
	locked  map[string]time.Time
	rates   map[string]rateEntry
}

func (s *Service) UseManaged(store *ManagedStore) { s.managed = store }
func (s *Service) Managed() bool                  { return s.managed != nil }

type rateEntry struct {
	start time.Time
	count int
}

func New(config Config) *Service {
	secret := config.JWTSecret
	if secret == "" {
		secret = config.Password
	}
	return &Service{config: config, secret: []byte(secret), now: time.Now, failed: map[string]int{}, locked: map[string]time.Time{}, rates: map[string]rateEntry{}}
}

func (s *Service) Mint(username, level string) (string, error) {
	if len(s.secret) == 0 {
		return "", errors.New("JWT signing secret is not configured")
	}
	now := s.now().UTC()
	claims := map[string]any{"sub": username, "level": level, "iat": now.Unix(), "exp": now.Add(TokenTTL).Unix(), "type": "access"}
	if s.managed != nil {
		rev, err := s.managed.revision(username)
		if err != nil {
			return "", err
		}
		claims["revision"] = rev
	}
	return s.sign(claims)
}

func (s *Service) sign(claims map[string]any) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (s *Service) verify(token string) (Principal, bool) {
	if len(token) == 0 || len(token) > maxTokenSize {
		return Principal{}, false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || len(s.secret) == 0 {
		return Principal{}, false
	}
	decode := base64.RawURLEncoding.DecodeString
	headerBytes, err := decode(parts[0])
	if err != nil {
		return Principal{}, false
	}
	var header map[string]any
	if json.Unmarshal(headerBytes, &header) != nil || header["alg"] != "HS256" {
		return Principal{}, false
	}
	sig, err := decode(parts[2])
	if err != nil {
		return Principal{}, false
	}
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return Principal{}, false
	}
	payloadBytes, err := decode(parts[1])
	if err != nil {
		return Principal{}, false
	}
	var claims map[string]any
	if json.Unmarshal(payloadBytes, &claims) != nil {
		return Principal{}, false
	}
	sub, okSub := claims["sub"].(string)
	level, okLevel := claims["level"].(string)
	exp, okExp := numericDate(claims["exp"])
	if !okSub || sub == "" || !okLevel || level == "" || !okExp || !s.now().Before(exp) {
		return Principal{}, false
	}
	if iatValue, exists := claims["iat"]; exists {
		iat, ok := numericDate(iatValue)
		if !ok || iat.After(s.now()) {
			return Principal{}, false
		}
	}
	if nbfValue, exists := claims["nbf"]; exists {
		nbf, ok := numericDate(nbfValue)
		if !ok || s.now().Before(nbf) {
			return Principal{}, false
		}
	}
	p := Principal{Username: sub, Level: level, ExpiresAt: exp}
	if s.managed != nil && !s.managed.validRevision(p, claims) {
		return Principal{}, false
	}
	return p, true
}

func numericDate(v any) (time.Time, bool) {
	var n float64
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return time.Time{}, false
		}
		n = f
	case float64:
		n = x
	default:
		return time.Time{}, false
	}
	if n != n || n > 1e12 || n < -1e12 {
		return time.Time{}, false
	}
	sec, frac := int64(n), n-float64(int64(n))
	return time.Unix(sec, int64(frac*float64(time.Second))).UTC(), true
}

func (s *Service) API(r *http.Request) (Principal, error) {
	if s.managed != nil && !transport.SecureRequest(r) {
		return Principal{}, &AuthError{Status: 403, Detail: "HTTPS or explicit local setup required"}
	}
	if p, ok := s.forwardPrincipal(r); ok {
		return p, nil
	}
	if scheme, value, ok := authHeader(r); ok && strings.EqualFold(scheme, "Bearer") {
		if p, valid := s.verify(value); valid {
			return p, nil
		}
	}
	if c, err := r.Cookie(AdminCookie); err == nil {
		if p, ok := s.verify(c.Value); ok {
			return p, nil
		}
	}
	if scheme, value, ok := authHeader(r); ok && strings.EqualFold(scheme, "Basic") {
		if user, pass, valid := decodeBasic(value); valid && s.basicCredentials(r, user, pass) {
			return Principal{Username: user, Level: LevelAdmin, ExpiresAt: time.Unix(s.now().Add(TokenTTL).Unix(), 0).UTC()}, nil
		}
	}
	return Principal{}, &AuthError{Status: http.StatusUnauthorized, Detail: "Invalid or missing credentials", Challenge: "Bearer"}
}

func (s *Service) AdminPage(r *http.Request) (Principal, error) {
	if s.managed != nil && !transport.SecureRequest(r) {
		return Principal{}, &AuthError{Status: 403, Detail: "HTTPS or explicit local setup required"}
	}
	if p, ok := s.forwardPrincipal(r); ok {
		return p, nil
	}
	if s.managed != nil {
		if c, err := r.Cookie(AdminCookie); err == nil {
			if p, ok := s.verify(c.Value); ok && p.Level == LevelAdmin {
				return p, nil
			}
		}
	}
	if s.config.Password == "" && s.managed == nil {
		return Principal{}, &AuthError{Status: http.StatusNotImplemented, Detail: "Admin authentication not configured. Please set ADMIN_PASSWORD environment variable."}
	}
	scheme, value, ok := authHeader(r)
	if ok && strings.EqualFold(scheme, "Basic") {
		if user, pass, valid := decodeBasic(value); valid && s.basicCredentials(r, user, pass) {
			return Principal{Username: user, Level: LevelAdmin, ExpiresAt: time.Unix(s.now().Add(TokenTTL).Unix(), 0).UTC()}, nil
		}
	}
	return Principal{}, &AuthError{Status: http.StatusUnauthorized, Detail: "Invalid credentials", Challenge: "Basic"}
}

func (s *Service) PublicAdmin(r *http.Request) (Principal, bool) {
	if s.managed != nil && !transport.SecureRequest(r) {
		return Principal{}, false
	}
	if p, ok := s.forwardPrincipal(r); ok {
		return p, true
	}
	if c, err := r.Cookie(AdminCookie); err == nil {
		p, ok := s.verify(c.Value)
		if ok && p.Level == LevelAdmin {
			return p, true
		}
	}
	return Principal{}, false
}

func (s *Service) forwardPrincipal(r *http.Request) (Principal, bool) {
	if !s.config.TrustForwardAuth {
		return Principal{}, false
	}
	user := r.Header.Get("X-authentik-username")
	if user == "" {
		return Principal{}, false
	}
	return Principal{Username: user, Level: LevelAdmin, ExpiresAt: time.Unix(s.now().Add(TokenTTL).Unix(), 0).UTC()}, true
}

func (s *Service) credentials(user, pass string) bool {
	if s.managed != nil {
		return s.managed.credentials(user, pass)
	}
	if s.config.Username == "" || s.config.Password == "" {
		return false
	}
	usernameMatches := hmac.Equal([]byte(user), []byte(s.config.Username))
	passwordMatches := hmac.Equal([]byte(pass), []byte(s.config.Password))
	return usernameMatches && passwordMatches
}

func (s *Service) basicCredentials(r *http.Request, user, pass string) bool {
	if s.managed == nil {
		return s.credentials(user, pass)
	}
	ip := remoteIP(r)
	now := s.now()
	s.mu.Lock()
	s.prune(now)
	until := s.locked[ip]
	allowed := !now.Before(until) && s.allow(ip, now, 5)
	s.mu.Unlock()
	if !allowed {
		return false
	}
	valid := s.credentials(user, pass)
	s.mu.Lock()
	defer s.mu.Unlock()
	if valid {
		delete(s.failed, ip)
		delete(s.locked, ip)
	} else {
		s.failed[ip]++
		if s.failed[ip] >= lockThreshold {
			s.locked[ip] = now.Add(lockDuration)
		}
	}
	return valid
}

func authHeader(r *http.Request) (string, string, bool) {
	v := r.Header.Get("Authorization")
	i := strings.IndexByte(v, ' ')
	if i < 0 {
		return "", "", false
	}
	return v[:i], strings.TrimSpace(v[i+1:]), true
}
func decodeBasic(v string) (string, string, bool) {
	b, err := base64.StdEncoding.DecodeString(v)
	if err != nil {
		return "", "", false
	}
	parts := strings.SplitN(string(b), ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func (s *Service) AdminCookie(username string) (*http.Cookie, error) {
	token, err := s.Mint(username, LevelAdmin)
	if err != nil {
		return nil, err
	}
	return &http.Cookie{Name: AdminCookie, Value: token, Path: "/", HttpOnly: true, Secure: !s.config.Development, SameSite: http.SameSiteLaxMode, MaxAge: MaxAge}, nil
}

// RemoteIP intentionally ignores forwarding headers. They are caller-controlled
// unless a separate, explicitly trusted proxy boundary is implemented.
func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func (s *Service) login(r *http.Request) (Principal, *AuthError) {
	ip := remoteIP(r)
	now := s.now()
	s.mu.Lock()
	s.prune(now)
	if until, ok := s.locked[ip]; ok && now.Before(until) {
		s.mu.Unlock()
		return Principal{}, &AuthError{Status: 429, Detail: "Too many failed attempts. Try again in 5 minutes."}
	}
	if !s.allow(ip, now, 5) {
		s.mu.Unlock()
		return Principal{}, &AuthError{Status: 429, Detail: "Rate limit exceeded"}
	}
	// Verification may query PostgreSQL and run Argon2. Rate bookkeeping must
	// not hold the global service mutex across either operation.
	s.mu.Unlock()
	if s.config.Password == "" && s.managed == nil {
		return Principal{}, &AuthError{Status: 501, Detail: "Admin authentication not configured"}
	}
	scheme, value, ok := authHeader(r)
	user, pass, valid := decodeBasic(value)
	if !ok || !strings.EqualFold(scheme, "Basic") || !valid {
		return Principal{}, &AuthError{Status: 401, Detail: "Not authenticated", Challenge: "Basic"}
	}
	if !s.credentials(user, pass) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.failed[ip]++
		if s.failed[ip] >= lockThreshold {
			s.locked[ip] = now.Add(lockDuration)
		}
		return Principal{}, &AuthError{Status: 401, Detail: "Invalid credentials", Challenge: "Basic"}
	}
	s.mu.Lock()
	delete(s.failed, ip)
	delete(s.locked, ip)
	s.mu.Unlock()
	return Principal{Username: user, Level: LevelAdmin}, nil
}

func (s *Service) allow(ip string, now time.Time, limit int) bool {
	entry, ok := s.rates[ip]
	if !ok || !now.Before(entry.start.Add(rateWindow)) {
		s.rates[ip] = rateEntry{start: now, count: 1}
		return true
	}
	if entry.count >= limit {
		return false
	}
	entry.count++
	s.rates[ip] = entry
	return true
}
func (s *Service) prune(now time.Time) {
	for ip, e := range s.rates {
		if !now.Before(e.start.Add(rateWindow)) {
			delete(s.rates, ip)
		}
	}
	for ip, until := range s.locked {
		if !now.Before(until) {
			delete(s.locked, ip)
			delete(s.failed, ip)
		}
	}
	for len(s.rates) >= maxTrackedIPs {
		for ip := range s.rates {
			delete(s.rates, ip)
			break
		}
	}
	for len(s.failed) >= maxTrackedIPs {
		for ip := range s.failed {
			delete(s.failed, ip)
			break
		}
	}
	for len(s.locked) >= maxTrackedIPs {
		for ip := range s.locked {
			delete(s.locked, ip)
			break
		}
	}
}
