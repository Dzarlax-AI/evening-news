package auth

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/transport"
)

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	Level       string `json:"level"`
}

// Register mounts the auth API and the two small status endpoints that depend
// on authentication state.
func (s *Service) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/setup/status", s.managedStatus)
	mux.HandleFunc("POST /api/v1/setup/claim", s.managedClaim)
	mux.HandleFunc("POST /auth/login", s.handleLogin)
	mux.HandleFunc("GET /auth/me", s.handleMe)
	mux.HandleFunc("POST /auth/refresh", s.handleRefresh)
	mux.HandleFunc("GET /auth/security-status", s.handleSecurityStatus)
	mux.HandleFunc("POST /auth/logout", func(w http.ResponseWriter, _ *http.Request) {
		if s.managed != nil {
			http.SetCookie(w, &http.Cookie{Name: AdminCookie, Value: "", Path: "/", HttpOnly: true, Secure: !s.config.Development, SameSite: http.SameSiteLaxMode, MaxAge: -1})
		}
		writeJSON(w, 200, map[string]string{"message": "Logged out successfully. Please discard your token."})
	})
	mux.HandleFunc("GET /auth-status", s.handleAuthStatus)
	mux.HandleFunc("GET /api/public/admin-check", s.handleAdminCheck)
}

func (s *Service) handleLogin(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.managed != nil && !transport.SecureRequest(r) {
		writeJSON(w, 403, map[string]string{"detail": "HTTPS required"})
		return
	}
	if s.managed != nil && r.Header.Get("Origin") != "" && (crossSite(r) || r.Header.Get("X-CSRF-Token") != "1") {
		writeJSON(w, 403, map[string]string{"detail": "CSRF check failed"})
		return
	}
	p, failure := s.login(r)
	if failure != nil {
		writeAuthError(w, failure)
		return
	}
	token, err := s.Mint(p.Username, LevelAdmin)
	if err != nil {
		writeJSON(w, 500, map[string]string{"detail": "JWT signing secret is not configured"})
		return
	}
	if s.managed != nil && r.Header.Get("Origin") != "" {
		cookie, cookieErr := s.AdminCookie(p.Username)
		if cookieErr != nil {
			writeJSON(w, 503, map[string]string{"detail": "Session unavailable"})
			return
		}
		http.SetCookie(w, cookie)
	}
	writeJSON(w, 200, tokenResponse{AccessToken: token, TokenType: "bearer", ExpiresIn: int(TokenTTL.Seconds()), Level: LevelAdmin})
}

func (s *Service) handleMe(w http.ResponseWriter, r *http.Request) {
	p, err := s.API(r)
	if err != nil {
		writeAuthError(w, asAuthError(err))
		return
	}
	expiresAt := p.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = time.Unix(s.now().Add(TokenTTL).Unix(), 0).UTC()
	}
	expires := strings.TrimSuffix(expiresAt.UTC().Format(time.RFC3339Nano), "Z") + "+00:00"
	writeJSON(w, 200, map[string]string{"username": p.Username, "level": p.Level, "expires_at": expires})
}

func (s *Service) handleRefresh(w http.ResponseWriter, r *http.Request) {
	p, err := s.API(r)
	if err != nil {
		writeAuthError(w, asAuthError(err))
		return
	}
	token, mintErr := s.Mint(p.Username, p.Level)
	if mintErr != nil {
		writeJSON(w, 500, map[string]string{"detail": "JWT signing secret is not configured"})
		return
	}
	writeJSON(w, 200, tokenResponse{AccessToken: token, TokenType: "bearer", ExpiresIn: int(TokenTTL.Seconds()), Level: p.Level})
}

func (s *Service) handleAuthStatus(w http.ResponseWriter, r *http.Request) {
	configured := s.config.Password != ""
	if s.managed != nil {
		var err error
		configured, err = s.managed.configured(r.Context())
		if err != nil {
			writeJSON(w, 503, map[string]string{"detail": "Authentication unavailable"})
			return
		}
	}
	writeJSON(w, 200, map[string]any{"admin_auth_configured": configured, "admin_username": s.config.Username, "password_set": configured})
}

func (s *Service) handleAdminCheck(w http.ResponseWriter, r *http.Request) {
	p, ok := s.PublicAdmin(r)
	if !ok {
		writeJSON(w, 200, map[string]bool{"admin": false})
		return
	}
	writeJSON(w, 200, map[string]any{"admin": true, "user": p.Username})
}

func (s *Service) handleSecurityStatus(w http.ResponseWriter, r *http.Request) {
	configured := s.config.Password != ""
	if s.managed != nil {
		var err error
		configured, err = s.managed.configured(r.Context())
		if err != nil {
			writeJSON(w, 503, map[string]string{"detail": "Authentication unavailable"})
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, 200, map[string]any{"admin_auth_configured": configured, "jwt_expiration_hours": 24, "rate_limiting_enabled": true, "lockout_threshold": lockThreshold, "lockout_duration_minutes": 5, "failed_attempts_count": len(s.failed), "locked_ips_count": len(s.locked)})
}

func asAuthError(err error) *AuthError {
	if e, ok := err.(*AuthError); ok {
		return e
	}
	return &AuthError{Status: 401, Detail: "Invalid or missing credentials", Challenge: "Bearer"}
}
func writeAuthError(w http.ResponseWriter, e *AuthError) {
	if e.Challenge != "" {
		w.Header().Set("WWW-Authenticate", e.Challenge)
	}
	writeJSON(w, e.Status, map[string]string{"detail": e.Detail})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
