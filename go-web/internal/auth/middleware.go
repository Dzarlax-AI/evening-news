package auth

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

func RequireAPIAdmin(service *Service, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := service.API(r)
		if err != nil {
			writeMiddlewareAuthError(w, err)
			return
		}
		if principal.Level != LevelAdmin {
			writeMiddlewareJSON(w, http.StatusForbidden, map[string]string{"detail": "Admin access required"})
			return
		}
		if mutating(r.Method) && crossSite(r) && !explicitAuthorization(service, r) {
			writeMiddlewareJSON(w, http.StatusForbidden, map[string]string{"detail": "Cross-site cookie request rejected"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequirePublicAdmin protects public-page mutations that intentionally accept
// the admin cookie or a trusted ForwardAuth principal. The endpoint predicate
// is authoritative for authentication; cross-origin requests are rejected
// regardless of any Authorization header because this route does not use that
// header as its authorization source.
func RequirePublicAdmin(isPublicAdmin func(*http.Request) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isPublicAdmin == nil || !isPublicAdmin(r) {
			writeMiddlewareJSON(w, http.StatusUnauthorized, map[string]string{"detail": "Admin access required"})
			return
		}
		if mutating(r.Method) && crossSite(r) {
			writeMiddlewareJSON(w, http.StatusForbidden, map[string]string{"detail": "Cross-site cookie request rejected"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeMiddlewareJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeMiddlewareAuthError(w http.ResponseWriter, err error) {
	var ae *AuthError
	if errors.As(err, &ae) {
		writeAuthError(w, ae)
		return
	}
	writeAuthError(w, asAuthError(err))
}
func mutating(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch || method == http.MethodDelete
}
func explicitAuthorization(service *Service, r *http.Request) bool {
	if strings.TrimSpace(r.Header.Get("Authorization")) == "" {
		return false
	}
	request := r.Clone(r.Context())
	request.Header = r.Header.Clone()
	request.Header.Del("Cookie")
	request.Header.Del("X-authentik-username")
	_, err := service.API(request)
	return err == nil
}
func crossSite(r *http.Request) bool {
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site")), "cross-site") {
		return true
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}
	p, err := url.Parse(origin)
	if err != nil || p.Host == "" || p.User != nil || p.Path != "" || p.RawQuery != "" || p.Fragment != "" || (p.Scheme != "https" && p.Scheme != "http") {
		return true
	}
	if !strings.EqualFold(strings.TrimSuffix(p.Host, "."), strings.TrimSuffix(r.Host, ".")) {
		return true
	}
	reqScheme := r.URL.Scheme
	if reqScheme != "http" && reqScheme != "https" {
		reqScheme = "http"
		if r.TLS != nil {
			reqScheme = "https"
		}
	}
	if !strings.EqualFold(reqScheme, p.Scheme) {
		return true
	}
	return false
}
