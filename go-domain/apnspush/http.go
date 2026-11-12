package apnspush

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

type Handler struct {
	Service *Service
	Token   string
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h == nil || h.Service == nil || h.Token == "" {
		http.Error(w, `{"error":"unavailable"}`, 503)
		return
	}
	auth := "Bearer " + h.Token
	if len(auth) != len(r.Header.Get("Authorization")) || subtle.ConstantTimeCompare([]byte(auth), []byte(r.Header.Get("Authorization"))) != 1 {
		http.Error(w, `{"error":"unauthorized"}`, 401)
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/internal/web/v1/push/installations"), "/")
	if r.URL.Path == "/internal/web/v1/push/canary" && r.Method == http.MethodPost {
		var in CanaryRequest
		if decodeStrict(w, r, &in, 1024) != nil {
			http.Error(w, `{"error":"invalid_request"}`, 400)
			return
		}
		out, status, err := h.Service.Canary(r.Context(), in, r.Header.Get("X-Canonical-Client-IP"))
		h.respondCanary(w, status, out, err)
		return
	}
	if path == "" && r.Method == http.MethodPost {
		var in Registration
		if decodeStrict(w, r, &in, 4096) != nil {
			http.Error(w, `{"error":"invalid_request"}`, 400)
			return
		}
		s, status, err := h.Service.Register(r.Context(), in, r.Header.Get("X-Push-Capability"), r.Header.Get("X-Canonical-Client-IP"))
		h.respond(w, status, s, err)
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) == 0 || !installationID.MatchString(parts[0]) {
		http.Error(w, `{"error":"invalid_request"}`, 400)
		return
	}
	id := parts[0]
	cap := r.Header.Get("X-Push-Capability")
	ip := r.Header.Get("X-Canonical-Client-IP")
	if len(parts) == 2 && parts[1] == "activate" && r.Method == http.MethodPost {
		var in struct {
			SubscriptionVersion int64  `json:"subscription_version"`
			Nonce               string `json:"nonce"`
		}
		if decodeStrict(w, r, &in, 1024) != nil {
			http.Error(w, `{"error":"invalid_request"}`, 400)
			return
		}
		s, err := h.Service.Activate(r.Context(), id, cap, ip, in.Nonce, in.SubscriptionVersion)
		h.respond(w, 200, s, err)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodPut {
		var in struct {
			ExpectedVersion int64  `json:"expected_version"`
			DeviceToken     string `json:"device_token"`
			Locale          string `json:"locale"`
			Environment     string `json:"environment"`
		}
		if decodeStrict(w, r, &in, 4096) != nil {
			http.Error(w, `{"error":"invalid_request"}`, 400)
			return
		}
		reg := Registration{InstallationID: id, DeviceToken: in.DeviceToken, Locale: in.Locale, Environment: in.Environment}
		s, err := h.Service.Update(r.Context(), id, reg, cap, ip, in.ExpectedVersion)
		h.respond(w, 200, s, err)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodDelete {
		err := h.Service.Delete(r.Context(), id, cap, ip)
		switch {
		case err == nil:
			w.WriteHeader(http.StatusNoContent)
		case errors.Is(err, ErrRateLimit):
			http.Error(w, `{"error":"rate_limited"}`, 429)
		case errors.Is(err, ErrRequest):
			http.Error(w, `{"error":"unauthorized"}`, 401)
		case errors.Is(err, ErrUnavailable):
			http.Error(w, `{"error":"unavailable"}`, 503)
		case errors.Is(err, ErrUnauthorized):
			http.Error(w, `{"error":"unauthorized"}`, 401)
		default:
			http.Error(w, `{"error":"rate_limited"}`, 429)
		}
		return
	}
	http.NotFound(w, r)
}
func (h *Handler) respondCanary(w http.ResponseWriter, status int, out CanaryStatus, err error) {
	switch {
	case errors.Is(err, ErrConflict):
		status = 409
	case errors.Is(err, ErrUnauthorized):
		status = 401
	case errors.Is(err, ErrRateLimit):
		status = 429
	case errors.Is(err, ErrRequest):
		status = 400
	case err != nil:
		status = 503
	}
	if status == 429 {
		http.Error(w, `{"error":"rate_limited"}`, 429)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(out)
}
func (h *Handler) respond(w http.ResponseWriter, status int, s Status, err error) {
	switch {
	case errors.Is(err, ErrConflict):
		status = 409
	case errors.Is(err, ErrRequest):
		if status == 429 {
			break
		}
		status = 400
	case errors.Is(err, ErrUnauthorized):
		status = 401
	case errors.Is(err, ErrRateLimit):
		status = 429
	case errors.Is(err, ErrUnavailable):
		status = 503
	case err != nil:
		if status == 429 {
			break
		}
		status = 503
	}
	if status == 0 {
		status = 200
	}
	if status == 429 {
		http.Error(w, `{"error":"rate_limited"}`, 429)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(s)
}

// decodeStrict rejects unknown, duplicate, null, malformed, and trailing JSON.
func decodeStrict(w http.ResponseWriter, r *http.Request, out any, max int64) error {
	if r.Body == nil {
		return ErrRequest
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, max))
	if err != nil {
		return ErrRequest
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return ErrRequest
	}
	if hasDuplicateJSONKey(raw) {
		return ErrRequest
	}
	for _, v := range object {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return ErrRequest
		}
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return ErrRequest
	}
	return nil
}
func hasDuplicateJSONKey(raw []byte) bool {
	d := json.NewDecoder(bytes.NewReader(raw))
	var walk func() bool
	walk = func() bool {
		t, e := d.Token()
		if e != nil {
			return false
		}
		del, ok := t.(json.Delim)
		if !ok {
			return false
		}
		switch del {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return true
				}
				key, ok := k.(string)
				if !ok || seen[key] {
					return true
				}
				seen[key] = true
				if walk() {
					return true
				}
			}
			_, e = d.Token()
			return e != nil
		case '[':
			for d.More() {
				if walk() {
					return true
				}
			}
			_, e = d.Token()
			return e != nil
		}
		return false
	}
	return walk()
}
