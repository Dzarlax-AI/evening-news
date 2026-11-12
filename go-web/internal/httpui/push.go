package httpui

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/transport"
)

var pushIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func (h *backendHandler) push(w http.ResponseWriter, r *http.Request) {
	if !transport.SecureRequest(r) {
		writeProblem(w, 403, "HTTPS required")
		return
	}
	ops, ok := h.ops.(backend.PushOperations)
	if !ok {
		writeProblem(w, 503, "Push unavailable")
		return
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		writeProblem(w, 401, "Unauthorized")
		return
	}
	parts := strings.Split(values[0], " ")
	if len(parts) != 2 || parts[0] != "Bearer" || len(parts[1]) != 43 {
		writeProblem(w, 401, "Unauthorized")
		return
	}
	if b, e := base64.RawURLEncoding.DecodeString(parts[1]); e != nil || len(b) != 32 {
		writeProblem(w, 401, "Unauthorized")
		return
	}
	capability := parts[1]
	clientIP, valid := transport.CanonicalClientIP(r, h.trustedProxies)
	if !valid {
		writeProblem(w, 400, "Invalid client address")
		return
	}
	operation := "/push/installations"
	if id := r.PathValue("id"); id != "" {
		operation += "/" + id
		if strings.HasSuffix(r.URL.Path, "/activate") {
			operation += "/activate"
		}
	}
	if !validPushOperation(operation, r.Method) {
		writeProblem(w, 400, "Invalid push request")
		return
	}
	var body json.RawMessage
	if r.Method == http.MethodDelete {
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1))
		if err != nil || len(data) != 0 {
			writeProblem(w, 400, "Invalid push request")
			return
		}
	} else {
		data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
		if err != nil || !json.Valid(data) || hasDuplicateJSONKeys(data) || hasNullJSONField(data) {
			writeProblem(w, 400, "Invalid push request")
			return
		}
		if !validPushBody(data, r.Method, strings.HasSuffix(operation, "/activate")) {
			writeProblem(w, 400, "Invalid push request")
			return
		}
		body = data
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	out, status, err := ops.Push(ctx, r.Method, operation, capability, clientIP.String(), body)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	if status == http.StatusNoContent {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(out)
}
func (h *backendHandler) pushCanary(w http.ResponseWriter, r *http.Request) {
	ops, ok := h.ops.(backend.PushCanaryOperations)
	if !ok {
		writeProblem(w, 503, "Push unavailable")
		return
	}
	if !transport.SecureRequest(r) {
		writeProblem(w, 403, "HTTPS required")
		return
	}
	ip, ok := transport.CanonicalClientIP(r, h.trustedProxies)
	if !ok {
		writeProblem(w, 400, "Invalid client address")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
	if err != nil || !json.Valid(body) || hasDuplicateJSONKeys(body) || hasNullJSONField(body) {
		writeProblem(w, 400, "Invalid canary request")
		return
	}
	var in struct {
		InstallationID string `json:"installation_id"`
		EditionID      string `json:"edition_id"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if decodePushJSON(body, &in) != nil || !pushIDPattern.MatchString(in.InstallationID) || !pushIDPattern.MatchString(in.EditionID) || !pushIDPattern.MatchString(in.IdempotencyKey) {
		writeProblem(w, 400, "Invalid canary request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	out, status, err := ops.PushCanary(ctx, "/push/canary", ip.String(), body)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(out)
}
func validPushOperation(op, method string) bool {
	if op == "/push/installations" {
		return method == http.MethodPost
	}
	parts := strings.Split(strings.TrimPrefix(op, "/push/installations/"), "/")
	if len(parts) < 1 || !pushIDPattern.MatchString(parts[0]) {
		return false
	}
	if len(parts) == 1 {
		return method == http.MethodPut || method == http.MethodDelete
	}
	return len(parts) == 2 && parts[1] == "activate" && method == http.MethodPost
}
func validPushBody(raw []byte, method string, activation bool) bool {
	if method == http.MethodPost && !activation {
		var v struct {
			InstallationID string `json:"installation_id"`
			Token          string `json:"device_token"`
			Locale         string `json:"locale"`
			Environment    string `json:"environment"`
		}
		if decodePushJSON(raw, &v) != nil {
			return false
		}
		return pushIDPattern.MatchString(v.InstallationID) && validPushToken(v.Token) && validPushLocaleEnv(v.Locale, v.Environment)
	}
	if method == http.MethodPut {
		var v struct {
			ExpectedVersion int64  `json:"expected_version"`
			Token           string `json:"device_token"`
			Locale          string `json:"locale"`
			Environment     string `json:"environment"`
		}
		if decodePushJSON(raw, &v) != nil {
			return false
		}
		return v.ExpectedVersion > 0 && validPushToken(v.Token) && validPushLocaleEnv(v.Locale, v.Environment)
	}
	if activation {
		var v struct {
			Version int64  `json:"subscription_version"`
			Nonce   string `json:"nonce"`
		}
		if decodePushJSON(raw, &v) != nil {
			return false
		}
		b, e := base64.RawURLEncoding.DecodeString(v.Nonce)
		return v.Version > 0 && e == nil && len(b) == 32 && len(v.Nonce) == 43
	}
	return false
}
func decodePushJSON(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil || d.Decode(new(any)) != io.EOF {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func validPushToken(s string) bool {
	if len(s) < 2 || len(s) > 1024 || len(s)%2 != 0 || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
func validPushLocaleEnv(l, e string) bool {
	return (l == "ru" || l == "sr-Latn") && (e == "sandbox" || e == "production")
}
func hasNullJSONField(raw []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return true
	}
	for _, v := range fields {
		if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return true
		}
	}
	return false
}
