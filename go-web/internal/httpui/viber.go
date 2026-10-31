package httpui

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/transport"
)

func (h *backendHandler) viberOperation(w http.ResponseWriter, r *http.Request) {
	ops, ok := h.ops.(backend.ViberOperations)
	if !ok {
		writeProblem(w, 503, "Viber unavailable")
		return
	}
	var body json.RawMessage
	if r.Method == http.MethodPost {
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4096))
		if err != nil || !json.Valid(raw) || hasDuplicateJSONKeys(raw) {
			writeProblem(w, 422, "Invalid Viber request")
			return
		}
		body = raw
	}
	operation := strings.TrimPrefix(r.URL.Path, "/api/v1/viber/")
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) {
		return ops.Viber(ctx, r.Method, operation, body, r.URL.Query().Get("edition_id"))
	})
}

// The public callback relays raw bytes to domain verification without resolving credentials in web.
func (h *backendHandler) viberWebhook(w http.ResponseWriter, r *http.Request) {
	if !transport.SecureRequest(r) {
		writeProblem(w, http.StatusForbidden, "HTTPS callback required")
		return
	}
	ops, ok := h.ops.(backend.ViberOperations)
	if !ok {
		writeProblem(w, 503, "Viber unavailable")
		return
	}
	signature := r.Header.Get("X-Viber-Content-Signature")
	if len(signature) != 64 {
		writeProblem(w, 401, "Invalid callback signature")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	if err != nil {
		writeProblem(w, 413, "Callback too large")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	_, err = ops.ViberWebhook(ctx, body, signature)
	if err != nil {
		writeBackendError(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
}
