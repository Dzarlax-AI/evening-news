package webbackend

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestJevCredentialProviderReachesStore(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/internal/web/v1/credentials/jev", nil)
		req.SetPathValue("provider", "jev")
		w := httptest.NewRecorder()
		(&Server{}).credential(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: got %d, want unavailable store", method, w.Code)
		}
	}
}
