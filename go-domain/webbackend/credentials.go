package webbackend

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
)

type credentialWrite struct {
	ExpectedRevision *int64  `json:"expected_revision"`
	Value            *string `json:"value,omitempty"`
}

func (s *Server) credential(w http.ResponseWriter, r *http.Request) {
	provider := r.PathValue("provider")
	if provider != "gemini" && provider != "openai" && provider != "telegram" {
		reject(w, 422, "Invalid provider")
		return
	}
	if s.Credentials == nil {
		reject(w, 503, "Credential store unavailable")
		return
	}
	if r.Method == http.MethodGet {
		m, err := s.Credentials.Metadata(r.Context(), provider)
		if err != nil {
			reject(w, 503, "Credential store unavailable")
			return
		}
		write(w, 200, m)
		return
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	d.DisallowUnknownFields()
	var in credentialWrite
	if d.Decode(&in) != nil || d.Decode(new(any)) != io.EOF || in.ExpectedRevision == nil || (r.Method == http.MethodPut && in.Value == nil) || (r.Method == http.MethodDelete && in.Value != nil) {
		reject(w, 422, "Invalid credential request")
		return
	}
	var m credentialstore.Metadata
	var err error
	if r.Method == http.MethodPut {
		m, err = s.Credentials.Put(r.Context(), provider, *in.ExpectedRevision, []byte(*in.Value))
	} else {
		m, err = s.Credentials.Delete(r.Context(), provider, *in.ExpectedRevision)
	}
	switch {
	case errors.Is(err, credentialstore.ErrConflict):
		reject(w, 409, "Credential changed; reload before saving")
	case errors.Is(err, credentialstore.ErrInvalid):
		reject(w, 422, "Invalid credential request")
	case err != nil:
		reject(w, 503, "Credential store unavailable")
	default:
		write(w, 200, m)
	}
}
