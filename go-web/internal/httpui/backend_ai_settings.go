package httpui

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
)

type aiModelsInput struct {
	Article *string `json:"article"`
	Digest  *string `json:"digest"`
	Text    *string `json:"text"`
}
type aiProfileInput struct {
	Models *aiModelsInput `json:"models"`
}
type aiProfilesInput struct {
	Gemini *aiProfileInput `json:"gemini"`
	OpenAI *aiProfileInput `json:"openai"`
}
type aiUpdateInput struct {
	ExpectedRevision *int64           `json:"expected_revision"`
	ActiveProvider   *string          `json:"active_provider"`
	Profiles         *aiProfilesInput `json:"profiles"`
	ResetToEnv       *bool            `json:"reset_to_env"`
}

func (v aiUpdateInput) valid() bool {
	if v.ExpectedRevision == nil || v.ActiveProvider == nil || v.ResetToEnv == nil || v.Profiles == nil || v.Profiles.Gemini == nil || v.Profiles.OpenAI == nil {
		return false
	}
	for _, p := range []*aiProfileInput{v.Profiles.Gemini, v.Profiles.OpenAI} {
		if p.Models == nil || p.Models.Article == nil || p.Models.Digest == nil || p.Models.Text == nil {
			return false
		}
	}
	return true
}
func (h *backendHandler) aiSettingsGet(w http.ResponseWriter, r *http.Request) {
	ops, ok := h.ops.(backend.AISettingsOperations)
	if !ok {
		writeProblem(w, 503, "Private operation unavailable")
		return
	}
	h.call(w, r, ops.GetAISettings)
}
func (h *backendHandler) aiSettingsPut(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeProblem(w, 422, "Invalid AI settings request")
		return
	}
	var in aiUpdateInput
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if hasDuplicateJSONKeys(data) || dec.Decode(&in) != nil || !in.valid() {
		writeProblem(w, 422, "Invalid AI settings request")
		return
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		writeProblem(w, 422, "Invalid AI settings request")
		return
	}
	ops, ok := h.ops.(backend.AISettingsOperations)
	if !ok {
		writeProblem(w, 503, "Private operation unavailable")
		return
	}
	h.call(w, r, func(ctx context.Context) (json.RawMessage, error) {
		out, err := ops.UpdateAISettings(ctx, json.RawMessage(data))
		if api, ok := err.(*backend.APIError); ok {
			if api.Status == 422 {
				return nil, &backend.APIError{Status: 422, Detail: "Selected provider requires a configured key and valid models"}
			}
			if api.Status == 409 {
				return nil, &backend.APIError{Status: 409, Detail: "AI settings changed; reload before saving"}
			}
		}
		return out, err
	})
}
