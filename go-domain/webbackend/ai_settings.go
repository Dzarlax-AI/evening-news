package webbackend

import (
	"errors"
	"net/http"

	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/privateapi"
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
func (s *Server) aiSettingsGet(w http.ResponseWriter, r *http.Request) {
	if s.Runtime == nil {
		reject(w, 503, "Executor runtime is unavailable")
		return
	}
	v, err := s.Runtime.GetAISettings(r.Context())
	if err != nil {
		reject(w, 503, "AI settings unavailable")
		return
	}
	write(w, 200, v)
}
func (s *Server) aiSettingsPut(w http.ResponseWriter, r *http.Request) {
	var in aiUpdateInput
	if privateapi.DecodeJSON(readAll(r), &in) != nil || !in.valid() {
		reject(w, 422, "Invalid AI settings request")
		return
	}
	if s.Runtime == nil {
		reject(w, 503, "Executor runtime is unavailable")
		return
	}
	models := func(p *aiProfileInput) airuntime.AIProfile {
		return airuntime.AIProfile{Models: airuntime.AIModels{Article: *p.Models.Article, Digest: *p.Models.Digest, Text: *p.Models.Text}}
	}
	v, err := s.Runtime.UpdateAISettings(r.Context(), airuntime.AISettingsUpdate{ExpectedRevision: *in.ExpectedRevision, ActiveProvider: *in.ActiveProvider, ResetToEnv: *in.ResetToEnv, Profiles: airuntime.AIProfiles{Gemini: models(in.Profiles.Gemini), OpenAI: models(in.Profiles.OpenAI)}})
	switch {
	case errors.Is(err, airuntime.ErrAISettingsInvalid):
		reject(w, 422, "Selected provider requires a configured key and valid models")
	case errors.Is(err, airuntime.ErrAISettingsConflict):
		reject(w, 409, "AI settings changed; reload before saving")
	case err != nil:
		reject(w, 503, "AI settings unavailable")
	default:
		write(w, 200, v)
	}
}
