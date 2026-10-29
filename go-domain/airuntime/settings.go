package airuntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
)

var ErrAISettingsInvalid = errors.New("ai_settings_invalid")
var ErrAISettingsConflict = errors.New("ai_settings_conflict")

type AIModels struct {
	Article string `json:"article"`
	Digest  string `json:"digest"`
	Text    string `json:"text"`
}
type AIProfile struct {
	Models AIModels `json:"models"`
}
type AIProfiles struct {
	Gemini AIProfile `json:"gemini"`
	OpenAI AIProfile `json:"openai"`
}
type AIConfigured struct {
	Gemini bool `json:"gemini"`
	OpenAI bool `json:"openai"`
}
type AISettings struct {
	Revision       int64        `json:"revision"`
	ActiveProvider string       `json:"active_provider"`
	Profiles       AIProfiles   `json:"profiles"`
	Configured     AIConfigured `json:"configured"`
	Source         string       `json:"source"`
}
type AISettingsUpdate struct {
	ExpectedRevision int64      `json:"expected_revision"`
	ActiveProvider   string     `json:"active_provider"`
	Profiles         AIProfiles `json:"profiles"`
	ResetToEnv       bool       `json:"reset_to_env"`
}
type storedAISettings struct {
	ActiveProvider string     `json:"active_provider"`
	Profiles       AIProfiles `json:"profiles"`
}

func decodeStoredAISettings(raw []byte) (storedAISettings, error) {
	type models struct {
		Article *string `json:"article"`
		Digest  *string `json:"digest"`
		Text    *string `json:"text"`
	}
	type profile struct {
		Models *models `json:"models"`
	}
	var v struct {
		ActiveProvider *string `json:"active_provider"`
		Profiles       *struct {
			Gemini *profile `json:"gemini"`
			OpenAI *profile `json:"openai"`
		} `json:"profiles"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&v) != nil || dec.Decode(new(any)) != io.EOF || v.ActiveProvider == nil || v.Profiles == nil || v.Profiles.Gemini == nil || v.Profiles.OpenAI == nil {
		return storedAISettings{}, ErrAISettingsInvalid
	}
	convert := func(p *profile) (AIProfile, error) {
		if p.Models == nil || p.Models.Article == nil || p.Models.Digest == nil || p.Models.Text == nil {
			return AIProfile{}, ErrAISettingsInvalid
		}
		return AIProfile{Models: AIModels{Article: *p.Models.Article, Digest: *p.Models.Digest, Text: *p.Models.Text}}, nil
	}
	gem, err := convert(v.Profiles.Gemini)
	if err != nil {
		return storedAISettings{}, err
	}
	open, err := convert(v.Profiles.OpenAI)
	if err != nil {
		return storedAISettings{}, err
	}
	return storedAISettings{ActiveProvider: *v.ActiveProvider, Profiles: AIProfiles{Gemini: gem, OpenAI: open}}, nil
}

var modelID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func (s *Server) defaultAISettings() AISettings {
	provider := s.config.ActiveProvider
	if provider == "" {
		provider = "gemini"
	}
	text := s.config.TextModel
	if text == "" {
		text = s.config.Model
	}
	digest := s.config.DigestModel
	if digest == "" {
		digest = s.config.Model
	}
	return AISettings{ActiveProvider: provider, Profiles: AIProfiles{Gemini: AIProfile{AIModels{s.config.Model, digest, text}}, OpenAI: AIProfile{AIModels{s.config.OpenAIArticleModel, s.config.OpenAIDigestModel, s.config.OpenAITextModel}}}, Configured: AIConfigured{strings.TrimSpace(s.config.APIKey) != "", strings.TrimSpace(s.config.OpenAIKey) != ""}, Source: "env"}
}
func validateSettingsShape(v AISettings) error {
	if v.ActiveProvider != "gemini" && v.ActiveProvider != "openai" {
		return ErrAISettingsInvalid
	}
	for _, p := range []AIProfile{v.Profiles.Gemini, v.Profiles.OpenAI} {
		for _, model := range []string{p.Models.Article, p.Models.Digest, p.Models.Text} {
			if model != "" && !modelID.MatchString(model) {
				return ErrAISettingsInvalid
			}
		}
	}
	active := v.Profiles.Gemini
	if v.ActiveProvider == "openai" {
		active = v.Profiles.OpenAI
	}
	if active.Models.Article == "" || active.Models.Digest == "" || active.Models.Text == "" {
		return ErrAISettingsInvalid
	}
	return nil
}
func validateSettings(v AISettings) error {
	if err := validateSettingsShape(v); err != nil {
		return err
	}
	if (v.ActiveProvider == "gemini" && !v.Configured.Gemini) || (v.ActiveProvider == "openai" && !v.Configured.OpenAI) {
		return ErrAISettingsInvalid
	}
	return nil
}
func (s *Server) GetAISettings(ctx context.Context) (AISettings, error) {
	out := s.defaultAISettings()
	if s.db == nil {
		return out, nil
	}
	var revision int64
	var raw []byte
	err := s.db.QueryRow(ctx, `SELECT COALESCE((SELECT (value #>> '{}')::bigint FROM settings WHERE key='ai_settings_revision'),0), (SELECT value FROM settings WHERE key='ai_settings')`).Scan(&revision, &raw)
	if err != nil {
		return AISettings{}, err
	}
	out.Revision = revision
	if len(raw) > 0 {
		stored, decodeErr := decodeStoredAISettings(raw)
		if decodeErr != nil {
			return AISettings{}, ErrAISettingsInvalid
		}
		out.ActiveProvider = stored.ActiveProvider
		out.Profiles = stored.Profiles
		out.Source = "override"
		if err := validateSettingsShape(out); err != nil {
			return AISettings{}, err
		}
	}
	return out, nil
}
func (s *Server) UpdateAISettings(ctx context.Context, in AISettingsUpdate) (AISettings, error) {
	if in.ExpectedRevision < 0 || s.db == nil {
		return AISettings{}, ErrAISettingsInvalid
	}
	target := s.defaultAISettings()
	if !in.ResetToEnv {
		target.ActiveProvider = in.ActiveProvider
		target.Profiles = in.Profiles
	}
	if err := validateSettings(target); err != nil {
		return AISettings{}, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return AISettings{}, err
	}
	defer tx.Rollback(ctx)
	// Serializes creation and reset as well as updates, including when the row is absent.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121, 728437)`); err != nil {
		return AISettings{}, err
	}
	var rev int64
	err = tx.QueryRow(ctx, `SELECT (value #>> '{}')::bigint FROM settings WHERE key='ai_settings_revision'`).Scan(&rev)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return AISettings{}, err
	}
	if rev != in.ExpectedRevision {
		return AISettings{}, ErrAISettingsConflict
	}
	if in.ResetToEnv {
		_, err = tx.Exec(ctx, `DELETE FROM settings WHERE key='ai_settings'`)
	} else {
		raw, e := json.Marshal(storedAISettings{target.ActiveProvider, target.Profiles})
		if e != nil {
			return AISettings{}, ErrAISettingsInvalid
		}
		_, err = tx.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES('ai_settings',$1::jsonb,now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, raw)
	}
	if err != nil {
		return AISettings{}, err
	}
	rev++
	_, err = tx.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES('ai_settings_revision',to_jsonb($1::bigint),now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, rev)
	if err != nil {
		return AISettings{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return AISettings{}, err
	}
	target.Revision = rev
	if !in.ResetToEnv {
		target.Source = "override"
	}
	return target, nil
}

// ValidateAISettings validates the effective persisted profile without contacting providers.
func (s *Server) ValidateAISettings(ctx context.Context) error {
	settings, err := s.GetAISettings(ctx)
	if err != nil {
		return err
	}
	return validateSettings(settings)
}

// StartupAISettings permits management-only recovery when a valid persisted
// profile has lost its key and the environment profile can be restored.
func (s *Server) StartupAISettings(ctx context.Context) (bool, error) {
	settings, err := s.GetAISettings(ctx)
	if err != nil {
		return false, err
	}
	if err = validateSettings(settings); err == nil {
		return false, nil
	}
	if settings.Source == "override" && validateSettingsShape(settings) == nil && validateSettings(s.defaultAISettings()) == nil {
		return true, nil
	}
	return false, err
}
