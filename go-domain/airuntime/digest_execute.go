package airuntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/digestworkflow"
	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
	"github.com/jackc/pgx/v5"
)

func hashBytes(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func (s *Server) executeDigest(ctx context.Context, v digestInput, f authority.Fence) (map[string]any, error) {
	if err := f.Check(ctx); err != nil {
		return nil, err
	}
	wf := digestworkflow.Workflow{Pool: s.db, Fence: f, SiteBase: s.config.SiteBase}
	wf.ResolveProvider = func(ctx context.Context) (editorial.ProviderConfig, error) {
		snap, err := s.snapshot(ctx, "digest")
		if err != nil {
			return editorial.ProviderConfig{}, err
		}
		return editorial.ProviderConfig{Provider: snap.provider, Model: snap.model, Revision: snap.revision}, nil
	}
	wf.PriceUsage = func(u editorial.ProviderUsage) *float64 {
		values := usageInsert(usageRecord{usage: analysis.Usage{Provider: u.Provider, RequestedModel: u.RequestedModel, Model: u.Model, Normalized: u.Normalized}}, s.config)
		if cost, ok := values["cost"].(float64); ok {
			return &cost
		}
		return nil
	}
	wf.RecordUsage = func(ctx context.Context, tx pgx.Tx, u editorial.ProviderUsage) error {
		rec := usageRecord{domain: "digest", analysisType: "digest", usage: analysis.Usage{Provider: u.Provider, RequestedModel: u.RequestedModel, Model: u.Model, ProfileRevision: u.Revision, Normalized: u.Normalized, PromptTokens: u.PromptTokenCount, CompletionTokens: u.CandidatesTokenCount, TotalTokens: u.TotalTokenCount}}
		values := usageInsert(rec, s.config)
		details, err := json.Marshal(values["result"])
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO ai_usage_tracking(domain,analysis_type,tokens_used,credits_cost,analysis_result,created_at) VALUES($1,$2,$3,$4,$5,timezone('UTC',now()))`, rec.domain, rec.analysisType, values["tokens_used"], values["cost"], details)
		return err
	}
	deadline, _ := time.Parse(time.RFC3339Nano, v.Envelope.Deadline)
	identity := digestworkflow.Identity{JobID: v.Envelope.JobID, Attempt: v.Envelope.Attempt, DomainInstanceID: v.DomainInstanceID, Deadline: deadline}
	switch v.Operation {
	case "summary_status":
		n, e := wf.Count(ctx, time.Now())
		return map[string]any{"summaries_count": n}, e
	case "summaries_generate":
		n, e := wf.Generate(ctx, v.Envelope.JobID, v.Envelope.Attempt, v.OperationID, time.Now())
		return map[string]any{"summaries_generated": n}, e
	case "digest_prepare":
		token, e := wf.Prepare(ctx, &s.digest.prepared, identity, time.Now())
		return map[string]any{"prepared_token": token}, e
	case "digest_publish":
		var input struct {
			Token string `json:"prepared_token"`
		}
		if json.Unmarshal(v.Input, &input) != nil {
			return nil, digestworkflow.ErrPrepared
		}
		prepared, e := s.digest.prepared.Claim(input.Token, identity)
		if e != nil {
			return nil, e
		}
		var destination string
		if s.credentials != nil {
			destination, e = s.managedDestination(ctx)
		} else {
			destination, e = wf.Destination(ctx, s.config.TelegramMain, s.config.TelegramService)
		}
		token := s.config.TelegramToken
		if s.credentials != nil {
			secret, _, resolveErr := s.credentials.Resolve(ctx, "telegram")
			if resolveErr != nil {
				return nil, resolveErr
			}
			token = string(secret)
		}
		if e != nil || token == "" {
			return nil, digestworkflow.ErrDestination
		}
		renderedBytes, e := json.Marshal(prepared.Rendered)
		if e != nil {
			return nil, e
		}
		renderHash := hashBytes(renderedBytes)
		proposed, e := digestworkflow.NewUUID()
		if e != nil {
			return nil, e
		}
		store := telegramdelivery.PostgresStore{Pool: s.db, Fence: f, JobID: v.Envelope.JobID, AttemptGeneration: v.Envelope.Attempt, DomainInstanceID: v.DomainInstanceID}
		intent, e := store.ResolveIntent(ctx, proposed, prepared.EditionID, hashBytes([]byte(destination)), renderHash)
		if e != nil {
			return nil, e
		}
		if intent.Completed {
			return map[string]any{"success": true, "parts_sent": intent.PartsSent, "parts_total": intent.PartsSent, "rich_message": intent.RichMessage}, nil
		}
		delivery := telegramdelivery.Delivery{Store: store, Sender: telegramdelivery.Transport{Client: s.client, Admission: s.limiter, Endpoint: s.config.TelegramEndpoint, Token: token}, Authority: f}
		makePart := func(method string, index int, payload any) telegramdelivery.Part {
			raw, _ := json.Marshal(payload)
			return telegramdelivery.Part{Intent: intent.ID, Method: method, Index: index, PayloadHash: hashBytes(raw), Payload: payload}
		}
		if s.config.TelegramRich {
			rich := makePart("sendRichMessage", 0, map[string]any{"chat_id": destination, "rich_message": map[string]string{"html": prepared.Rendered.RichHTML}})
			state, e := delivery.SendPart(ctx, rich)
			if e != nil {
				if errors.Is(e, telegramdelivery.ErrRejected) {
					return map[string]any{"success": false, "parts_sent": 0, "parts_total": 1, "rich_message": true}, nil
				}
				return nil, e
			}
			if state == telegramdelivery.Sent {
				if e = store.Complete(ctx, intent.ID, 1, true); e != nil {
					return nil, e
				}
				return map[string]any{"success": true, "parts_sent": 1, "parts_total": 1, "rich_message": true}, nil
			}
		}
		parts := prepared.Rendered.PlainParts
		sent := 0
		for i, text := range parts {
			part := makePart("sendMessage", i, map[string]any{"chat_id": destination, "text": text, "parse_mode": "HTML", "disable_web_page_preview": true})
			state, e := delivery.SendPart(ctx, part)
			if e != nil {
				if errors.Is(e, telegramdelivery.ErrRejected) {
					return map[string]any{"success": false, "parts_sent": sent, "parts_total": len(parts), "rich_message": false}, nil
				}
				return nil, e
			}
			if state != telegramdelivery.Sent {
				return map[string]any{"success": false, "parts_sent": sent, "parts_total": len(parts), "rich_message": false}, nil
			}
			sent++
		}
		if e = store.Complete(ctx, intent.ID, sent, false); e != nil {
			return nil, e
		}
		return map[string]any{"success": true, "parts_sent": sent, "parts_total": len(parts), "rich_message": false}, nil
	}
	return nil, digestworkflow.ErrInvalid
}

var digestModelPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

func editorialEndpoint(base, model string) (string, error) {
	if !digestModelPattern.MatchString(model) {
		return "", errors.New("invalid_endpoint")
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("invalid_endpoint")
	}
	path := strings.TrimRight(u.Path, "/")
	suffix := "/" + model + ":generateContent"
	if strings.HasSuffix(path, ":generateContent") {
		if !strings.HasSuffix(path, suffix) {
			return "", errors.New("invalid_endpoint")
		}
	} else if strings.HasSuffix(path, "/models") {
		path += suffix
	} else {
		return "", errors.New("invalid_endpoint")
	}
	u.Path = path
	return u.String(), nil
}
