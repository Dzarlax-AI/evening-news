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
	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
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
	var pinned *appsettings.JobOperational
	if s.credentials != nil {
		settings, err := (appsettings.Store{Pool: s.db}).JobOperational(ctx, v.Envelope.JobID, int64(v.Envelope.Attempt))
		if err != nil {
			return nil, err
		}
		pinned = &settings
	}
	deadline, _ := time.Parse(time.RFC3339Nano, v.Envelope.Deadline)
	identity := digestworkflow.Identity{JobID: v.Envelope.JobID, Attempt: v.Envelope.Attempt, DomainInstanceID: v.DomainInstanceID, Deadline: deadline}
	switch v.Operation {
	case "summary_status":
		n, e := wf.Count(ctx, time.Now())
		return map[string]any{"summaries_count": n}, e
	case "summaries_generate":
		var n int
		var e error
		if pinned != nil && pinned.Settings.SerbianEnabled {
			policy := &digestworkflow.SerbianRetryPolicy{}
			var payload struct {
				SendTelegram *bool `json:"send_telegram"`
			}
			_ = json.Unmarshal(v.Payload, &payload)
			publish := v.Kind == "telegram_digest" || (v.Kind == "news_digest" && (payload.SendTelegram == nil || *payload.SendTelegram))
			policy.PublicationPinned = publish
			if publish && pinned.Settings.DeliveryEnabled && pinned.Telegram != nil && pinned.Telegram.SerbianDeliveryEnabled && appsettings.ValidateTelegram(appsettings.TelegramUpdate{ChatID: pinned.Telegram.ChatID, ServiceChatID: pinned.Telegram.ServiceChatID, SerbianChatID: pinned.Telegram.SerbianChatID, SerbianDeliveryEnabled: true}) == nil {
				policy.DeliveryEnabled, policy.Destination = true, pinned.Telegram.SerbianChatID
			}
			wf.RetryPolicy = policy
			n, e = wf.GenerateLocalized(ctx, v.Envelope.JobID, v.Envelope.Attempt, v.OperationID, time.Now())
		} else {
			n, e = wf.Generate(ctx, v.Envelope.JobID, v.Envelope.Attempt, v.OperationID, time.Now())
		}
		return map[string]any{"summaries_generated": n}, e
	case "serbian_digest_retry":
		var payload struct {
			RussianEditionID string `json:"russian_edition_id"`
		}
		if json.Unmarshal(v.Payload, &payload) != nil || !uuidPattern.MatchString(payload.RussianEditionID) {
			return nil, digestworkflow.ErrInvalid
		}
		tx, err := s.db.Begin(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback(ctx)
		if err = f.Validate(ctx, tx); err != nil {
			return nil, err
		}
		var date time.Time
		var destination *string
		var deliveryEnabled bool
		err = tx.QueryRow(ctx, `SELECT e.edition_date,r.destination,(r.delivery_enabled AND r.publication_attempted) FROM editorial_serbian_retry r JOIN editorial_digest_editions e ON e.id=r.russian_edition_id AND e.locale='ru' WHERE r.russian_edition_id=$1::uuid AND r.last_job_id=$2::uuid`, payload.RussianEditionID, v.Envelope.JobID).Scan(&date, &destination, &deliveryEnabled)
		if err != nil {
			return nil, digestworkflow.ErrInvalid
		}
		if err = tx.Commit(ctx); err != nil {
			return nil, err
		}
		if _, err = wf.RetrySerbian(ctx, v.Envelope.JobID, v.Envelope.Attempt, v.OperationID, date, payload.RussianEditionID); err != nil {
			return nil, err
		}
		if !deliveryEnabled {
			return map[string]any{"status": "success", "parts_sent": 0}, nil
		}
		if destination == nil || *destination == "" {
			return nil, digestworkflow.ErrDestination
		}
		sr, err := wf.PrepareSerbian(ctx, identity, date, payload.RussianEditionID, *destination)
		if errors.Is(err, digestworkflow.ErrNotReady) {
			return map[string]any{"status": "success", "parts_sent": 0}, nil
		}
		if err != nil {
			return nil, err
		}
		published, err := s.publishPrepared(ctx, v, f, sr)
		if err != nil {
			return nil, err
		}
		status := "success"
		if ok, _ := published["success"].(bool); !ok {
			status = "warning"
		}
		return map[string]any{"status": status, "success": published["success"], "parts_sent": published["parts_sent"]}, nil
	case "digest_prepare":

		destinations := map[string]string{}
		if pinned != nil && pinned.Telegram != nil {
			destinations["ru"] = pinned.Telegram.ChatID
			if pinned.Settings.SerbianEnabled && pinned.Telegram.SerbianDeliveryEnabled {
				if err := appsettings.ValidateTelegram(appsettings.TelegramUpdate{ChatID: pinned.Telegram.ChatID, ServiceChatID: pinned.Telegram.ServiceChatID, SerbianChatID: pinned.Telegram.SerbianChatID, SerbianDeliveryEnabled: true}); err != nil {
					return nil, digestworkflow.ErrDestination
				}
				destinations["sr-Latn"] = pinned.Telegram.SerbianChatID
			}
		}
		token, e := wf.PrepareLocalized(ctx, &s.digest.prepared, identity, time.Now(), destinations)
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
		policy := digestworkflow.SerbianRetryPolicy{PublicationPinned: true}
		if pinned != nil && pinned.Settings.DeliveryEnabled && pinned.Settings.SerbianEnabled && pinned.Telegram != nil && pinned.Telegram.SerbianDeliveryEnabled && appsettings.ValidateTelegram(appsettings.TelegramUpdate{ChatID: pinned.Telegram.ChatID, ServiceChatID: pinned.Telegram.ServiceChatID, SerbianChatID: pinned.Telegram.SerbianChatID, SerbianDeliveryEnabled: true}) == nil {
			policy.DeliveryEnabled, policy.Destination = true, pinned.Telegram.SerbianChatID
		}
		if err := wf.BindSerbianPublication(ctx, prepared.EditionID, policy); err != nil {
			return nil, err
		}

		if pinned != nil && !pinned.Settings.DeliveryEnabled {
			return map[string]any{"success": false, "status": "delivery_disabled", "parts_sent": 0}, nil
		}
		result, russianErr := s.publishPrepared(ctx, v, f, prepared)
		if err := wf.MarkRussianPublicationAttempted(ctx, prepared.EditionID); err != nil {
			return nil, err
		}
		if result == nil {
			result = map[string]any{}
		}
		if prepared.Secondary == nil && pinned != nil && pinned.Settings.SerbianEnabled && pinned.Telegram != nil && pinned.Telegram.SerbianDeliveryEnabled {
			if _, err := wf.RetrySerbian(ctx, v.Envelope.JobID, v.Envelope.Attempt, v.OperationID, time.Now(), prepared.EditionID); err == nil {
				sr, err := wf.PrepareSerbian(ctx, identity, time.Now(), prepared.EditionID, pinned.Telegram.SerbianChatID)
				if err == nil {
					prepared.Secondary = &sr
				}
			}
		}
		if prepared.Secondary != nil {
			secondary, err := s.publishPrepared(ctx, v, f, *prepared.Secondary)
			if err != nil {
				result["serbian_delivery"] = map[string]any{"success": false, "status": "delivery_not_confirmed"}
			} else {
				result["serbian_delivery"] = secondary
			}
		} else if pinned != nil && pinned.Settings.SerbianEnabled {
			result["serbian_delivery"] = map[string]any{"success": false, "status": "not_ready_or_disabled"}
		}
		if russianErr != nil {
			return nil, russianErr
		}
		return result, nil

	}
	return nil, digestworkflow.ErrInvalid
}

func (s *Server) publishPrepared(ctx context.Context, v digestInput, f authority.Fence, prepared digestworkflow.Prepared) (map[string]any, error) {
	destination := prepared.Destination
	var e error
	if destination == "" {
		if prepared.DestinationPinned {
			return nil, digestworkflow.ErrDestination
		}
		if prepared.Edition.Locale != "ru" {
			return nil, digestworkflow.ErrDestination
		}
		if s.credentials != nil {
			destination, e = s.managedDestination(ctx)
		} else {
			destination, e = (digestworkflow.Workflow{Pool: s.db, Fence: f}).Destination(ctx, s.config.TelegramMain, s.config.TelegramService)
		}
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
