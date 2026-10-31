package airuntime

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
	"github.com/Dzarlax-AI/evening-news/go-domain/viberdelivery"
	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
)

func (s *Server) viberStore(v digestInput, f authority.Fence) viberdelivery.PostgresStore {
	return viberdelivery.PostgresStore{Pool: s.db, Fence: f, JobID: v.Envelope.JobID, AttemptGeneration: v.Envelope.Attempt, DomainInstanceID: v.DomainInstanceID}
}
func (s *Server) bindViberPublication(ctx context.Context, v digestInput, f authority.Fence, pinned *appsettings.JobOperational, russianEditionID string) error {
	if pinned == nil || pinned.Viber == nil || !pinned.Settings.DeliveryEnabled || !pinned.Settings.SerbianEnabled || !pinned.Viber.DeliveryEnabled || pinned.Viber.EnabledFrom == nil {
		return nil
	}
	config := pinned.Viber
	return s.viberStore(v, f).Request(ctx, viberdelivery.BindInput{RussianEditionID: russianEditionID, ChannelID: config.ChannelID, SenderID: config.SenderID, SettingsRevision: config.Revision, CredentialRevision: config.CredentialRevision, EnabledFrom: *config.EnabledFrom})
}

func viberOutcome(d viberdelivery.Delivery) map[string]any {
	sent := 0
	for _, p := range d.Parts {
		if p.State == "sent" {
			sent++
		}
	}
	status := "warning"
	if d.State == "completed" {
		status = "success"
	} else if d.State == "ready" || d.State == "retry_wait" || d.State == "waiting_edition" {
		status = "deferred"
	}
	return map[string]any{"status": status, "success": d.State == "completed", "parts_sent": sent, "parts_total": len(d.Parts), "settled": true}
}
func (s *Server) executeViberDelivery(ctx context.Context, v digestInput, f authority.Fence) (map[string]any, error) {
	var payload struct {
		DeliveryID string `json:"delivery_id"`
	}
	if json.Unmarshal(v.Payload, &payload) != nil || !uuidPattern.MatchString(payload.DeliveryID) {
		return nil, viberdelivery.ErrConflict
	}
	store := s.viberStore(v, f)
	d, err := store.Load(ctx, payload.DeliveryID)
	if err != nil {
		return nil, err
	}
	if d.State != "ready" && d.State != "retry_wait" {
		return viberOutcome(d), nil
	}
	if s.credentials == nil {
		return nil, viberdelivery.ErrConflict
	}
	secret, meta, err := s.credentials.Resolve(ctx, "viber")
	if err != nil {
		return nil, err
	}
	if !meta.Configured || meta.Revision != d.CredentialRevision || len(secret) == 0 {
		if err = store.Stop(ctx, d.ID, "configuration_changed"); err != nil {
			return nil, err
		}
		d.State = "configuration_changed"
		return viberOutcome(d), nil
	}
	token := string(secret)
	if len(d.Parts) == 0 {
		tx, e := s.db.Begin(ctx)
		if e != nil {
			return nil, e
		}
		defer tx.Rollback(ctx)
		if e = f.Validate(ctx, tx); e != nil {
			return nil, e
		}
		var snapshotRaw, editionRaw, ruSnapshotRaw, ruEditionRaw []byte
		var snapshotHash, contentHash string
		e = tx.QueryRow(ctx, `SELECT sr.snapshot,sr.edition,ru.snapshot,ru.edition,sr.snapshot_hash,encode(sha256(convert_to(sr.edition::text,'UTF8')),'hex') FROM editorial_digest_editions sr JOIN editorial_digest_editions ru ON ru.id=$2::uuid AND ru.locale='ru' AND sr.job_id=ru.job_id AND sr.attempt_generation=ru.attempt_generation WHERE sr.id=$1::uuid AND sr.locale='sr-Latn'`, d.SerbianEditionID, d.RussianEditionID).Scan(&snapshotRaw, &editionRaw, &ruSnapshotRaw, &ruEditionRaw, &snapshotHash, &contentHash)
		if e != nil {
			return nil, e
		}
		if e = tx.Commit(ctx); e != nil {
			return nil, e
		}
		var snapshot, ruSnapshot editorial.Snapshot
		var edition, ruEdition editorial.Edition
		if snapshotHash != d.SnapshotHash || contentHash != d.ContentHash || json.Unmarshal(snapshotRaw, &snapshot) != nil || json.Unmarshal(editionRaw, &edition) != nil || json.Unmarshal(ruSnapshotRaw, &ruSnapshot) != nil || json.Unmarshal(ruEditionRaw, &ruEdition) != nil || editorial.ValidateEdition(ruSnapshot, "ru", ruEdition) != nil || !editorial.SameEditorialPlan(ruEdition, edition) {
			return nil, viberdelivery.ErrConflict
		}
		rendered, e := editorial.RenderViber(snapshot, "sr-Latn", edition, editorial.ViberRenderOptions{AuthToken: token, Sender: d.SenderID})
		if e != nil {
			if err = store.Stop(ctx, d.ID, "rejected"); err != nil {
				return nil, err
			}
			d.State = "rejected"
			return viberOutcome(d), nil
		}
		parts := make([]viberdelivery.PartInput, len(rendered.Parts))
		for i, text := range rendered.Parts {
			parts[i] = viberdelivery.PartInput{Text: text}
		}
		if err = store.Materialize(ctx, d.ID, rendered.Version, parts); err != nil {
			return nil, err
		}
		d, err = store.Load(ctx, d.ID)
		if err != nil {
			return nil, err
		}
	}
	// Preflight every frozen part before reserving even the first POST.
	for _, part := range d.Parts {
		units, size := editorial.ViberPayloadSize(part.Text, d.SenderID, token)
		if units > editorial.ViberSoftUTF16Limit || size > editorial.ViberSoftJSONByteLimit || size <= 0 {
			return nil, viberdelivery.ErrConflict
		}
	}
	transport := viberdelivery.Transport{Client: s.client, Token: token}
	for _, part := range d.Parts {
		for {
			reservation, e := store.Reserve(ctx, d.ID, part.Index, part.Hash)
			if e != nil {
				return nil, telegramdelivery.ErrUnknown
			}
			if reservation == viberdelivery.AlreadySent {
				break
			}
			if reservation == viberdelivery.Wait {
				timer := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return nil, ctx.Err()
				case <-timer.C:
				}
				continue
			}
			if reservation != viberdelivery.Reserved {
				fresh, e := store.Load(ctx, d.ID)
				if e != nil {
					return nil, e
				}
				if fresh.State == "delivery_unknown" || fresh.State == "sending" {
					return nil, telegramdelivery.ErrUnknown
				}
				return viberOutcome(fresh), nil
			}
			if e = store.CheckSendAuthority(ctx, d.ID, part.Index); e != nil {
				return nil, telegramdelivery.ErrUnknown
			}
			result := transport.Send(ctx, map[string]any{"from": d.SenderID, "type": "text", "text": part.Text})
			if e = store.Finish(ctx, d.ID, part.Index, result); e != nil {
				return nil, telegramdelivery.ErrUnknown
			}
			if result.State == viberdelivery.Unknown {
				return nil, telegramdelivery.ErrUnknown
			}
			if result.State != viberdelivery.Sent {
				fresh, e := store.Load(ctx, d.ID)
				if e != nil {
					return nil, e
				}
				return viberOutcome(fresh), nil
			}
			break
		}
	}
	d, err = store.Load(ctx, d.ID)
	if err != nil {
		return nil, err
	}
	return viberOutcome(d), nil
}
