package viberdelivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TransactionFence interface {
	Validate(context.Context, pgx.Tx) error
	ViberFenceIdentity() (string, int, string, json.RawMessage)
}
type PostgresStore struct {
	Pool              *pgxpool.Pool
	Fence             TransactionFence
	JobID             string
	AttemptGeneration int
	DomainInstanceID  string
}
type BindInput struct {
	RussianEditionID, ChannelID, SenderID string
	SettingsRevision, CredentialRevision  int64
	EnabledFrom                           time.Time
}
type PartInput struct{ Text string }
type Part struct {
	Index                           int
	Text, Hash, State, MessageToken string
	Attempts                        int
}
type Delivery struct {
	SnapshotHash, ContentHash                                                                     string
	ID, RussianEditionID, SerbianEditionID, ChannelID, SenderID, State, RenderVersion, RenderHash string
	SettingsRevision, CredentialRevision                                                          int64
	EnabledFrom                                                                                   time.Time
	Parts                                                                                         []Part
}
type Reservation string

const (
	Reserved    Reservation = "reserved"
	AlreadySent Reservation = "already_sent"
	Wait        Reservation = "wait"
	InFlight    Reservation = "in_flight"
)

var ErrConflict = errors.New("viber delivery conflict")

func (s PostgresStore) begin(ctx context.Context) (pgx.Tx, error) {
	if s.Pool == nil || s.Fence == nil || s.JobID == "" || s.DomainInstanceID == "" || s.AttemptGeneration < 1 {
		return nil, ErrConflict
	}
	job, attempt, _, _ := s.Fence.ViberFenceIdentity()
	if job != s.JobID || attempt != s.AttemptGeneration {
		return nil, ErrConflict
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	if e = s.Fence.Validate(ctx, tx); e != nil {
		tx.Rollback(ctx)
		return nil, e
	}
	return tx, nil
}

// lockControls serializes activation checks with both settings updates and token rotation.
func lockControls(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(1649258121,728440),pg_advisory_xact_lock_shared(1649258121,728441)`); err != nil {
		return err
	}
	var revision int64
	err := tx.QueryRow(ctx, `SELECT revision FROM integration_credentials WHERE provider='viber' FOR SHARE`).Scan(&revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return err
}
func currentBinding(ctx context.Context, tx pgx.Tx, in BindInput) (bool, error) {
	var ok bool
	err := tx.QueryRow(ctx, `SELECT COALESCE((SELECT (value #>> '{}')::boolean FROM settings WHERE key='operational_delivery_enabled'),true) AND COALESCE((SELECT (value->>'delivery_enabled')::boolean AND (value->>'enabled_from')::timestamptz=$1 AND value->>'channel_id'=$2 AND value->>'sender_id'=$3 AND (value->>'credential_revision')::bigint=$4 AND (value->>'verified_credential_revision')::bigint=$4 AND (value->>'webhook_ready')::boolean FROM settings WHERE key='viber_settings'),false) AND COALESCE((SELECT revision=$4 AND ciphertext IS NOT NULL FROM integration_credentials WHERE provider='viber'),false)`, in.EnabledFrom, in.ChannelID, in.SenderID, in.CredentialRevision).Scan(&ok)
	return ok, err
}
func validBindInput(in BindInput) bool {
	return in.RussianEditionID != "" && in.ChannelID != "" && in.SenderID != "" && in.CredentialRevision > 0 && in.SettingsRevision >= 0 && !in.EnabledFrom.IsZero()
}

// Request durably records the frozen publication before any Telegram network work.
// Binding is an optimization; the worker also materializes pending requests.
func (s PostgresStore) Request(ctx context.Context, in BindInput) error {
	if !validBindInput(in) {
		return ErrConflict
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockControls(ctx, tx); err != nil {
		return err
	}
	ok, err := currentBinding(ctx, tx, in)
	if err != nil {
		return err
	}
	if !ok {
		return tx.Commit(ctx)
	}
	_, err = tx.Exec(ctx, `INSERT INTO viber_publication_requests(russian_edition_id,channel_id,sender_id,settings_revision,credential_revision,enabled_from)
 SELECT id,$2,$3,$4,$5,$6 FROM editorial_digest_editions WHERE id=$1 AND locale='ru' AND job_id IS NOT NULL AND (created_at AT TIME ZONE 'UTC') >= $6
 ON CONFLICT (russian_edition_id,channel_id,purpose) DO NOTHING`, in.RussianEditionID, in.ChannelID, in.SenderID, in.SettingsRevision, in.CredentialRevision, in.EnabledFrom.UTC())
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	// A failed immediate binding remains pending for the worker.
	_, _ = s.Bind(ctx, in)
	return nil
}
func (s PostgresStore) Bind(ctx context.Context, in BindInput) (string, error) {
	if !validBindInput(in) {
		return "", ErrConflict
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	if e = lockControls(ctx, tx); e != nil {
		return "", e
	}
	ok, e := currentBinding(ctx, tx, in)
	if e != nil {
		return "", e
	}
	if !ok {
		return "", ErrConflict
	}
	var id string
	e = tx.QueryRow(ctx, `INSERT INTO viber_digest_deliveries(id,russian_edition_id,serbian_edition_id,channel_id,sender_id,settings_revision,credential_revision,enabled_from,state,snapshot_hash,content_hash)
 SELECT gen_random_uuid(),ru.id,sr.id,$2,$3,$4,$5,$6,CASE WHEN sr.id IS NULL THEN 'waiting_edition' ELSE 'ready' END,sr.snapshot_hash,CASE WHEN sr.id IS NULL THEN NULL ELSE encode(sha256(convert_to(sr.edition::text,'UTF8')),'hex') END
 FROM editorial_digest_editions ru LEFT JOIN editorial_digest_editions sr ON sr.job_id=ru.job_id AND sr.attempt_generation=ru.attempt_generation AND sr.locale='sr-Latn'
 WHERE ru.id=$1 AND ru.locale='ru' AND ru.job_id IS NOT NULL AND (ru.created_at AT TIME ZONE 'UTC') >= $6
 ON CONFLICT (russian_edition_id,channel_id,purpose) DO UPDATE SET updated_at=viber_digest_deliveries.updated_at
 WHERE viber_digest_deliveries.sender_id=excluded.sender_id AND viber_digest_deliveries.settings_revision=excluded.settings_revision AND viber_digest_deliveries.credential_revision=excluded.credential_revision AND viber_digest_deliveries.enabled_from=excluded.enabled_from RETURNING id::text`, in.RussianEditionID, in.ChannelID, in.SenderID, in.SettingsRevision, in.CredentialRevision, in.EnabledFrom.UTC()).Scan(&id)
	if e != nil {
		return "", e
	}
	if e = tx.Commit(ctx); e != nil {
		return "", e
	}
	return id, nil
}
func (s PostgresStore) deliveryAuthorized(id string) bool {
	_, _, kind, payload := s.Fence.ViberFenceIdentity()
	var body struct {
		DeliveryID string `json:"delivery_id"`
	}
	return kind == "viber_digest_delivery" && json.Unmarshal(payload, &body) == nil && body.DeliveryID == id
}
func (s PostgresStore) Load(ctx context.Context, id string) (Delivery, error) {
	if s.Fence == nil || !s.deliveryAuthorized(id) {
		return Delivery{}, ErrConflict
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return Delivery{}, e
	}
	defer tx.Rollback(ctx)
	var d Delivery
	e = tx.QueryRow(ctx, `SELECT id::text,russian_edition_id::text,COALESCE(serbian_edition_id::text,''),channel_id,sender_id,state,COALESCE(render_version,''),COALESCE(render_hash,''),COALESCE(snapshot_hash,''),COALESCE(content_hash,''),settings_revision,credential_revision,enabled_from FROM viber_digest_deliveries WHERE id=$1 AND last_job_id=$2 FOR UPDATE`, id, s.JobID).Scan(&d.ID, &d.RussianEditionID, &d.SerbianEditionID, &d.ChannelID, &d.SenderID, &d.State, &d.RenderVersion, &d.RenderHash, &d.SnapshotHash, &d.ContentHash, &d.SettingsRevision, &d.CredentialRevision, &d.EnabledFrom)
	if e != nil {
		return d, e
	}
	if d.State == "sending" {
		var stale bool
		if e = tx.QueryRow(ctx, `SELECT attempt_generation IS DISTINCT FROM $2 OR domain_instance_id IS DISTINCT FROM $3::uuid FROM viber_digest_deliveries WHERE id=$1`, id, s.AttemptGeneration, s.DomainInstanceID).Scan(&stale); e != nil {
			return d, e
		}
		if stale {
			if _, e = tx.Exec(ctx, `UPDATE viber_digest_deliveries SET state='delivery_unknown',updated_at=now() WHERE id=$1`, id); e != nil {
				return d, e
			}
			d.State = "delivery_unknown"
		}
	}
	rows, e := tx.Query(ctx, `SELECT part_index,text,payload_hash,state,COALESCE(message_token,''),attempts FROM viber_digest_parts WHERE delivery_id=$1 ORDER BY part_index`, id)
	if e != nil {
		return d, e
	}
	for rows.Next() {
		var p Part
		if e = rows.Scan(&p.Index, &p.Text, &p.Hash, &p.State, &p.MessageToken, &p.Attempts); e != nil {
			rows.Close()
			return d, e
		}
		d.Parts = append(d.Parts, p)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return d, e
	}
	e = tx.Commit(ctx)
	return d, e
}
func hashText(t string) string { h := sha256.Sum256([]byte(t)); return hex.EncodeToString(h[:]) }
func (s PostgresStore) Materialize(ctx context.Context, id, version string, parts []PartInput) error {
	if s.Fence == nil || !s.deliveryAuthorized(id) {
		return ErrConflict
	}
	if version == "" || len(parts) == 0 || len(parts) > 512 {
		return ErrConflict
	}
	h := sha256.New()
	for _, p := range parts {
		if p.Text == "" || len(p.Text) > 1<<20 {
			return ErrConflict
		}
		h.Write([]byte(hashText(p.Text)))
	}
	rh := hex.EncodeToString(h.Sum(nil))
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var state string
	var previous *string
	var priorVersion *string
	e = tx.QueryRow(ctx, `SELECT state,render_hash,render_version FROM viber_digest_deliveries WHERE id=$1 AND last_job_id=$2 FOR UPDATE`, id, s.JobID).Scan(&state, &previous, &priorVersion)
	if e != nil {
		return e
	}
	if state != "ready" && state != "retry_wait" {
		return ErrConflict
	}
	if previous != nil {
		if *previous != rh || priorVersion == nil || *priorVersion != version {
			return ErrConflict
		}
		return tx.Commit(ctx)
	}
	for i, p := range parts {
		if _, e = tx.Exec(ctx, `INSERT INTO viber_digest_parts(delivery_id,part_index,text,payload_hash,state) VALUES($1,$2,$3,$4,'pending')`, id, i, p.Text, hashText(p.Text)); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, `UPDATE viber_digest_deliveries SET render_hash=$2,render_version=$3,updated_at=now() WHERE id=$1`, id, rh, version); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

func (s PostgresStore) Reserve(ctx context.Context, id string, index int, hash string) (Reservation, error) {
	if s.Fence == nil || !s.deliveryAuthorized(id) {
		return InFlight, ErrConflict
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return InFlight, e
	}
	defer tx.Rollback(ctx)
	if e = lockControls(ctx, tx); e != nil {
		return InFlight, e
	}
	var state, channel, sender string
	var credential int64
	var enabledFrom time.Time
	var expired bool
	e = tx.QueryRow(ctx, `SELECT state,channel_id,sender_id,credential_revision,enabled_from,created_at<=clock_timestamp()-interval '24 hours' FROM viber_digest_deliveries WHERE id=$1 AND last_job_id=$2 FOR UPDATE`, id, s.JobID).Scan(&state, &channel, &sender, &credential, &enabledFrom, &expired)
	if e != nil {
		return InFlight, e
	}
	if state != "ready" && state != "retry_wait" {
		return InFlight, nil
	}
	if expired {
		if _, e = tx.Exec(ctx, `UPDATE viber_digest_deliveries SET state='exhausted',updated_at=now() WHERE id=$1`, id); e != nil {
			return InFlight, e
		}
		return InFlight, tx.Commit(ctx)
	}
	var ok bool
	e = tx.QueryRow(ctx, `SELECT COALESCE((SELECT (value #>> '{}')::boolean FROM settings WHERE key='operational_delivery_enabled'),true)`).Scan(&ok)
	if e != nil {
		return InFlight, e
	}
	if !ok {
		if _, e = tx.Exec(ctx, `UPDATE viber_digest_deliveries SET state='cancelled',updated_at=now() WHERE id=$1`, id); e != nil {
			return InFlight, e
		}
		return InFlight, tx.Commit(ctx)
	}
	e = tx.QueryRow(ctx, `SELECT COALESCE((SELECT (value->>'delivery_enabled')::boolean AND (value->>'credential_revision')::bigint=$1 AND (value->>'verified_credential_revision')::bigint=$1 AND (value->>'webhook_ready')::boolean AND value->>'channel_id'=$2 AND value->>'sender_id'=$3 AND (value->>'enabled_from')::timestamptz=$4 FROM settings WHERE key='viber_settings'),false) AND COALESCE((SELECT revision=$1 AND ciphertext IS NOT NULL FROM integration_credentials WHERE provider='viber'),false)`, credential, channel, sender, enabledFrom).Scan(&ok)
	if e != nil {
		return InFlight, e
	}
	if !ok {
		_, e = tx.Exec(ctx, `UPDATE viber_digest_deliveries SET state='configuration_changed',updated_at=now() WHERE id=$1`, id)
		if e != nil {
			return InFlight, e
		}
		return InFlight, tx.Commit(ctx)
	}
	var pstate, phash string
	var attempts int
	e = tx.QueryRow(ctx, `SELECT state,payload_hash,attempts FROM viber_digest_parts WHERE delivery_id=$1 AND part_index=$2 FOR UPDATE`, id, index).Scan(&pstate, &phash, &attempts)
	if e != nil {
		return InFlight, e
	}
	if phash != hash {
		return InFlight, ErrConflict
	}
	if pstate == "sent" {
		return AlreadySent, tx.Commit(ctx)
	}
	if (pstate != "pending" && pstate != "rejected") || attempts >= 3 {
		return InFlight, nil
	}
	e = tx.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM viber_digest_parts WHERE delivery_id=$1 AND ((part_index<$2 AND state<>'sent') OR state IN ('sending','unknown')))`, id, index).Scan(&ok)
	if e != nil || !ok {
		return InFlight, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO viber_channel_leases(channel_id) VALUES($1) ON CONFLICT DO NOTHING`, channel); e != nil {
		return InFlight, e
	}
	var available bool
	e = tx.QueryRow(ctx, `SELECT (test_request_id IS NULL OR lease_until<=clock_timestamp()) AND (delivery_id IS NULL OR delivery_id=$2 OR lease_until<=clock_timestamp()) AND next_post_at<=clock_timestamp() AND cooldown_until<=clock_timestamp() FROM viber_channel_leases WHERE channel_id=$1 FOR UPDATE`, channel, id).Scan(&available)
	if e != nil {
		return InFlight, e
	}
	if !available {
		return Wait, tx.Commit(ctx)
	}
	if _, e = tx.Exec(ctx, `UPDATE viber_channel_leases SET delivery_id=$2,test_request_id=NULL,lease_until=clock_timestamp()+interval '60 seconds',next_post_at=clock_timestamp()+interval '1 second' WHERE channel_id=$1`, channel, id); e != nil {
		return InFlight, e
	}
	if _, e = tx.Exec(ctx, `UPDATE viber_digest_parts SET state='sending',attempts=attempts+1,job_id=$3,attempt_generation=$4,domain_instance_id=$5,updated_at=now() WHERE delivery_id=$1 AND part_index=$2`, id, index, s.JobID, s.AttemptGeneration, s.DomainInstanceID); e != nil {
		return InFlight, e
	}
	if _, e = tx.Exec(ctx, `UPDATE viber_digest_deliveries SET state='sending',job_id=$2,attempt_generation=$3,domain_instance_id=$4,updated_at=now() WHERE id=$1`, id, s.JobID, s.AttemptGeneration, s.DomainInstanceID); e != nil {
		return InFlight, e
	}
	if e = tx.Commit(ctx); e != nil {
		return InFlight, e
	}
	return Reserved, nil
}
func (s PostgresStore) Finish(ctx context.Context, id string, index int, result Result) error {
	if s.Fence == nil || !s.deliveryAuthorized(id) {
		return ErrConflict
	}
	if result.State != Sent && result.State != Rejected && result.State != Unknown {
		return ErrConflict
	}
	if result.State == Sent && result.MessageToken == "" {
		return ErrConflict
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var channel string
	var attempts int
	e = tx.QueryRow(ctx, `SELECT d.channel_id,p.attempts FROM viber_digest_deliveries d JOIN viber_digest_parts p ON p.delivery_id=d.id WHERE d.id=$1 AND p.part_index=$2 AND d.state='sending' AND d.last_job_id=$3 AND d.job_id=$3 AND d.attempt_generation=$4 AND d.domain_instance_id=$5 AND p.state='sending' AND p.job_id=$3 AND p.attempt_generation=$4 AND p.domain_instance_id=$5 FOR UPDATE OF d,p`, id, index, s.JobID, s.AttemptGeneration, s.DomainInstanceID).Scan(&channel, &attempts)
	if e != nil {
		return e
	}
	state := string(result.State)
	deliveryState := "delivery_unknown"
	var token any
	if result.State == Sent {
		token = result.MessageToken
		deliveryState = "ready"
	} else if result.State == Rejected {
		deliveryState = "rejected"
		if result.StatusCode == 429 || result.APIStatus == 12 {
			if attempts >= 3 {
				deliveryState = "exhausted"
			} else {
				deliveryState = "retry_wait"
			}
		}
	}
	if _, e = tx.Exec(ctx, `UPDATE viber_digest_parts SET state=$3,message_token=$4,updated_at=now() WHERE delivery_id=$1 AND part_index=$2`, id, index, state, token); e != nil {
		return e
	}
	if deliveryState == "ready" {
		var all bool
		e = tx.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM viber_digest_parts WHERE delivery_id=$1 AND state<>'sent')`, id).Scan(&all)
		if e != nil {
			return e
		}
		if all {
			deliveryState = "completed"
		}
	}
	delay := result.RetryAfter
	if delay < 0 {
		delay = 0
	}
	if delay > time.Hour {
		delay = time.Hour
	}
	if deliveryState == "retry_wait" {
		min := 30 * time.Second
		if attempts > 1 {
			min = 120 * time.Second
		}
		if delay < min {
			delay = min
		}
	}
	if _, e = tx.Exec(ctx, `UPDATE viber_digest_deliveries SET state=$2,next_dispatch_at=clock_timestamp()+($3 * interval '1 second'),updated_at=now() WHERE id=$1`, id, deliveryState, delay.Seconds()); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE viber_channel_leases SET delivery_id=CASE WHEN $4='ready' THEN delivery_id ELSE NULL END,lease_until=CASE WHEN $4='ready' THEN clock_timestamp()+interval '60 seconds' ELSE NULL END,next_post_at=GREATEST(next_post_at,clock_timestamp()+interval '1 second'),cooldown_until=GREATEST(cooldown_until,clock_timestamp()+($2 * interval '1 second')) WHERE channel_id=$1 AND delivery_id=$3`, channel, delay.Seconds(), id, deliveryState); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

// Stop records a terminal decision without changing a sending or unknown part.
func (s PostgresStore) Stop(ctx context.Context, id, state string) error {
	if s.Fence == nil || !s.deliveryAuthorized(id) {
		return ErrConflict
	}
	switch state {
	case "rejected", "configuration_changed", "cancelled", "exhausted":
	default:
		return ErrConflict
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	tag, e := tx.Exec(ctx, `UPDATE viber_digest_deliveries d SET state=$3,updated_at=now() WHERE id=$1 AND last_job_id=$2 AND state IN ('ready','retry_wait') AND NOT EXISTS(SELECT 1 FROM viber_digest_parts p WHERE p.delivery_id=d.id AND (p.state IN ('sending','unknown') OR ($3='rejected' AND p.state='sent')))`, id, s.JobID, state)
	if e != nil {
		return e
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return tx.Commit(ctx)
}

// CheckSendAuthority revalidates the durable reservation immediately before POST.
func (s PostgresStore) CheckSendAuthority(ctx context.Context, id string, index int) error {
	if s.Fence == nil || !s.deliveryAuthorized(id) {
		return ErrConflict
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = lockControls(ctx, tx); err != nil {
		return err
	}
	var in BindInput
	var valid bool
	err = tx.QueryRow(ctx, `SELECT d.channel_id,d.sender_id,d.credential_revision,d.enabled_from,
 d.created_at>clock_timestamp()-interval '24 hours' AND l.delivery_id=d.id AND l.test_request_id IS NULL AND l.lease_until>clock_timestamp()
 FROM viber_digest_deliveries d JOIN viber_digest_parts p ON p.delivery_id=d.id AND p.part_index=$2
 JOIN viber_channel_leases l ON l.channel_id=d.channel_id
 WHERE d.id=$1 AND d.state='sending' AND d.last_job_id=$3 AND d.job_id=$3 AND d.attempt_generation=$4 AND d.domain_instance_id=$5
 AND p.state='sending' AND p.job_id=$3 AND p.attempt_generation=$4 AND p.domain_instance_id=$5 FOR UPDATE OF d,p,l`, id, index, s.JobID, s.AttemptGeneration, s.DomainInstanceID).Scan(&in.ChannelID, &in.SenderID, &in.CredentialRevision, &in.EnabledFrom, &valid)
	if err != nil {
		return err
	}
	if !valid {
		return ErrConflict
	}
	valid, err = currentBinding(ctx, tx, in)
	if err != nil {
		return err
	}
	if !valid {
		return ErrConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE viber_channel_leases SET lease_until=clock_timestamp()+interval '60 seconds' WHERE channel_id=$1 AND delivery_id=$2`, in.ChannelID, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
