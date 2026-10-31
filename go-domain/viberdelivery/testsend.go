package viberdelivery

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrTestConflict = errors.New("viber test settings changed")
var ErrTestNotReady = errors.New("viber test is not ready")
var ErrTestBusy = errors.New("viber channel is busy")

type TestReceipt struct {
	RequestID    string    `json:"request_id"`
	State        string    `json:"state"`
	MessageToken string    `json:"message_token,omitempty"`
	StatusCode   int       `json:"status_code"`
	APIStatus    int       `json:"api_status"`
	CreatedAt    time.Time `json:"created_at"`
}
type TestStore struct{ Pool *pgxpool.Pool }

func scanTest(row pgx.Row) (TestReceipt, error) {
	var r TestReceipt
	err := row.Scan(&r.RequestID, &r.State, &r.MessageToken, &r.StatusCode, &r.APIStatus, &r.CreatedAt)
	return r, err
}

const testSelect = `SELECT request_id::text,state,COALESCE(message_token,''),status_code,api_status,created_at FROM viber_test_receipts WHERE request_id=$1`
const recoverTestByID = `UPDATE viber_test_receipts t SET state='unknown',updated_at=clock_timestamp() WHERE t.request_id=$1 AND t.state='sending' AND NOT EXISTS (SELECT 1 FROM viber_channel_leases l WHERE l.channel_id=t.channel_id AND l.test_request_id=t.request_id AND l.delivery_id IS NULL AND l.lease_until>clock_timestamp())`
const recoverTestByChannel = `UPDATE viber_test_receipts t SET state='unknown',updated_at=clock_timestamp() WHERE t.channel_id=$1 AND t.state='sending' AND NOT EXISTS (SELECT 1 FROM viber_channel_leases l WHERE l.channel_id=t.channel_id AND l.test_request_id=t.request_id AND l.delivery_id IS NULL AND l.lease_until>clock_timestamp())`

// Begin records the sending receipt and acquires the shared channel lease before
// any network operation. Existing IDs return the durable result without a send.
func (s TestStore) Begin(ctx context.Context, id string, expected, credentialRevision int64) (TestReceipt, bool, error) {
	if s.Pool == nil || id == "" || expected < 0 || credentialRevision < 1 {
		return TestReceipt{}, false, ErrTestNotReady
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return TestReceipt{}, false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('viber-test-request'),hashtext($1))`, id); err != nil {
		return TestReceipt{}, false, err
	}
	if _, err = tx.Exec(ctx, recoverTestByID, id); err != nil {
		return TestReceipt{}, false, err
	}
	r, err := scanTest(tx.QueryRow(ctx, testSelect, id))
	if err == nil {
		return r, false, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return TestReceipt{}, false, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(1649258121,728441)`); err != nil {
		return TestReceipt{}, false, err
	}
	var actual int64
	var configured bool
	if err = tx.QueryRow(ctx, `SELECT revision,ciphertext IS NOT NULL FROM integration_credentials WHERE provider='viber' FOR SHARE`).Scan(&actual, &configured); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TestReceipt{}, false, ErrTestNotReady
		}
		return TestReceipt{}, false, err
	}
	var revision int64
	var channel, sender string
	var valid bool
	err = tx.QueryRow(ctx, `SELECT COALESCE((value->>'revision')::bigint,0),COALESCE(value->>'channel_id',''),COALESCE(value->>'sender_id',''),COALESCE((value->>'webhook_ready')::boolean,false) AND value->>'verified_at' IS NOT NULL AND (value->>'credential_revision')::bigint=$1 AND (value->>'verified_credential_revision')::bigint=$1 FROM settings WHERE key='viber_settings'`, credentialRevision).Scan(&revision, &channel, &sender, &valid)
	if errors.Is(err, pgx.ErrNoRows) {
		return TestReceipt{}, false, ErrTestNotReady
	}
	if err != nil {
		return TestReceipt{}, false, err
	}
	if revision != expected {
		return TestReceipt{}, false, ErrTestConflict
	}
	if !configured || actual != credentialRevision || !valid || channel == "" || sender == "" {
		return TestReceipt{}, false, ErrTestNotReady
	}
	if _, err = tx.Exec(ctx, `INSERT INTO viber_channel_leases(channel_id) VALUES($1) ON CONFLICT DO NOTHING`, channel); err != nil {
		return TestReceipt{}, false, err
	}
	var available bool
	if err = tx.QueryRow(ctx, `SELECT (lease_until IS NULL OR lease_until<=clock_timestamp()) AND next_post_at<=clock_timestamp() AND cooldown_until<=clock_timestamp() FROM viber_channel_leases WHERE channel_id=$1 FOR UPDATE`, channel).Scan(&available); err != nil {
		return TestReceipt{}, false, err
	}
	if !available {
		return TestReceipt{}, false, ErrTestBusy
	}
	if _, err = tx.Exec(ctx, `INSERT INTO viber_test_receipts(request_id,channel_id,sender_id,settings_revision,credential_revision,state) VALUES($1,$2,$3,$4,$5,'sending')`, id, channel, sender, revision, credentialRevision); err != nil {
		return TestReceipt{}, false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE viber_channel_leases SET delivery_id=NULL,test_request_id=$2,lease_until=clock_timestamp()+interval '60 seconds',next_post_at=clock_timestamp()+interval '1 second' WHERE channel_id=$1`, channel, id); err != nil {
		return TestReceipt{}, false, err
	}
	r, err = scanTest(tx.QueryRow(ctx, testSelect, id))
	if err != nil {
		return TestReceipt{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return TestReceipt{RequestID: id, State: "unknown"}, false, err
	}
	return r, true, nil
}

// Check renews the exact lease and verifies the non-secret configuration
// immediately before the POST. It never retries an uncertain sending receipt.
func (s TestStore) Check(ctx context.Context, id string) (string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(1649258121,728441)`); err != nil {
		return "", err
	}
	var rev int64
	if err = tx.QueryRow(ctx, `SELECT revision FROM integration_credentials WHERE provider='viber' FOR SHARE`).Scan(&rev); err != nil {
		return "", err
	}
	var channel, sender string
	var valid bool
	err = tx.QueryRow(ctx, `SELECT t.channel_id,t.sender_id,t.credential_revision=$2 AND l.test_request_id=t.request_id AND l.delivery_id IS NULL AND l.lease_until>clock_timestamp() AND c.ciphertext IS NOT NULL AND c.revision=t.credential_revision AND COALESCE((s.value->>'revision')::bigint,0)=t.settings_revision AND s.value->>'channel_id'=t.channel_id AND s.value->>'sender_id'=t.sender_id AND (s.value->>'credential_revision')::bigint=t.credential_revision AND (s.value->>'verified_credential_revision')::bigint=t.credential_revision AND COALESCE((s.value->>'webhook_ready')::boolean,false) AND s.value->>'verified_at' IS NOT NULL FROM viber_test_receipts t JOIN viber_channel_leases l ON l.channel_id=t.channel_id JOIN integration_credentials c ON c.provider='viber' JOIN settings s ON s.key='viber_settings' WHERE t.request_id=$1 AND t.state='sending' FOR UPDATE OF t,l`, id, rev).Scan(&channel, &sender, &valid)
	if err != nil {
		return "", err
	}
	if !valid {
		return "", ErrTestConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE viber_channel_leases SET lease_until=clock_timestamp()+interval '60 seconds' WHERE channel_id=$1 AND test_request_id=$2`, channel, id); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return sender, nil
}
func (s TestStore) Finish(ctx context.Context, id string, result Result) (TestReceipt, error) {
	if result.State != Sent && result.State != Rejected && result.State != Unknown {
		return TestReceipt{}, ErrTestConflict
	}
	if result.State == Sent && result.MessageToken == "" {
		return TestReceipt{}, ErrTestConflict
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return TestReceipt{}, err
	}
	defer tx.Rollback(ctx)
	var channel string
	err = tx.QueryRow(ctx, `SELECT channel_id FROM viber_test_receipts WHERE request_id=$1 AND state='sending' FOR UPDATE`, id).Scan(&channel)
	if err != nil {
		return TestReceipt{}, err
	}
	var token any
	if result.State == Sent {
		token = result.MessageToken
	}
	if _, err = tx.Exec(ctx, `UPDATE viber_test_receipts SET state=$2,message_token=$3,status_code=$4,api_status=$5,updated_at=clock_timestamp() WHERE request_id=$1`, id, string(result.State), token, result.StatusCode, result.APIStatus); err != nil {
		return TestReceipt{}, err
	}
	cooldown := result.RetryAfter
	if cooldown < 0 {
		cooldown = 0
	}
	if cooldown > time.Hour {
		cooldown = time.Hour
	}
	if result.State == Rejected && (result.StatusCode == 429 || result.APIStatus == 12) && cooldown < 30*time.Second {
		cooldown = 30 * time.Second
	}
	if _, err = tx.Exec(ctx, `UPDATE viber_channel_leases SET test_request_id=NULL,lease_until=NULL,next_post_at=GREATEST(next_post_at,clock_timestamp()+interval '1 second'),cooldown_until=GREATEST(cooldown_until,clock_timestamp()+($3*interval '1 second')) WHERE channel_id=$1 AND test_request_id=$2`, channel, id, cooldown.Seconds()); err != nil {
		return TestReceipt{}, err
	}
	r, err := scanTest(tx.QueryRow(ctx, testSelect, id))
	if err != nil {
		return TestReceipt{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return TestReceipt{RequestID: id, State: "unknown"}, err
	}
	return r, nil
}
func (s TestStore) Last(ctx context.Context, channel string) (TestReceipt, error) {
	if s.Pool == nil {
		return TestReceipt{}, ErrTestNotReady
	}
	if _, err := s.Pool.Exec(ctx, recoverTestByChannel, channel); err != nil {
		return TestReceipt{}, err
	}
	return scanTest(s.Pool.QueryRow(ctx, `SELECT request_id::text,state,COALESCE(message_token,''),status_code,api_status,created_at FROM viber_test_receipts WHERE channel_id=$1 ORDER BY created_at DESC,request_id DESC LIMIT 1`, channel))
}
func (s TestStore) Get(ctx context.Context, id string) (TestReceipt, error) {
	if s.Pool == nil {
		return TestReceipt{}, ErrTestNotReady
	}
	if _, err := s.Pool.Exec(ctx, recoverTestByID, id); err != nil {
		return TestReceipt{}, err
	}
	return scanTest(s.Pool.QueryRow(ctx, testSelect, id))
}
