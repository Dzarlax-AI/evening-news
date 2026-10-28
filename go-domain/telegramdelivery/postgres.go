package telegramdelivery

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TransactionFence validates authority using this transaction without taking
// another pool connection, and holds authority row locks until commit.
type TransactionFence interface {
	Validate(context.Context, pgx.Tx) error
}
type PostgresStore struct {
	Pool              *pgxpool.Pool
	Fence             TransactionFence
	JobID             string
	DomainInstanceID  string
	AttemptGeneration int
}

var errReceiptConflict = errors.New("delivery_receipt_conflict")

func (s PostgresStore) Reserve(ctx context.Context, p Part) (Reservation, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return InFlight, err
	}
	defer tx.Rollback(ctx)
	if err = s.Fence.Validate(ctx, tx); err != nil {
		return InFlight, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO digest_delivery_parts (intent_id,method,part_index,payload_hash,job_id,attempt_generation,state)
 VALUES ($1,$2,$3,$4,$5,$6,'sending') ON CONFLICT (intent_id,method,part_index) DO NOTHING`, p.Intent, p.Method, p.Index, p.PayloadHash, s.JobID, s.AttemptGeneration)
	if err != nil {
		return InFlight, err
	}
	reservation := New
	if tag.RowsAffected() == 0 {
		var state, hash string
		var status *int
		if err = tx.QueryRow(ctx, `SELECT state,payload_hash,status_code FROM digest_delivery_parts WHERE intent_id=$1 AND method=$2 AND part_index=$3 FOR UPDATE`, p.Intent, p.Method, p.Index).Scan(&state, &hash, &status); err != nil {
			return InFlight, err
		}
		if hash != p.PayloadHash {
			return InFlight, errReceiptConflict
		}
		switch state {
		case "sent":
			reservation = AlreadySent
		case "rejected":
			if status != nil && *status == 429 {
				reservation = RateLimited
			} else {
				reservation = PreviouslyRejected
			}
		default:
			reservation = InFlight
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return InFlight, err
	}
	return reservation, nil
}
func (s PostgresStore) Finish(ctx context.Context, p Part, result Result) error {
	if result.State != Sent && result.State != Rejected && result.State != Unknown {
		return errReceiptConflict
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = s.Fence.Validate(ctx, tx); err != nil {
		return err
	}
	var messageID any
	if result.State == Sent && result.MessageID != 0 {
		messageID = result.MessageID
	}
	var status any
	if result.StatusCode != 0 {
		status = result.StatusCode
	}
	tag, err := tx.Exec(ctx, `UPDATE digest_delivery_parts SET state=$1,message_id=$2,updated_at=timezone('UTC',now()), status_code=$9, retry_after_seconds=$10
 WHERE intent_id=$3 AND method=$4 AND part_index=$5 AND payload_hash=$6 AND job_id=$7 AND attempt_generation=$8 AND state='sending'`, string(result.State), messageID, p.Intent, p.Method, p.Index, p.PayloadHash, s.JobID, s.AttemptGeneration, status, result.RetryAfterSeconds)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errReceiptConflict
	}
	return tx.Commit(ctx)
}

// ResolveIntent creates or finds the immutable edition/destination intent under
// the same transaction fence as part reservation. The caller supplies canonical
// IDs and hashes; actual destination data is kept outside the receipt.
type Intent struct {
	ID          string
	Completed   bool
	PartsSent   int
	RichMessage bool
}

func (s PostgresStore) ResolveIntent(ctx context.Context, intentID, editionID, destinationHash, renderHash string) (Intent, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Intent{}, err
	}
	defer tx.Rollback(ctx)
	if err = s.Fence.Validate(ctx, tx); err != nil {
		return Intent{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO digest_delivery_intents (id,edition_id,destination_hash,purpose,render_hash,job_id,attempt_generation,domain_instance_id,state)
 VALUES ($1,$2,$3,'ru_digest',$4,$5,$6,$7,'active') ON CONFLICT (edition_id,destination_hash,purpose) DO NOTHING`, intentID, editionID, destinationHash, renderHash, s.JobID, s.AttemptGeneration, s.DomainInstanceID)
	if err != nil {
		return Intent{}, err
	}
	var intent Intent
	var hash, owner, instance, state string
	var attempt int
	var count *int
	var rich *bool
	err = tx.QueryRow(ctx, `SELECT id::text,render_hash,job_id::text,attempt_generation,domain_instance_id::text,state,parts_sent,rich_message FROM digest_delivery_intents WHERE edition_id=$1 AND destination_hash=$2 AND purpose='ru_digest' FOR UPDATE`, editionID, destinationHash).Scan(&intent.ID, &hash, &owner, &attempt, &instance, &state, &count, &rich)
	if err != nil {
		return Intent{}, err
	}
	if hash != renderHash {
		return Intent{}, errReceiptConflict
	}
	if state == "completed" {
		if count == nil || rich == nil {
			return Intent{}, errReceiptConflict
		}
		intent.Completed = true
		intent.PartsSent = *count
		intent.RichMessage = *rich
	} else if state != "active" || owner != s.JobID || attempt != s.AttemptGeneration || instance != s.DomainInstanceID {
		return Intent{}, ErrUnknown
	}
	if err = tx.Commit(ctx); err != nil {
		return Intent{}, err
	}
	return intent, nil
}

// Complete records the outcome only after every part is confirmed sent.
// Ambiguous commit must be treated as unknown by the caller.
func (s PostgresStore) Complete(ctx context.Context, id string, count int, rich bool) error {
	if count < 1 {
		return errReceiptConflict
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = s.Fence.Validate(ctx, tx); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE digest_delivery_intents SET state='completed',parts_sent=$1,rich_message=$2 WHERE id=$3 AND job_id=$4 AND attempt_generation=$5 AND domain_instance_id=$6 AND state='active'`, count, rich, id, s.JobID, s.AttemptGeneration, s.DomainInstanceID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errReceiptConflict
	}
	return tx.Commit(ctx)
}
