package appsettings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const operationalRevisionKey = "operational_settings_revision"

var canonicalJobID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Operational contains only user-facing, non-secret job controls. Keep its
// fields limited to collection bounds and the processing/delivery switches.
type Operational struct {
	Revision          int64  `json:"revision"`
	LimitsEnabled     bool   `json:"limits_enabled"`
	MaxArticles       int    `json:"max_articles"`
	PerSource         int    `json:"per_source"`
	Days              int    `json:"days"`
	NewestDate        string `json:"newest_date"`
	ProcessingEnabled bool   `json:"processing_enabled"`
	DeliveryEnabled   bool   `json:"delivery_enabled"`
}

type OperationalUpdate struct {
	ExpectedRevision  int64  `json:"expected_revision"`
	LimitsEnabled     bool   `json:"limits_enabled"`
	MaxArticles       int    `json:"max_articles"`
	PerSource         int    `json:"per_source"`
	Days              int    `json:"days"`
	NewestDate        string `json:"newest_date"`
	ProcessingEnabled bool   `json:"processing_enabled"`
	DeliveryEnabled   bool   `json:"delivery_enabled"`
}

type JobOperational struct {
	JobID             string      `json:"job_id"`
	AttemptGeneration int64       `json:"attempt_generation"`
	PinnedAt          time.Time   `json:"pinned_at"`
	Settings          Operational `json:"settings"`
}

func defaultOperational() Operational {
	return Operational{LimitsEnabled: false, MaxArticles: 50, PerSource: 50, Days: 1, ProcessingEnabled: true, DeliveryEnabled: true}
}

func (v Operational) validate() error {
	if v.Revision < 0 || v.MaxArticles < 1 || v.MaxArticles > 100000 || v.PerSource < 1 || v.PerSource > 100000 || v.Days < 0 || v.Days > 36500 {
		return ErrInvalid
	}
	if v.NewestDate != "" {
		if _, err := time.Parse("2006-01-02", v.NewestDate); err != nil {
			return ErrInvalid
		}
	}
	return nil
}

func (s Store) Operational(ctx context.Context) (Operational, error) {
	if s.Pool == nil {
		return Operational{}, errors.New("settings unavailable")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Operational{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(1649258121,728440)`); err != nil {
		return Operational{}, err
	}
	v, err := operationalInTx(ctx, tx)
	if err != nil {
		return Operational{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Operational{}, err
	}
	return v, nil
}

func (s Store) UpdateOperational(ctx context.Context, in OperationalUpdate) (Operational, error) {
	v := Operational{Revision: in.ExpectedRevision, LimitsEnabled: in.LimitsEnabled, MaxArticles: in.MaxArticles, PerSource: in.PerSource, Days: in.Days, NewestDate: strings.TrimSpace(in.NewestDate), ProcessingEnabled: in.ProcessingEnabled, DeliveryEnabled: in.DeliveryEnabled}
	if in.ExpectedRevision < 0 || v.validate() != nil {
		return Operational{}, ErrInvalid
	}
	if s.Pool == nil {
		return Operational{}, errors.New("settings unavailable")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Operational{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728440)`); err != nil {
		return Operational{}, err
	}
	var rev int64
	err = tx.QueryRow(ctx, `SELECT (value #>> '{}')::bigint FROM settings WHERE key=$1`, operationalRevisionKey).Scan(&rev)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Operational{}, err
	}
	if rev != in.ExpectedRevision {
		return Operational{}, ErrConflict
	}
	pairs := [][2]any{
		{"operational_limits_enabled", v.LimitsEnabled}, {"operational_max_articles", v.MaxArticles},
		{"operational_per_source", v.PerSource}, {"operational_days", v.Days},
		{"operational_newest_date", v.NewestDate}, {"operational_processing_enabled", v.ProcessingEnabled},
		{"operational_delivery_enabled", v.DeliveryEnabled},
	}
	for _, pair := range pairs {
		encoded, marshalErr := json.Marshal(pair[1])
		if marshalErr != nil {
			return Operational{}, marshalErr
		}
		if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES($1,$2::jsonb,now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, pair[0], encoded); err != nil {
			return Operational{}, err
		}
	}
	rev++
	if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES($1,to_jsonb($2::bigint),now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, operationalRevisionKey, rev); err != nil {
		return Operational{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Operational{}, err
	}
	v.Revision = rev
	return v, nil
}

// PinOperational captures the current settings once for a running job attempt.
// The small transaction serializes concurrent first reads and validates that
// the worker still owns the requested job generation.
func (s Store) PinOperational(ctx context.Context, jobID string, attempt int64) (JobOperational, error) {
	if s.Pool == nil || !canonicalJobID.MatchString(jobID) || attempt < 1 {
		return JobOperational{}, ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return JobOperational{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728440)`); err != nil {
		return JobOperational{}, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('managed-job-operational-v1'),hashtext($1))`, jobID+":"+fmt.Sprint(attempt)); err != nil {
		return JobOperational{}, err
	}
	var state string
	var current int64
	if err = tx.QueryRow(ctx, `SELECT state,attempt_generation FROM background_jobs WHERE id=$1::uuid FOR SHARE`, jobID).Scan(&state, &current); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return JobOperational{}, ErrConflict
		}
		return JobOperational{}, err
	}
	if state != "running" || current != attempt {
		return JobOperational{}, ErrConflict
	}
	key := fmt.Sprintf("managed_job_config:%s:%d", jobID, attempt)
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT value FROM settings WHERE key=$1`, key).Scan(&raw)
	if err == nil {
		var pinned JobOperational
		if json.Unmarshal(raw, &pinned) != nil || pinned.JobID != jobID || pinned.AttemptGeneration != attempt || pinned.PinnedAt.IsZero() || pinned.Settings.validate() != nil {
			return JobOperational{}, errors.New("pinned operational settings are invalid")
		}
		if err = tx.Commit(ctx); err != nil {
			return JobOperational{}, err
		}
		return pinned, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return JobOperational{}, err
	}
	settings, err := operationalInTx(ctx, tx)
	if err != nil {
		return JobOperational{}, err
	}
	pinned := JobOperational{JobID: jobID, AttemptGeneration: attempt, PinnedAt: time.Now().UTC(), Settings: settings}
	data, err := json.Marshal(pinned)
	if err != nil {
		return JobOperational{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES($1,$2::jsonb,now())`, key, data); err != nil {
		return JobOperational{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return JobOperational{}, err
	}
	return pinned, nil
}

func (s Store) JobOperational(ctx context.Context, jobID string, attempt int64) (JobOperational, error) {
	if s.Pool == nil || !canonicalJobID.MatchString(jobID) || attempt < 1 {
		return JobOperational{}, ErrInvalid
	}
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT value FROM settings WHERE key=$1`, fmt.Sprintf("managed_job_config:%s:%d", jobID, attempt)).Scan(&raw)
	if err != nil {
		return JobOperational{}, err
	}
	var pinned JobOperational
	if json.Unmarshal(raw, &pinned) != nil || pinned.JobID != jobID || pinned.AttemptGeneration != attempt || pinned.PinnedAt.IsZero() || pinned.Settings.validate() != nil {
		return JobOperational{}, errors.New("pinned operational settings are invalid")
	}
	return pinned, nil
}

func operationalInTx(ctx context.Context, tx pgx.Tx) (Operational, error) {
	v := defaultOperational()
	var revision int64
	err := tx.QueryRow(ctx, `SELECT (value #>> '{}')::bigint FROM settings WHERE key=$1`, operationalRevisionKey).Scan(&revision)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Operational{}, err
	}
	v.Revision = revision
	// Read the seven known keys in one statement so pinning has a single DB
	// snapshot while remaining independent from the worker's lease transaction.
	rows, err := tx.Query(ctx, `SELECT key,value #>> '{}' FROM settings WHERE key = ANY($1)`, []string{
		"operational_limits_enabled", "operational_max_articles", "operational_per_source", "operational_days",
		"operational_newest_date", "operational_processing_enabled", "operational_delivery_enabled",
	})
	if err != nil {
		return Operational{}, err
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return Operational{}, err
		}
		seen++
		switch key {
		case "operational_limits_enabled":
			if v.LimitsEnabled, err = strconv.ParseBool(value); err != nil {
				return Operational{}, ErrInvalid
			}
		case "operational_max_articles":
			if v.MaxArticles, err = strconv.Atoi(value); err != nil {
				return Operational{}, ErrInvalid
			}
		case "operational_per_source":
			if v.PerSource, err = strconv.Atoi(value); err != nil {
				return Operational{}, ErrInvalid
			}
		case "operational_days":
			if v.Days, err = strconv.Atoi(value); err != nil {
				return Operational{}, ErrInvalid
			}
		case "operational_newest_date":
			v.NewestDate = value
		case "operational_processing_enabled":
			if v.ProcessingEnabled, err = strconv.ParseBool(value); err != nil {
				return Operational{}, ErrInvalid
			}
		case "operational_delivery_enabled":
			if v.DeliveryEnabled, err = strconv.ParseBool(value); err != nil {
				return Operational{}, ErrInvalid
			}
		}
	}
	if err := rows.Err(); err != nil {
		return Operational{}, err
	}
	if (seen == 0 && v.Revision != 0) || (seen != 0 && seen != 7) {
		return Operational{}, errors.New("operational settings are incomplete")
	}
	return v, v.validate()
}
