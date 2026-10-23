package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/calendar"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound               = errors.New("not found")
	ErrConflict               = errors.New("revision conflict")
	ErrImportConflict         = errors.New("import conflict")
	ErrImportPaused           = errors.New("import requires pause")
	ErrLeaseLost              = errors.New("dispatch lease lost")
	ErrOccurrenceNotFound     = errors.New("occurrence not found")
	ErrOccurrenceNotRetryable = errors.New("occurrence cannot be retried")
	ErrInvalidCursor          = errors.New("invalid cursor")
)

// InitError contains only bounded diagnostic metadata. It deliberately never
// retains a driver error, which may include connection details.
type InitError struct {
	Phase     string
	Statement int
	SQLState  string
}

func (e *InitError) Error() string {
	message := "scheduler initialization failed phase=" + e.Phase
	if e.Statement > 0 {
		message += fmt.Sprintf(" statement=%d", e.Statement)
	}
	if e.SQLState != "" {
		message += " sqlstate=" + e.SQLState
	}
	return message
}

var sqlStatePattern = regexp.MustCompile(`^[0-9A-Z]{5}$`)

func initError(phase string, statement int, err error) error {
	result := &InitError{Phase: phase, Statement: statement}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && sqlStatePattern.MatchString(pgErr.Code) {
		result.SQLState = pgErr.Code
	}
	return result
}

type Store struct {
	Pool      *pgxpool.Pool
	schedules string
	control   string
	outbox    string
	state     string
}

func Open(ctx context.Context, dsn, schema string, maxConns int32, startPaused bool) (*Store, error) {
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, initError("db_pool", 0, err)
	}
	poolConfig.MaxConns = maxConns
	poolConfig.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, initError("db_pool", 0, err)
	}
	s := &Store{
		Pool:      pool,
		schedules: pgx.Identifier{schema, "schedules"}.Sanitize(),
		control:   pgx.Identifier{schema, "control"}.Sanitize(),
		outbox:    pgx.Identifier{schema, "occurrences"}.Sanitize(),
		state:     pgx.Identifier{schema, "service_state"}.Sanitize(),
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, initError("db_connect", 0, err)
	}
	if err := s.Migrate(ctx, schema, startPaused); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Migrate(ctx context.Context, schema string, startPaused bool) error {
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	migrationSQL, err := migrations.Files.ReadFile("001_initial.sql")
	if err != nil {
		return initError("migration_read", 0, err)
	}
	pausedLiteral := "FALSE"
	if startPaused {
		pausedLiteral = "TRUE"
	}
	sql := strings.ReplaceAll(string(migrationSQL), "{{SCHEMA}}", schemaIdent)
	sql = strings.ReplaceAll(sql, "{{START_PAUSED}}", pausedLiteral)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return initError("migration_begin", 0, err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, "go-scheduler:"+schema); err != nil {
		return initError("migration_lock", 0, err)
	}
	var schemaExists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=$1)`, schema).Scan(&schemaExists); err != nil {
		return initError("migration_schema_check", 0, err)
	}
	if !schemaExists {
		if _, err := tx.Exec(ctx, `CREATE SCHEMA `+schemaIdent); err != nil {
			return initError("migration_schema_create", 0, err)
		}
	}
	for i, stmt := range strings.Split(sql, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return initError("migration_statement", i+1, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return initError("migration_commit", 0, err)
	}
	return nil
}

func (s *Store) Close() { s.Pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }

func (s *Store) GetSchedule(ctx context.Context, key string) (model.Schedule, error) {
	row := s.Pool.QueryRow(ctx, `SELECT schedule_key, revision, enabled, calendar, target_alias, operation, payload, next_due_at, created_at, updated_at FROM `+s.schedules+` WHERE schedule_key=$1`, key)
	item, err := scanSchedule(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Schedule{}, ErrNotFound
	}
	return item, err
}

func (s *Store) ListSchedules(ctx context.Context, limit int, cursor string) ([]model.Schedule, string, error) {
	var after string
	if cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, "", ErrInvalidCursor
		}
		after = string(decoded)
		if len(after) == 0 || len(after) > 128 {
			return nil, "", ErrInvalidCursor
		}
	}
	rows, err := s.Pool.Query(ctx, `SELECT schedule_key, revision, enabled, calendar, target_alias, operation, payload, next_due_at, created_at, updated_at FROM `+s.schedules+` WHERE schedule_key>$1 ORDER BY schedule_key LIMIT $2`, after, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	result := make([]model.Schedule, 0, limit+1)
	for rows.Next() {
		item, err := scanSchedule(rows)
		if err != nil {
			return nil, "", err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(result) <= limit {
		return result, "", nil
	}
	result = result[:limit]
	next := base64.RawURLEncoding.EncodeToString([]byte(result[len(result)-1].Key))
	return result, next, nil
}

func (s *Store) PutSchedule(ctx context.Context, key string, mutation model.Mutation, targets map[string]model.Target, now time.Time) (model.Schedule, bool, error) {
	if err := calendar.Validate(mutation.Calendar); err != nil {
		return model.Schedule{}, false, err
	}
	if err := calendar.ValidateResource(key, mutation.Target, mutation.Operation, mutation.Payload, targets); err != nil {
		return model.Schedule{}, false, err
	}
	var next *time.Time
	if mutation.Enabled {
		value, err := calendar.NextAfter(mutation.Calendar, now)
		if err != nil {
			return model.Schedule{}, false, err
		}
		next = &value
	}
	calJSON, _ := json.Marshal(mutation.Calendar)
	payloadJSON, err := json.Marshal(mutation.Payload)
	if err != nil || len(payloadJSON) > 4096 {
		return model.Schedule{}, false, errors.New("payload must be an object no larger than 4096 bytes")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return model.Schedule{}, false, err
	}
	defer tx.Rollback(ctx)
	current, currentErr := scanSchedule(tx.QueryRow(ctx, `SELECT schedule_key, revision, enabled, calendar, target_alias, operation, payload, next_due_at, created_at, updated_at FROM `+s.schedules+` WHERE schedule_key=$1 FOR UPDATE`, key))
	if errors.Is(currentErr, pgx.ErrNoRows) {
		if mutation.ExpectedRevision != 0 {
			return model.Schedule{}, false, ErrConflict
		}
		_, err = tx.Exec(ctx, `INSERT INTO `+s.schedules+` (schedule_key,revision,enabled,calendar,target_alias,operation,payload,next_due_at,created_at,updated_at) VALUES ($1,1,$2,$3::jsonb,$4,$5,$6::jsonb,$7,$8,$8)`, key, mutation.Enabled, string(calJSON), mutation.Target, mutation.Operation, string(payloadJSON), next, now.UTC())
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return model.Schedule{}, false, ErrConflict
			}
			return model.Schedule{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return model.Schedule{}, false, err
		}
		created, err := s.GetSchedule(ctx, key)
		return created, true, err
	}
	if currentErr != nil {
		return model.Schedule{}, false, currentErr
	}
	if mutation.ExpectedRevision != current.Revision {
		return model.Schedule{}, false, ErrConflict
	}
	same, err := sameConfig(ctx, tx, s.schedules, current, mutation)
	if err != nil {
		return model.Schedule{}, false, err
	}
	if same {
		return current, false, tx.Commit(ctx)
	}
	_, err = tx.Exec(ctx, `UPDATE `+s.schedules+` SET revision=revision+1,enabled=$2,calendar=$3::jsonb,target_alias=$4,operation=$5,payload=$6::jsonb,next_due_at=$7,updated_at=$8 WHERE schedule_key=$1`, key, mutation.Enabled, string(calJSON), mutation.Target, mutation.Operation, string(payloadJSON), next, now.UTC())
	if err != nil {
		return model.Schedule{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return model.Schedule{}, false, err
	}
	updated, err := s.GetSchedule(ctx, key)
	return updated, false, err
}

func (s *Store) DeleteSchedule(ctx context.Context, key string, expected int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var revision int64
	err = tx.QueryRow(ctx, `SELECT revision FROM `+s.schedules+` WHERE schedule_key=$1 FOR UPDATE`, key).Scan(&revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if revision != expected {
		return ErrConflict
	}
	if _, err := tx.Exec(ctx, `DELETE FROM `+s.schedules+` WHERE schedule_key=$1`, key); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type Control struct {
	Revision  int64     `json:"revision"`
	Paused    bool      `json:"paused"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (s *Store) GetControl(ctx context.Context) (Control, error) {
	var c Control
	err := s.Pool.QueryRow(ctx, `SELECT revision,paused,updated_at FROM `+s.control+` WHERE id=1`).Scan(&c.Revision, &c.Paused, &c.UpdatedAt)
	return c, err
}

func (s *Store) SetPaused(ctx context.Context, expected int64, paused bool, now time.Time) (Control, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Control{}, err
	}
	defer tx.Rollback(ctx)
	var current Control
	err = tx.QueryRow(ctx, `SELECT revision,paused,updated_at FROM `+s.control+` WHERE id=1 FOR UPDATE`).Scan(&current.Revision, &current.Paused, &current.UpdatedAt)
	if err != nil {
		return Control{}, err
	}
	if current.Revision != expected {
		return Control{}, ErrConflict
	}
	if current.Paused == paused {
		if err := tx.Commit(ctx); err != nil {
			return Control{}, err
		}
		return current, nil
	}
	err = tx.QueryRow(ctx, `UPDATE `+s.control+` SET revision=revision+1,paused=$1,updated_at=$2 WHERE id=1 RETURNING revision,paused,updated_at`, paused, now.UTC()).Scan(&current.Revision, &current.Paused, &current.UpdatedAt)
	if err != nil {
		return Control{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Control{}, err
	}
	return current, nil
}

type ImportItem struct {
	Key       string         `json:"schedule_key"`
	Enabled   bool           `json:"enabled"`
	Calendar  model.Calendar `json:"calendar"`
	Target    string         `json:"target_alias"`
	Operation string         `json:"operation"`
	Payload   map[string]any `json:"payload"`
	NextDueAt *time.Time     `json:"next_due_at"`
}

type ImportResult struct {
	ControlRevision int64    `json:"control_revision"`
	Created         []string `json:"created"`
	Unchanged       []string `json:"unchanged"`
}

func (s *Store) Import(ctx context.Context, expectedControl int64, items []ImportItem, targets map[string]model.Target, now time.Time) (ImportResult, error) {
	if len(items) < 1 || len(items) > 100 {
		return ImportResult{}, errors.New("import batch must contain 1 through 100 schedules")
	}
	seen := map[string]struct{}{}
	for _, item := range items {
		if _, ok := seen[item.Key]; ok {
			return ImportResult{}, errors.New("duplicate schedule key in import")
		}
		seen[item.Key] = struct{}{}
		if err := calendar.Validate(item.Calendar); err != nil {
			return ImportResult{}, err
		}
		if err := calendar.ValidateResource(item.Key, item.Target, item.Operation, item.Payload, targets); err != nil {
			return ImportResult{}, err
		}
		if !item.Enabled && item.NextDueAt != nil {
			return ImportResult{}, errors.New("disabled imports must have null next_due_at")
		}
		if item.Enabled && item.NextDueAt == nil {
			return ImportResult{}, errors.New("enabled imports must have a next_due_at")
		}
		b, err := json.Marshal(item.Payload)
		if err != nil || len(b) > 4096 {
			return ImportResult{}, errors.New("payload must be an object no larger than 4096 bytes")
		}
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ImportResult{}, err
	}
	defer tx.Rollback(ctx)
	var control Control
	if err = tx.QueryRow(ctx, `SELECT revision,paused,updated_at FROM `+s.control+` WHERE id=1 FOR UPDATE`).Scan(&control.Revision, &control.Paused, &control.UpdatedAt); err != nil {
		return ImportResult{}, err
	}
	if !control.Paused {
		return ImportResult{}, ErrImportPaused
	}
	if control.Revision != expectedControl {
		return ImportResult{}, ErrConflict
	}
	result := ImportResult{ControlRevision: control.Revision, Created: []string{}, Unchanged: []string{}}
	for _, item := range items {
		current, readErr := scanSchedule(tx.QueryRow(ctx, `SELECT schedule_key,revision,enabled,calendar,target_alias,operation,payload,next_due_at,created_at,updated_at FROM `+s.schedules+` WHERE schedule_key=$1 FOR UPDATE`, item.Key))
		if errors.Is(readErr, pgx.ErrNoRows) {
			cal, _ := json.Marshal(item.Calendar)
			payload, _ := json.Marshal(item.Payload)
			_, err = tx.Exec(ctx, `INSERT INTO `+s.schedules+`(schedule_key,revision,enabled,calendar,target_alias,operation,payload,next_due_at,created_at,updated_at) VALUES($1,1,$2,$3::jsonb,$4,$5,$6::jsonb,$7,$8,$8)`, item.Key, item.Enabled, string(cal), item.Target, item.Operation, string(payload), item.NextDueAt, now.UTC())
			if err != nil {
				return ImportResult{}, err
			}
			result.Created = append(result.Created, item.Key)
			continue
		}
		if readErr != nil {
			return ImportResult{}, readErr
		}
		m := model.Mutation{Enabled: item.Enabled, Calendar: item.Calendar, Target: item.Target, Operation: item.Operation, Payload: item.Payload}
		same, err := sameConfig(ctx, tx, s.schedules, current, m)
		if err != nil {
			return ImportResult{}, err
		}
		if !same || !sameTime(current.NextDueAt, item.NextDueAt) {
			return ImportResult{}, ErrImportConflict
		}
		result.Unchanged = append(result.Unchanged, item.Key)
	}
	if err = tx.Commit(ctx); err != nil {
		return ImportResult{}, err
	}
	sort.Strings(result.Created)
	sort.Strings(result.Unchanged)
	return result, nil
}

type Claimed struct {
	Envelope    model.Envelope
	TargetAlias string
	LeaseToken  string
	Attempt     int
}

func (s *Store) GenerateDue(ctx context.Context, now time.Time, limit, maxOutbox int) (int, error) {
	now = now.UTC().Truncate(time.Microsecond)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var paused bool
	if err = tx.QueryRow(ctx, `SELECT paused FROM `+s.control+` WHERE id=1 FOR UPDATE`).Scan(&paused); err != nil {
		return 0, err
	}
	if paused {
		if _, err = tx.Exec(ctx, `UPDATE `+s.state+` SET last_tick_at=$1 WHERE id=1`, now.UTC()); err != nil {
			return 0, err
		}
		return 0, tx.Commit(ctx)
	}
	var outstanding int64
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM `+s.outbox+` WHERE status<>'accepted'`).Scan(&outstanding); err != nil {
		return 0, err
	}
	if outstanding >= int64(maxOutbox) {
		if _, err = tx.Exec(ctx, `UPDATE `+s.state+` SET last_tick_at=$1 WHERE id=1`, now); err != nil {
			return 0, err
		}
		return 0, tx.Commit(ctx)
	}
	if free := maxOutbox - int(outstanding); limit > free {
		limit = free
	}
	rows, err := tx.Query(ctx, `SELECT s.schedule_key,s.revision,s.calendar,s.target_alias,s.operation,s.payload,s.next_due_at FROM `+s.schedules+` s WHERE s.enabled AND s.next_due_at IS NOT NULL AND s.next_due_at<=$1 AND NOT EXISTS (SELECT 1 FROM `+s.outbox+` o WHERE o.schedule_key=s.schedule_key AND o.status<>'accepted') ORDER BY s.next_due_at,s.schedule_key LIMIT $2 FOR UPDATE OF s SKIP LOCKED`, now.UTC(), limit)
	if err != nil {
		return 0, err
	}
	type due struct {
		key        string
		revision   int64
		calendar   []byte
		target, op string
		payload    []byte
		when       time.Time
	}
	var dueRows []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.key, &d.revision, &d.calendar, &d.target, &d.op, &d.payload, &d.when); err != nil {
			rows.Close()
			return 0, err
		}
		dueRows = append(dueRows, d)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	for _, d := range dueRows {
		var cal model.Calendar
		var payload map[string]any
		if err = json.Unmarshal(d.calendar, &cal); err != nil {
			return 0, err
		}
		if err = decodeJSONMap(d.payload, &payload); err != nil {
			return 0, err
		}
		id, err := newUUID()
		if err != nil {
			return 0, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO `+s.outbox+`(occurrence_id,schedule_key,schedule_revision,scheduled_for,target_alias,operation,payload,status,available_at) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,'pending',$8)`, id, d.key, d.revision, d.when, d.target, d.op, string(d.payload), now.UTC())
		if err != nil {
			return 0, err
		}
		next, err := calendar.NextAfter(cal, now)
		if err != nil {
			return 0, err
		}
		if _, err = tx.Exec(ctx, `UPDATE `+s.schedules+` SET next_due_at=$2 WHERE schedule_key=$1`, d.key, next); err != nil {
			return 0, err
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE `+s.state+` SET last_tick_at=$1 WHERE id=1`, now.UTC()); err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(dueRows), nil
}

func (s *Store) Claim(ctx context.Context, _ time.Time, lease time.Duration, limit int) ([]Claimed, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var paused bool
	if err = tx.QueryRow(ctx, `SELECT paused FROM `+s.control+` WHERE id=1 FOR UPDATE`).Scan(&paused); err != nil {
		return nil, err
	}
	if paused {
		return []Claimed{}, tx.Commit(ctx)
	}
	var dbNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil {
		return nil, err
	}
	dbNow = dbNow.UTC().Truncate(time.Microsecond)
	rows, err := tx.Query(ctx, `SELECT occurrence_id::text,schedule_key,schedule_revision,scheduled_for,target_alias,operation,payload,attempts FROM `+s.outbox+` WHERE ((status='pending' AND available_at<=$1) OR (status='leased' AND lease_until<=$1)) ORDER BY available_at,created_at,occurrence_id LIMIT $2 FOR UPDATE SKIP LOCKED`, dbNow, limit)
	if err != nil {
		return nil, err
	}
	type candidate struct {
		id, key           string
		revision          int64
		scheduled         time.Time
		target, operation string
		payload           []byte
		attempt           int
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.key, &c.revision, &c.scheduled, &c.target, &c.operation, &c.payload, &c.attempt); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	claimed := make([]Claimed, 0, len(candidates))
	for _, c := range candidates {
		token, err := newUUID()
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(ctx, `UPDATE `+s.outbox+` SET status='leased',lease_token=$2,lease_until=$3,attempts=attempts+1,updated_at=$4 WHERE occurrence_id=$1::uuid`, c.id, token, dbNow.Add(lease), dbNow); err != nil {
			return nil, err
		}
		var payload map[string]any
		if err = decodeJSONMap(c.payload, &payload); err != nil {
			return nil, err
		}
		claimed = append(claimed, Claimed{LeaseToken: token, TargetAlias: c.target, Attempt: c.attempt + 1, Envelope: model.Envelope{Version: 1, OccurrenceID: c.id, ScheduleKey: c.key, ScheduleRevision: c.revision, ScheduledFor: c.scheduled.UTC(), Operation: c.operation, Payload: payload}})
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *Store) Ack(ctx context.Context, id, token, jobID string, now time.Time) error {
	now = now.UTC().Truncate(time.Microsecond)
	tag, err := s.Pool.Exec(ctx, `UPDATE `+s.outbox+` SET status='accepted',job_id=$3::uuid,lease_token=NULL,lease_until=NULL,last_error_code=NULL,updated_at=$4 WHERE occurrence_id=$1::uuid AND status='leased' AND lease_token=$2::uuid AND lease_until>clock_timestamp()`, id, token, jobID, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	_, err = s.Pool.Exec(ctx, `UPDATE `+s.state+` SET last_dispatch_at=$1 WHERE id=1`, now.UTC())
	return err
}

func (s *Store) MarkDispatch(ctx context.Context, now time.Time) error {
	_, err := s.Pool.Exec(ctx, `UPDATE `+s.state+` SET last_dispatch_at=$1 WHERE id=1`, now.UTC())
	return err
}

func (s *Store) Retry(ctx context.Context, id, token, code string, next time.Time, now time.Time) error {
	now = now.UTC().Truncate(time.Microsecond)
	tag, err := s.Pool.Exec(ctx, `UPDATE `+s.outbox+` SET status='pending',available_at=$3,lease_token=NULL,lease_until=NULL,last_error_code=$4,updated_at=$5 WHERE occurrence_id=$1::uuid AND status='leased' AND lease_token=$2::uuid AND lease_until>clock_timestamp()`, id, token, next.UTC().Truncate(time.Microsecond), code, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) Block(ctx context.Context, id, token, code string, now time.Time) error {
	now = now.UTC().Truncate(time.Microsecond)
	tag, err := s.Pool.Exec(ctx, `UPDATE `+s.outbox+` SET status='blocked',lease_token=NULL,lease_until=NULL,last_error_code=$3,updated_at=$4 WHERE occurrence_id=$1::uuid AND status='leased' AND lease_token=$2::uuid AND lease_until>clock_timestamp()`, id, token, code, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrLeaseLost
	}
	return nil
}

func (s *Store) RetryBlocked(ctx context.Context, id string, now time.Time) (string, error) {
	now = now.UTC().Truncate(time.Microsecond)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var status string
	err = tx.QueryRow(ctx, `SELECT status FROM `+s.outbox+` WHERE occurrence_id=$1::uuid FOR UPDATE`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrOccurrenceNotFound
	}
	if err != nil {
		return "", err
	}
	switch status {
	case "accepted":
		return status, ErrOccurrenceNotRetryable
	case "blocked":
		_, err = tx.Exec(ctx, `UPDATE `+s.outbox+` SET status='pending',available_at=$2,lease_token=NULL,lease_until=NULL,last_error_code=NULL,updated_at=$2 WHERE occurrence_id=$1::uuid`, id, now.UTC())
		if err != nil {
			return "", err
		}
		status = "pending"
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	return status, nil
}

type Status struct {
	Paused          bool       `json:"paused"`
	ControlRevision int64      `json:"control_revision"`
	LastTickAt      *time.Time `json:"last_tick_at"`
	LastDispatchAt  *time.Time `json:"last_dispatch_at"`
	PendingOutbox   int64      `json:"pending_outbox"`
}

func (s *Store) GetStatus(ctx context.Context) (Status, error) {
	var st Status
	err := s.Pool.QueryRow(ctx, `SELECT c.paused,c.revision,ss.last_tick_at,ss.last_dispatch_at,(SELECT count(*) FROM `+s.outbox+` WHERE status<>'accepted') FROM `+s.control+` c CROSS JOIN `+s.state+` ss WHERE c.id=1 AND ss.id=1`).Scan(&st.Paused, &st.ControlRevision, &st.LastTickAt, &st.LastDispatchAt, &st.PendingOutbox)
	return st, err
}

func (s *Store) ListOccurrences(ctx context.Context, key, cursor string, limit int) ([]model.Occurrence, string, error) {
	var created time.Time
	id := "00000000-0000-0000-0000-000000000000"
	if cursor != "" {
		b, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return nil, "", ErrInvalidCursor
		}
		parts := strings.SplitN(string(b), "|", 2)
		if len(parts) != 2 {
			return nil, "", ErrInvalidCursor
		}
		created, err = time.Parse(time.RFC3339Nano, parts[0])
		if err != nil || !canonicalCursorUUID(parts[1]) {
			return nil, "", ErrInvalidCursor
		}
		id = parts[1]
	}
	var rows pgx.Rows
	var err error
	const occurrenceColumns = `occurrence_id::text,schedule_key,schedule_revision,scheduled_for,target_alias,status,lease_until,COALESCE(status='leased' AND lease_until>clock_timestamp(),FALSE),attempts,created_at,updated_at,job_id::text`
	if key != "" {
		rows, err = s.Pool.Query(ctx, `SELECT `+occurrenceColumns+` FROM `+s.outbox+` WHERE schedule_key=$1 AND (created_at,occurrence_id)>($2,$3::uuid) ORDER BY created_at,occurrence_id LIMIT $4`, key, created, id, limit+1)
	} else {
		rows, err = s.Pool.Query(ctx, `SELECT `+occurrenceColumns+` FROM `+s.outbox+` WHERE (created_at,occurrence_id)>($1,$2::uuid) ORDER BY created_at,occurrence_id LIMIT $3`, created, id, limit+1)
	}
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	result := []model.Occurrence{}
	for rows.Next() {
		var x model.Occurrence
		var job *string
		if err := rows.Scan(&x.ID, &x.ScheduleKey, &x.ScheduleRevision, &x.ScheduledFor, &x.TargetAlias, &x.Status, &x.LeaseExpiresAt, &x.LeaseActive, &x.Attempts, &x.CreatedAt, &x.UpdatedAt, &job); err != nil {
			return nil, "", err
		}
		x.JobID = job
		result = append(result, x)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(result) <= limit {
		return result, "", nil
	}
	result = result[:limit]
	last := result[len(result)-1]
	raw := last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID
	return result, base64.RawURLEncoding.EncodeToString([]byte(raw)), nil
}

func canonicalCursorUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, char := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if char != '-' {
				return false
			}
			continue
		}
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func (s *Store) MarkTick(ctx context.Context, now time.Time) error {
	_, err := s.Pool.Exec(ctx, `UPDATE `+s.state+` SET last_tick_at=$1 WHERE id=1`, now.UTC())
	return err
}

type rowScanner interface{ Scan(...any) error }

func scanSchedule(row rowScanner) (model.Schedule, error) {
	var x model.Schedule
	var calJSON, payloadJSON []byte
	var next *time.Time
	err := row.Scan(&x.Key, &x.Revision, &x.Enabled, &calJSON, &x.Target, &x.Operation, &payloadJSON, &next, &x.CreatedAt, &x.UpdatedAt)
	if err != nil {
		return x, err
	}
	if err = json.Unmarshal(calJSON, &x.Calendar); err != nil {
		return x, err
	}
	if err = decodeJSONMap(payloadJSON, &x.Payload); err != nil {
		return x, err
	}
	x.NextDueAt = next
	x.CreatedAt = x.CreatedAt.UTC()
	x.UpdatedAt = x.UpdatedAt.UTC()
	if x.NextDueAt != nil {
		t := x.NextDueAt.UTC()
		x.NextDueAt = &t
	}
	return x, nil
}

func decodeJSONMap(data []byte, dst *map[string]any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()
	return decoder.Decode(dst)
}

func sameConfig(ctx context.Context, tx pgx.Tx, schedules string, current model.Schedule, m model.Mutation) (bool, error) {
	if current.Enabled != m.Enabled || !reflect.DeepEqual(calendar.Normalize(current.Calendar), calendar.Normalize(m.Calendar)) || current.Target != m.Target || current.Operation != m.Operation {
		return false, nil
	}
	payloadJSON, err := json.Marshal(m.Payload)
	if err != nil {
		return false, err
	}
	var payloadEqual bool
	err = tx.QueryRow(ctx, `SELECT payload=$2::jsonb FROM `+schedules+` WHERE schedule_key=$1`, current.Key, string(payloadJSON)).Scan(&payloadEqual)
	return payloadEqual, err
}
func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.UTC().Equal(b.UTC())
}

func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:]), nil
}
