package terminalevents

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strconv"
	"strings"
)

func scheduleIdentity(j terminalJob) string {
	if j.ScheduleKey != nil && *j.ScheduleKey != "" && len([]rune(*j.ScheduleKey)) <= 128 {
		return "key:" + *j.ScheduleKey
	}
	var payload map[string]any
	if json.Unmarshal(j.Payload, &payload) != nil {
		return ""
	}
	value, exists := payload["schedule_id"]
	if !exists {
		return ""
	}
	var id string
	switch v := value.(type) {
	case string:
		id = v
	case float64:
		if v != float64(int64(v)) {
			return ""
		}
		id = strconv.FormatInt(int64(v), 10)
	default:
		return ""
	}
	if id == "" || len([]rune(id)) > 128 {
		return ""
	}
	return "id:" + id
}
func previousPosition(ctx context.Context, tx pgx.Tx, j terminalJob, identity string) (*int64, error) {
	var n int64
	var err error
	if strings.HasPrefix(identity, "key:") {
		err = tx.QueryRow(ctx, `SELECT queue_position FROM background_jobs WHERE kind='news_processing' AND queue_position<$1 AND schedule_key=$2 ORDER BY queue_position DESC LIMIT 1`, j.QueuePosition, identity[4:]).Scan(&n)
	} else {
		err = tx.QueryRow(ctx, `SELECT queue_position FROM background_jobs WHERE kind='news_processing' AND queue_position<$1 AND (schedule_key IS NULL OR schedule_key='') AND payload->>'schedule_id'=$2 ORDER BY queue_position DESC LIMIT 1`, j.QueuePosition, identity[3:]).Scan(&n)
	}
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &n, nil
}
func loadObservation(ctx context.Context, tx pgx.Tx, j terminalJob, e Event) (*observation, error) {
	var runID, runState string
	var runResult []byte
	err := tx.QueryRow(ctx, `SELECT id::text,state,result::text FROM article_ingestion_runs WHERE job_id=$1::uuid AND attempt_generation=$2`, e.JobID, e.Attempt).Scan(&runID, &runState, &runResult)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT source_id,source_type,config_sha256,state,error_code FROM article_ingestion_source_runs WHERE run_id=$1::uuid ORDER BY source_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []sourceRecord{}
	for rows.Next() {
		var v sourceRecord
		if err = rows.Scan(&v.ID, &v.Type, &v.Config, &v.State, &v.Code); err != nil {
			return nil, err
		}
		items = append(items, v)
		if len(items) > 64 {
			return nil, nil
		}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return classifyObservation(j.State, runState, j.Result, runResult, items), nil
}
func loadLedger(ctx context.Context, tx pgx.Tx, identity string, create bool) (sourceLedger, bool, error) {
	key := settingKey(identity)
	if create {
		_, err := tx.Exec(ctx, `INSERT INTO settings(key,value,description) VALUES($1,'{"v":1,"last_position":0,"sources":{}}'::json,'Reserved scheduled source alert hysteresis state') ON CONFLICT (key) DO NOTHING`, key)
		if err != nil {
			return sourceLedger{}, false, err
		}
	}
	var data []byte
	err := tx.QueryRow(ctx, `SELECT value::text FROM settings WHERE key=$1 FOR UPDATE`, key).Scan(&data)
	if err == pgx.ErrNoRows {
		return sourceLedger{}, false, nil
	}
	if err != nil {
		return sourceLedger{}, false, err
	}
	v, ok := validSourceLedger(data)
	return v, ok, nil
}
func saveLedger(ctx context.Context, tx pgx.Tx, identity string, v sourceLedger) error {
	data, err := json.Marshal(v)
	if err != nil || len(data) > 12*1024 || len(v.Sources) > 64 {
		return fmt.Errorf("invalid source alert state")
	}
	_, err = tx.Exec(ctx, `UPDATE settings SET value=$2::json,updated_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE key=$1`, settingKey(identity), data)
	return err
}
func abandonLedger(v *sourceLedger, job string) {
	for _, x := range v.Sources {
		if x.EventJob != nil && *x.EventJob == job && oneOf(x.Incident, "attempting", "recovering") {
			x.Incident = "unknown"
			x.EventJob = nil
		}
	}
}
