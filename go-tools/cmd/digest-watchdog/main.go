// digest-watchdog is an independent, one-shot operator check. Run from a timer.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type occurrence struct {
	Key          string
	Due          time.Time
	Enabled      bool
	Paused       bool
	SendTelegram bool
}
type state struct {
	Occurrences map[string]occurrence `json:"occurrences"`
	Alerted     map[string]bool       `json:"alerted"`
}
type assessment struct {
	status  string
	unknown bool
	pending bool
	missing []string
}

func decision(now time.Time, o occurrence, a assessment, readErr bool) string {
	if !o.Enabled || o.Paused || !o.SendTelegram || now.Before(o.Due) {
		return ""
	}
	if a.unknown {
		if len(a.missing) > 0 {
			return "Delivery outcome is unknown: " + strings.Join(a.missing, ", ")
		}
		return "Delivery outcome is unknown"
	}
	if readErr {
		if !now.Before(o.Due.Add(30 * time.Minute)) {
			return "Cannot verify digest status: database unavailable"
		}
		return ""
	}
	if a.pending {
		return ""
	}
	if !now.Before(o.Due.Add(30*time.Minute)) && (a.status != "succeeded" || len(a.missing) > 0) {
		if len(a.missing) > 0 {
			return "Digest delivery not confirmed: " + strings.Join(a.missing, ", ")
		}
		return "Digest is missing or did not complete"
	}
	return ""
}
func trackOccurrence(o occurrence, from time.Time) bool { return !o.Due.Before(from) }
func key(o occurrence) string                           { return o.Key + "/" + o.Due.UTC().Format(time.RFC3339Nano) }
func load(path string) (state, error) {
	s := state{map[string]occurrence{}, map[string]bool{}}
	b, e := os.ReadFile(path)
	if errors.Is(e, os.ErrNotExist) {
		return s, nil
	}
	if e != nil {
		return s, e
	}
	if e = json.Unmarshal(b, &s); e != nil {
		return s, e
	}
	if s.Occurrences == nil || s.Alerted == nil {
		return s, errors.New("invalid state")
	}
	return s, nil
}
func save(path string, s state) error {
	b, e := json.Marshal(s)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".watchdog-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	if c := f.Close(); e == nil {
		e = c
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func send(ctx context.Context, client *http.Client, endpoint, token, chat, msg string) error {
	v := url.Values{"chat_id": {chat}, "text": {msg}}
	req, e := http.NewRequestWithContext(ctx, "POST", endpoint+"/bot"+token+"/sendMessage", strings.NewReader(v.Encode()))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, e := client.Do(req)
	if e != nil {
		return errors.New("telegram outcome unconfirmed")
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return errors.New("telegram rejected or could not confirm notification")
	}
	var result struct {
		OK bool `json:"ok"`
	}
	if json.NewDecoder(res.Body).Decode(&result) != nil || !result.OK {
		return errors.New("telegram outcome unconfirmed")
	}
	return nil
}
func readSchedules(ctx context.Context, p *pgxpool.Pool, schema string) ([]occurrence, error) {
	q := fmt.Sprintf(`SELECT s.schedule_key,s.next_due_at,s.enabled,c.paused,(s.payload->'send_telegram' IS DISTINCT FROM 'false'::jsonb) FROM %s.schedules s CROSS JOIN %s.control c WHERE c.id=1 AND s.operation IN ('news_digest','telegram_digest') AND s.next_due_at IS NOT NULL UNION ALL SELECT o.schedule_key,o.scheduled_for,s.enabled,c.paused,(o.payload->'send_telegram' IS DISTINCT FROM 'false'::jsonb) FROM %s.occurrences o JOIN %s.schedules s USING(schedule_key) CROSS JOIN %s.control c WHERE c.id=1 AND o.operation IN ('news_digest','telegram_digest') AND o.scheduled_for >= now()-interval '48 hours'`, schema, schema, schema, schema, schema)
	rows, e := p.Query(ctx, q)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []occurrence
	for rows.Next() {
		var o occurrence
		if e = rows.Scan(&o.Key, &o.Due, &o.Enabled, &o.Paused, &o.SendTelegram); e != nil {
			return nil, e
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

type jobConfig struct {
	Settings struct {
		DeliveryEnabled bool `json:"delivery_enabled"`
		SerbianEnabled  bool `json:"serbian_enabled"`
	} `json:"settings"`
	Telegram *struct {
		SerbianDeliveryEnabled bool   `json:"serbian_delivery_enabled"`
		ChatID                 string `json:"chat_id"`
		SerbianChatID          string `json:"serbian_chat_id"`
	} `json:"telegram"`
	Viber *struct {
		DeliveryEnabled bool   `json:"delivery_enabled"`
		WebhookReady    bool   `json:"webhook_ready"`
		ChannelID       string `json:"channel_id"`
	} `json:"viber"`
}

func refreshControls(ctx context.Context, p *pgxpool.Pool, schema string, st *state) error {
	rows, err := p.Query(ctx, fmt.Sprintf(`SELECT s.schedule_key,s.enabled,c.paused FROM %s.schedules s CROSS JOIN %s.control c WHERE c.id=1 AND s.operation IN ('news_digest','telegram_digest')`, schema, schema))
	if err != nil {
		return err
	}
	defer rows.Close()
	controls := map[string]occurrence{}
	for rows.Next() {
		var o occurrence
		if err = rows.Scan(&o.Key, &o.Enabled, &o.Paused); err != nil {
			return err
		}
		controls[o.Key] = o
	}
	if err = rows.Err(); err != nil {
		return err
	}
	for k, o := range st.Occurrences {
		c, ok := controls[o.Key]
		o.Enabled = ok && c.Enabled
		o.Paused = c.Paused
		st.Occurrences[k] = o
	}
	return nil
}
func readJob(ctx context.Context, p *pgxpool.Pool, o occurrence, now time.Time) (assessment, error) {
	var a assessment
	var retry *time.Time
	var id string
	var gen int
	var payload []byte
	var deliveryStarted *bool
	e := p.QueryRow(ctx, `SELECT j.id::text,j.state,j.digest_retry_at,j.attempt_generation,j.payload,(SELECT x.digest_delivery_started FROM background_job_executions x WHERE x.job_id=j.id AND x.attempt_generation=j.attempt_generation) FROM background_jobs j WHERE j.schedule_key=$1 AND j.scheduled_for=$2::timestamptz AT TIME ZONE 'UTC' ORDER BY j.created_at DESC LIMIT 1`, o.Key, o.Due).Scan(&id, &a.status, &retry, &gen, &payload, &deliveryStarted)
	if errors.Is(e, pgx.ErrNoRows) {
		var enabled bool
		err := p.QueryRow(ctx, `SELECT COALESCE((SELECT (value #>> '{}')::boolean FROM settings WHERE key='operational_delivery_enabled'),true)`).Scan(&enabled)
		if err != nil {
			return a, err
		}
		if !enabled {
			a.status = "succeeded"
		}
		return a, nil
	}
	if e != nil {
		return a, e
	}
	a.unknown = a.status == "delivery_unknown" && (deliveryStarted == nil || *deliveryStarted)
	a.pending = retry != nil && now.Before(o.Due.Add(30*time.Minute))
	var request struct {
		SendTelegram *bool `json:"send_telegram"`
	}
	if json.Unmarshal(payload, &request) != nil {
		return a, errors.New("invalid job payload")
	}
	if request.SendTelegram != nil && !*request.SendTelegram {
		return a, nil
	}
	var raw []byte
	e = p.QueryRow(ctx, `SELECT json_build_object(
        'settings',json_build_object('delivery_enabled',value->'settings'->'delivery_enabled','serbian_enabled',value->'settings'->'serbian_enabled'),
        'telegram',CASE WHEN value->'telegram' IS NULL THEN NULL ELSE json_build_object('chat_id',value->'telegram'->'chat_id','serbian_chat_id',value->'telegram'->'serbian_chat_id','serbian_delivery_enabled',value->'telegram'->'serbian_delivery_enabled') END,
        'viber',CASE WHEN value->'viber' IS NULL THEN NULL ELSE json_build_object('delivery_enabled',value->'viber'->'delivery_enabled','webhook_ready',value->'viber'->'webhook_ready','channel_id',value->'viber'->'channel_id') END) FROM settings WHERE key=$1 AND json_typeof(value->'settings'->'delivery_enabled')='boolean' AND json_typeof(value->'settings'->'serbian_enabled')='boolean'`, fmt.Sprintf("managed_job_config:%s:%d", id, gen)).Scan(&raw)
	if errors.Is(e, pgx.ErrNoRows) {
		if a.status == "succeeded" {
			a.missing = append(a.missing, "delivery settings unavailable")
		}
		return a, nil
	}
	if e != nil {
		return a, e
	}
	var cfg jobConfig
	if json.Unmarshal(raw, &cfg) != nil {
		return a, errors.New("invalid job settings")
	}
	if !cfg.Settings.DeliveryEnabled {
		return assessment{status: "succeeded"}, nil
	}
	// The scheduled run can reuse an edition saved by a different job.
	// Follow the immutable publication selection when it exists.
	var russianEdition string
	err := p.QueryRow(ctx, `SELECT value->>'russian_edition_id' FROM settings
		WHERE key=$1 AND value->>'job_id'=$2 AND value->>'attempt_generation'=$3`,
		fmt.Sprintf("managed_digest_publication:%s:%d", id, gen), id, fmt.Sprint(gen)).Scan(&russianEdition)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return a, err
	}
	locales := []string{"ru"}
	if cfg.Settings.SerbianEnabled && cfg.Telegram != nil && cfg.Telegram.SerbianDeliveryEnabled {
		locales = append(locales, "sr-Latn")
	}
	for _, locale := range locales {
		destination := ""
		if cfg.Telegram != nil {
			destination = cfg.Telegram.ChatID
			if locale == "sr-Latn" {
				destination = cfg.Telegram.SerbianChatID
			}
		}
		if destination == "" {
			a.missing = append(a.missing, "Telegram "+locale+" configuration")
			continue
		}
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(destination)))
		var completed, uncertain bool
		e = p.QueryRow(ctx, `WITH selected AS (
			SELECT id,job_id,attempt_generation FROM editorial_digest_editions
			WHERE id=NULLIF($4,'')::uuid AND locale='ru'
		), intents AS (
			SELECT d.id,d.state FROM editorial_digest_editions e
			JOIN digest_delivery_intents d ON d.edition_id=e.id
			WHERE e.locale=$2 AND d.destination_hash=$3 AND (
				($4='' AND e.job_id=$1::uuid AND e.attempt_generation=$5)
				OR EXISTS (SELECT 1 FROM selected s WHERE s.id=e.id OR
					($2='sr-Latn' AND (e.job_id,e.attempt_generation)=(s.job_id,s.attempt_generation)))
			)
		)
		SELECT EXISTS(SELECT 1 FROM intents WHERE state='completed'),
			EXISTS(SELECT 1 FROM intents i JOIN digest_delivery_parts p ON p.intent_id=i.id WHERE p.state='unknown')`,
			id, locale, hash, russianEdition, gen).Scan(&completed, &uncertain)
		if e != nil {
			return a, e
		}
		if uncertain && !completed {
			a.unknown = true
		}
		if !completed {
			a.missing = append(a.missing, "Telegram "+locale)
		}
	}
	if cfg.Settings.SerbianEnabled && cfg.Viber != nil && cfg.Viber.DeliveryEnabled && cfg.Viber.WebhookReady {
		var completed, uncertain, pending bool
		e = p.QueryRow(ctx, `WITH deliveries AS (
			SELECT d.state FROM viber_digest_deliveries d
			JOIN editorial_digest_editions e ON e.id=d.russian_edition_id
			WHERE d.channel_id=$2 AND (e.id=NULLIF($3,'')::uuid
				OR ($3='' AND e.job_id=$1::uuid AND e.attempt_generation=$4))
		)
		SELECT EXISTS(SELECT 1 FROM deliveries WHERE state='completed'),
			EXISTS(SELECT 1 FROM deliveries WHERE state='delivery_unknown'),
			EXISTS(SELECT 1 FROM deliveries WHERE state IN ('waiting_edition','ready','retry_wait'))`,
			id, cfg.Viber.ChannelID, russianEdition, gen).Scan(&completed, &uncertain, &pending)
		if e != nil {
			return a, e
		}
		if uncertain && !completed {
			a.unknown = true
		}
		if pending && now.Before(o.Due.Add(30*time.Minute)) {
			a.pending = true
		}
		if !completed {
			a.missing = append(a.missing, "Viber sr-Latn")
		}
	}
	if len(a.missing) == 0 {
		a.unknown = false
		a.status = "succeeded"
	}
	return a, nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "digest watchdog:", e)
		os.Exit(1)
	}
}
func run() error {
	path := os.Getenv("WATCHDOG_STATE_FILE")
	schedulerURL := os.Getenv("WATCHDOG_SCHEDULER_DATABASE_URL")
	newsURL := os.Getenv("WATCHDOG_NEWS_DATABASE_URL")
	token := os.Getenv("WATCHDOG_TELEGRAM_TOKEN")
	chat := os.Getenv("WATCHDOG_TELEGRAM_CHAT_ID")
	monitorFrom, err := time.Parse(time.RFC3339, os.Getenv("WATCHDOG_MONITOR_FROM"))
	if err != nil {
		return errors.New("WATCHDOG_MONITOR_FROM must be an RFC3339 timestamp")
	}
	schema := os.Getenv("WATCHDOG_SCHEDULER_SCHEMA")
	archive := os.Getenv("WATCHDOG_ARCHIVE_URL")
	if archive != "" {
		u, err := url.Parse(archive)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return errors.New("invalid archive URL")
		}
	}
	if schema == "" {
		schema = "public"
	}
	if path == "" || schedulerURL == "" || newsURL == "" || token == "" || chat == "" {
		return errors.New("missing required configuration")
	}
	for _, r := range schema {
		if !(r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return errors.New("invalid scheduler schema")
		}
	}
	schema = `"` + schema + `"`
	lock, e := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return errors.New("cannot open state lock")
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return errors.New("another check is running")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	s, e := load(path)
	if e != nil {
		return errors.New("cannot read durable state")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	sched, e := pgxpool.New(ctx, schedulerURL)
	if e != nil {
		return errors.New("cannot configure scheduler database")
	}
	defer sched.Close()
	news, e := pgxpool.New(ctx, newsURL)
	if e != nil {
		return errors.New("cannot configure news database")
	}
	defer news.Close()
	now := time.Now().UTC()
	schedCtx, schedCancel := context.WithTimeout(ctx, 8*time.Second)
	list, readErr := readSchedules(schedCtx, sched, schema)
	if readErr == nil {
		for _, o := range list {
			if trackOccurrence(o, monitorFrom) {
				s.Occurrences[key(o)] = o
			}
		}
		readErr = refreshControls(schedCtx, sched, schema, &s)
		if readErr == nil {
			if e = save(path, s); e != nil {
				return errors.New("cannot persist observations")
			}
		}
	}
	schedCancel()
	for k, o := range s.Occurrences {
		if !trackOccurrence(o, monitorFrom) || now.After(o.Due.Add(48*time.Hour)) {
			delete(s.Occurrences, k)
			delete(s.Alerted, k)
			continue
		}
		if s.Alerted[k] || !o.Enabled || o.Paused || !o.SendTelegram || now.Before(o.Due) {
			continue
		}
		jobCtx, jobCancel := context.WithTimeout(ctx, 8*time.Second)
		a, err := readJob(jobCtx, news, o, now)
		jobCancel()
		if err != nil {
			a = assessment{}
		}
		msg := decision(now, o, a, readErr != nil || err != nil)
		if msg == "" {
			continue
		}
		if e = reserveAlert(ctx, path, s, k); e != nil {
			return e
		}
		msg = fmt.Sprintf("Evening News: %s\nSchedule: %s\nDue: %s", msg, o.Key, o.Due.UTC().Format(time.RFC3339))
		if archive != "" {
			msg += "\nArchive: " + archive
		}
		if e = send(ctx, http.DefaultClient, "https://api.telegram.org", token, chat, msg); e != nil {
			return e
		}
	}
	if e = save(path, s); e != nil {
		return errors.New("cannot persist state")
	}
	if readErr != nil {
		return errors.New("cannot read digest schedules")
	}
	return nil
}

func reserveAlert(ctx context.Context, path string, s state, key string) error {
	if ctx.Err() != nil {
		return errors.New("watchdog run deadline reached before notification")
	}
	s.Alerted[key] = true
	if err := save(path, s); err != nil {
		return errors.New("cannot persist notification reservation")
	}
	return nil
}
