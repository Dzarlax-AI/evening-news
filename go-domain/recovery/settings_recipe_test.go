package recovery_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/callbacks"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/learning"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This gate runs the documented psql files, not a second implementation of them.
// Its opt-in database must be an isolated local fixture, never a shared target.
func TestSettingsRecoveryRecipes(t *testing.T) {
	dsn := os.Getenv("NEWS_SETTINGS_RECOVERY_TEST_URL")
	if dsn == "" {
		t.Skip("isolated settings recovery fixture not configured")
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil || (cfg.Database != "settings_recovery_fixture" && cfg.Database != "viber_fixture") || (cfg.Host != "127.0.0.1" && cfg.Host != "localhost") {
		t.Fatal("fixture requires explicit loopback settings_recovery_fixture or viber_fixture database")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("fixture connection failed")
	}
	defer admin.Close(ctx)
	raw := make([]byte, 8)
	_, _ = rand.Read(raw)
	suffix := hex.EncodeToString(raw)
	source := "news_restore_src_" + suffix
	target := "news_restore_dst_" + suffix
	pools := []*pgxpool.Pool{}
	databases := []string{source, target}
	defer func() {
		for _, p := range pools {
			p.Close()
		}
		for _, db := range databases {
			_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{db}.Sanitize()+" WITH (FORCE)")
		}
	}()
	connect := func(db string) *pgxpool.Pool {
		t.Helper()
		c, e := pgxpool.ParseConfig(dsn)
		if e != nil {
			t.Fatal("fixture pool config failed")
		}
		c.ConnConfig.Database = db
		c.ConnConfig.RuntimeParams["search_path"] = "news"
		p, e := pgxpool.NewWithConfig(ctx, c)
		if e != nil {
			t.Fatal("fixture pool failed")
		}
		pools = append(pools, p)
		return p
	}
	for _, db := range []string{source, target} {
		if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{db}.Sanitize()); err != nil {
			t.Fatal("fixture database creation failed")
		}
		c := cfg.Copy()
		c.Database = db
		conn, e := pgx.ConnectConfig(ctx, c)
		if e != nil {
			t.Fatal("fixture schema connection failed")
		}
		_, e = conn.Exec(ctx, "CREATE SCHEMA news; CREATE SCHEMA neighbor; CREATE TABLE neighbor.sentinel(id integer); INSERT INTO neighbor.sentinel VALUES(7)")
		conn.Close(ctx)
		if e != nil {
			t.Fatal("fixture schema creation failed")
		}
		p := connect(db)
		r, e := migration.Run(ctx, p, true, false)
		if e != nil || len(r.Errors) != 0 {
			t.Fatal("fixture migration failed")
		}
		p.Close()
	}
	src := connect(source)
	installation := strings.Repeat("a", 32)
	key := bytes.Repeat([]byte{3}, 32)
	envelope, err := credentialstore.Seal(key, "fixture-key", installation, "gemini", []byte("synthetic-key-no-provider-call"))
	if err != nil {
		t.Fatal("fixture encryption failed")
	}
	viberEnvelope, err := credentialstore.Seal(key, "fixture-key", installation, "viber", []byte("synthetic-viber-token-no-api-call"))
	if err != nil {
		t.Fatal("Viber fixture encryption failed")
	}
	_, err = src.Exec(ctx, `INSERT INTO installation_state(id,installation_id,provisioning_complete,onboarding_completed) VALUES(1,$1,true,true);`, installation)
	if err != nil {
		t.Fatal("fixture identity seed failed")
	}
	_, err = src.Exec(ctx, `INSERT INTO managed_admin(id,username,password_hash) VALUES(1,'fixture-admin','synthetic-password-hash'); INSERT INTO settings(key,value) VALUES('ai_settings_revision','3'),('operational_delivery_enabled','true'),('operational_processing_enabled','true'),('telegram_chat_id','"synthetic-chat"'),('telegram_serbian_chat_id','"synthetic-sr"'),('telegram_serbian_delivery_enabled','true'),('operational_serbian_enabled','true'),('managed_job_config:retired','{"content":"must-not-be-exported"}');`)
	if err != nil {
		t.Fatal("fixture settings seed failed")
	}
	_, err = src.Exec(ctx, `INSERT INTO categories(name,display_name,color) VALUES('Fixture','Fixture category','#123456');
 INSERT INTO category_localizations(category_id,locale,display_name) SELECT id,'sr-Latn','Probna kategorija' FROM categories WHERE name='Fixture';
 INSERT INTO category_mapping(ai_category,fixed_category,created_by,usage_count) VALUES('Fixture input','Fixture','fixture',19);
 INSERT INTO extraction_patterns(domain,selector_pattern,extraction_strategy,discovered_by,success_count) VALUES('fixture.invalid','article','css','manual',9),('fixture.invalid','aside','css','ai',12);
 INSERT INTO integration_credentials(provider,revision) VALUES('openai',4);
	 INSERT INTO settings(key,value) VALUES
 ('ai_settings','{"active_provider":"gemini","profiles":{"gemini":{"models":{"article":"fixture-model","digest":"fixture-model","text":"fixture-model"}},"openai":{"models":{"article":"fixture-a","digest":"fixture-d","text":"fixture-t"}}}}'),
 ('viber_settings','{"revision":8,"delivery_enabled":true,"channel_id":"fixture-channel","channel_name":"Fixture Channel","sender_id":"fixture-sender","credential_revision":5,"verified_credential_revision":5,"webhook_ready":true,"verified_at":"2026-10-31T10:00:00Z","enabled_from":"2026-10-30T10:00:00Z"}');`)
	if err != nil {
		t.Fatal("fixture configuration seed failed")
	}
	_, err = src.Exec(ctx, `INSERT INTO integration_credentials(provider,revision,envelope_version,key_id,nonce,ciphertext) VALUES('gemini',1,$1,$2,$3,$4)`, envelope.Version, envelope.KeyID, envelope.Nonce, envelope.Ciphertext)
	if err != nil {
		t.Fatal("fixture credential seed failed")
	}
	_, err = src.Exec(ctx, `INSERT INTO integration_credentials(provider,revision,envelope_version,key_id,nonce,ciphertext) VALUES('viber',5,$1,$2,$3,$4)`, viberEnvelope.Version, viberEnvelope.KeyID, viberEnvelope.Nonce, viberEnvelope.Ciphertext)
	if err != nil {
		t.Fatal("Viber fixture credential seed failed")
	}
	for i := 1; i <= 8; i++ {
		typ := "rss"
		if i == 1 {
			typ = "telegram"
		}
		if i > 5 {
			typ = "custom"
		}
		_, err = src.Exec(ctx, `INSERT INTO sources(id,name,source_type,url,enabled,fetch_interval,config,last_fetch,error_count) VALUES($1,$2,$3,'https://fixture.invalid',true,60,'{"limit":5,"article_selectors":["article"],"offset":99,"previous_hashes":{"x":"y"},"last_error":"old"}',now(),9)`, i, fmt.Sprintf("synthetic-source-%d", i), typ)
		if err != nil {
			t.Fatal("fixture source seed failed")
		}
	}
	src.Close()
	dir := t.TempDir()
	runtime, _ := json.MarshalIndent(map[string]any{"version": 1, "service": "background", "installation_id": installation, "values": map[string]string{"NEWS_LIMIT_MAX_ARTICLES": "17", "NEWS_LIMIT_PER_SOURCE": "8"}}, "", "  ")
	if err = os.WriteFile(filepath.Join(dir, "background.json"), runtime, 0600); err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if json.Compact(&compact, runtime) != nil {
		t.Fatal("runtime input normalization failed")
	}
	if os.WriteFile(filepath.Join(dir, "background-input.json"), compact.Bytes(), 0600) != nil {
		t.Fatal("runtime input write failed")
	}
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	psql := func(db, file string, extra ...string) ([]byte, error) {
		t.Helper()
		c := exec.Command("psql", append([]string{"-XqAt", "-v", "ON_ERROR_STOP=1", "-h", cfg.Host, "-p", fmt.Sprint(cfg.Port), "-U", cfg.User, "-d", db, "-f", filepath.Join(repo, "deploy/recovery", file)}, extra...)...)
		c.Dir = dir
		c.Env = append(os.Environ(), "PGPASSWORD="+cfg.Password)
		out, err := c.Output()
		return out, err
	}
	data, err := psql(source, "export-settings.sql")
	if err != nil {
		t.Fatal("documented export failed")
	}
	if bytes.Contains(data, []byte("must-not-be-exported")) || bytes.Contains(data, []byte("previous_hashes")) || bytes.Contains(data, []byte("last_error")) {
		t.Fatal("volatile state escaped export allowlist")
	}
	var document map[string]any
	if json.Unmarshal(data, &document) != nil {
		t.Fatal("export is not one JSON document")
	}
	if document["schema_version"] != float64(20) {
		t.Fatal("export did not use schema version 20")
	}
	var exportedViber bool
	for _, row := range document["settings"].([]any) {
		if row.(map[string]any)["key"] == "viber_settings" {
			exportedViber = true
		}
	}
	if !exportedViber || bytes.Contains(data, []byte("viber_webhook_ack")) || bytes.Contains(data, []byte("viber_digest_deliveries")) || bytes.Contains(data, []byte("viber_digest_parts")) {
		t.Fatal("Viber setting missing or transient Viber history escaped settings export")
	}
	if err = os.WriteFile(filepath.Join(dir, "settings.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = psql(target, "import-settings.sql", "-v", "target_database="+target, "-v", "installation_id="+installation); err != nil {
		t.Fatal("documented import failed")
	}
	restored, e := psql(target, "export-settings.sql")
	var restoredDocument map[string]any
	if e != nil || json.Unmarshal(restored, &restoredDocument) != nil {
		t.Fatal("recovered export failed")
	}
	for _, section := range []string{"identity", "sources", "categories", "category_localizations", "category_mapping", "extraction_patterns", "schedule_settings", "managed_admin", "integration_credentials"} {
		if !reflect.DeepEqual(document[section], restoredDocument[section]) {
			t.Fatalf("recovered %s differs", section)
		}
	}
	for _, row := range document["settings"].([]any) {
		item := row.(map[string]any)
		if strings.HasPrefix(item["key"].(string), "operational_") || item["key"] == "telegram_serbian_delivery_enabled" || item["key"] == "viber_settings" {
			continue
		}
		found := false
		for _, recovered := range restoredDocument["settings"].([]any) {
			if reflect.DeepEqual(item, recovered) {
				found = true
			}
		}
		if !found {
			t.Fatal("recovered non-operational setting differs")
		}
	}
	operatorEnvelopeRoundTrip(t, dir, installation, key)
	dst := connect(target)
	pattern, patternErr := learning.New(dst).Context(ctx, callbacks.Request{RunID: "fixture", ArticleID: 1, ArticleAttempt: 1, InputFingerprint: "fixture", Input: json.RawMessage(`{"url":"https://fixture.invalid/article"}`)}, authority.Fence{})
	if patternErr != nil || pattern.Selector == nil || *pattern.Selector != "article" {
		t.Fatal("recovered manual extraction rule is not selected")
	}

	var count, sentinel int
	var paused bool
	if dst.QueryRow(ctx, `SELECT count(*) FROM sources`).Scan(&count) != nil || count != 8 {
		t.Fatal("source count mismatch")
	}
	if dst.QueryRow(ctx, `SELECT maintenance_mode FROM installation_state WHERE id=1`).Scan(&paused) != nil || !paused {
		t.Fatal("restore released maintenance")
	}
	op, err := (appsettings.Store{Pool: dst}).Operational(ctx)
	if err != nil || op.DeliveryEnabled || op.MaxArticles != 17 || op.PerSource != 8 {
		t.Fatal("delivery must remain disabled")
	}
	telegram, err := (appsettings.Store{Pool: dst}).Telegram(ctx)
	if err != nil || telegram.SerbianDeliveryEnabled || telegram.SerbianChatID != "synthetic-sr" {
		t.Fatal("Serbian destination not preserved disabled after recovery")
	}
	viber, err := (appsettings.Store{Pool: dst}).Viber(ctx)
	if err != nil || viber.DeliveryEnabled || viber.WebhookReady || viber.VerifiedCredentialRevision != 0 || viber.VerifiedAt != nil || viber.EnabledFrom != nil || viber.ChannelID != "fixture-channel" || viber.ChannelName != "Fixture Channel" || viber.SenderID != "fixture-sender" || viber.CredentialRevision != 5 {
		t.Fatal("Viber settings did not restore disabled and unverified with channel configuration preserved")
	}
	viberClear, _, err := (&credentialstore.Store{Pool: dst, Key: key, KeyID: "fixture-key", InstallationID: installation}).Resolve(ctx, "viber")
	if err != nil || string(viberClear) != "synthetic-viber-token-no-api-call" {
		t.Fatal("Viber encrypted credential recovery mismatch")
	}
	clear, _, err := (&credentialstore.Store{Pool: dst, Key: key, KeyID: "fixture-key", InstallationID: installation}).Resolve(ctx, "gemini")
	if err != nil || string(clear) != "synthetic-key-no-provider-call" {
		t.Fatal("credential recovery mismatch")
	}
	if _, _, err = (&credentialstore.Store{Pool: dst, Key: bytes.Repeat([]byte{4}, 32), KeyID: "fixture-key", InstallationID: installation}).Resolve(ctx, "gemini"); err == nil {
		t.Fatal("wrong credential key accepted")
	}
	for _, table := range []string{"articles", "editorial_digest_editions", "background_jobs", "source_collection_state", "setup_tokens", "article_ingestion_runs", "digest_delivery_parts", "viber_digest_deliveries", "viber_digest_parts", "viber_channel_leases", "viber_test_receipts"} {
		if dst.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{"news", table}.Sanitize()).Scan(&count) != nil || count != 0 {
			t.Fatal("history or runtime state restored")
		}
	}
	if dst.QueryRow(ctx, `SELECT count(*) FROM sources WHERE last_fetch IS NOT NULL OR error_count<>0`).Scan(&count) != nil || count != 0 {
		t.Fatal("source state not reset")
	}
	if dst.QueryRow(ctx, `SELECT id FROM neighbor.sentinel`).Scan(&sentinel) != nil || sentinel != 7 {
		t.Fatal("neighbor changed")
	}
	var next int
	if dst.QueryRow(ctx, `INSERT INTO sources(name,source_type,url,enabled,config) VALUES('new','rss','https://fixture.invalid',true,'{}') RETURNING id`).Scan(&next) != nil || next <= 8 {
		t.Fatal("source sequence not recovered")
	}
	dst.Close()
	if _, err = psql(target, "import-settings.sql", "-v", "target_database="+target, "-v", "installation_id="+installation); err == nil {
		t.Fatal("repeat restore accepted populated target")
	}
	before := append([]byte(nil), data...)
	document["schema_version"] = 999
	data, _ = json.Marshal(document)
	_ = os.WriteFile(filepath.Join(dir, "settings.json"), data, 0600)
	if _, err = psql(target, "import-settings.sql", "-v", "target_database="+target, "-v", "installation_id="+installation); err == nil {
		t.Fatal("unsupported schema accepted")
	}
	_ = os.WriteFile(filepath.Join(dir, "settings.json"), before, 0600)
	if _, err = psql(target, "import-settings.sql", "-v", "target_database="+target, "-v", "installation_id="+strings.Repeat("b", 32)); err == nil {
		t.Fatal("wrong installation accepted")
	}
	// Recovery materializes the complete typed operational set even for old or
	// interrupted sources. Compare via the production store, not SQL row counts.
	for index, tc := range []struct {
		name          string
		settings      []map[string]any
		schemaVersion int
		omitViber     bool
		viberValue    any
		maximum       int
		processing    bool
		invalid       bool
		history       bool
	}{
		{name: "schema_16_without_viber", schemaVersion: 16, omitViber: true, maximum: 17, processing: true},
		{name: "schema_18_without_viber", schemaVersion: 18, omitViber: true, settings: []map[string]any{{"key": "operational_settings_revision", "value": 8}, {"key": "operational_max_articles", "value": 23}, {"key": "operational_processing_enabled", "value": false}}, maximum: 23},
		{name: "schema_20_with_viber", schemaVersion: 20, maximum: 17, processing: true},
		{name: "old_history", history: true},
		{name: "invalid_limit", settings: []map[string]any{{"key": "operational_max_articles", "value": 100001}}, invalid: true},
		{name: "viber_missing_field", invalid: true, viberValue: map[string]any{"revision": 8, "delivery_enabled": true, "channel_id": "fixture-channel", "channel_name": "Fixture Channel", "sender_id": "fixture-sender", "credential_revision": 5, "verified_credential_revision": 5, "webhook_ready": true, "enabled_from": "2026-10-30T10:00:00Z"}},
		{name: "viber_unknown_field", invalid: true, viberValue: map[string]any{"revision": 8, "delivery_enabled": true, "channel_id": "fixture-channel", "channel_name": "Fixture Channel", "sender_id": "fixture-sender", "credential_revision": 5, "verified_credential_revision": 5, "webhook_ready": true, "verified_at": "2026-10-31T10:00:00Z", "enabled_from": "2026-10-30T10:00:00Z", "unknown": true}},
		{name: "viber_invalid_revision", invalid: true, viberValue: map[string]any{"revision": "8", "delivery_enabled": true, "channel_id": "fixture-channel", "channel_name": "Fixture Channel", "sender_id": "fixture-sender", "credential_revision": 5, "verified_credential_revision": 5, "webhook_ready": true, "verified_at": "2026-10-31T10:00:00Z", "enabled_from": "2026-10-30T10:00:00Z"}},
		{name: "viber_invalid_timestamp", invalid: true, viberValue: map[string]any{"revision": 8, "delivery_enabled": true, "channel_id": "fixture-channel", "channel_name": "Fixture Channel", "sender_id": "fixture-sender", "credential_revision": 5, "verified_credential_revision": 5, "webhook_ready": true, "verified_at": "not-a-timestamp", "enabled_from": "2026-10-30T10:00:00Z"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := fmt.Sprintf("news_restore_case_%d_%s", index, suffix)
			databases = append(databases, db)
			if _, e := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{db}.Sanitize()); e != nil {
				t.Fatal("case database failed")
			}
			c := cfg.Copy()
			c.Database = db
			conn, e := pgx.ConnectConfig(ctx, c)
			if e != nil {
				t.Fatal("case connection failed")
			}
			_, e = conn.Exec(ctx, "CREATE SCHEMA news")
			conn.Close(ctx)
			if e != nil {
				t.Fatal("case schema failed")
			}
			pool := connect(db)
			r, e := migration.Run(ctx, pool, true, false)
			if tc.history {
				_, e = pool.Exec(ctx, `INSERT INTO daily_summaries(date,category,summary_text,articles_count) VALUES('2026-01-01','Fixture','synthetic-only',1)`)
				if e != nil {
					t.Fatal("history fixture failed")
				}
			}
			pool.Close()
			if e != nil || len(r.Errors) > 0 {
				t.Fatal("case migrations failed")
			}
			var d map[string]any
			if json.Unmarshal(before, &d) != nil {
				t.Fatal("case document failed")
			}
			if tc.schemaVersion != 0 {
				d["schema_version"] = tc.schemaVersion
			}
			settings := []any{}
			for _, x := range d["settings"].([]any) {
				key := x.(map[string]any)["key"].(string)
				if !strings.HasPrefix(key, "operational_") && !(tc.omitViber && key == "viber_settings") && !(tc.viberValue != nil && key == "viber_settings") {
					settings = append(settings, x)
				}
			}
			if tc.omitViber {
				credentials := []any{}
				for _, x := range d["integration_credentials"].([]any) {
					if x.(map[string]any)["provider"] != "viber" {
						credentials = append(credentials, x)
					}
				}
				d["integration_credentials"] = credentials
			}
			for _, x := range tc.settings {
				settings = append(settings, x)
			}
			if tc.viberValue != nil {
				settings = append(settings, map[string]any{"key": "viber_settings", "value": tc.viberValue})
			}
			d["settings"] = settings
			b, _ := json.Marshal(d)
			if os.WriteFile(filepath.Join(dir, "settings.json"), b, 0600) != nil {
				t.Fatal("case input failed")
			}
			_, e = psql(db, "import-settings.sql", "-v", "target_database="+db, "-v", "installation_id="+installation)
			if tc.invalid || tc.history {
				if e == nil {
					t.Fatal("invalid recovery setting accepted")
				}
				pool = connect(db)
				var n int
				if pool.QueryRow(ctx, "SELECT count(*) FROM news.sources").Scan(&n) != nil || n != 0 {
					t.Fatal("failed import changed target")
				}
				pool.Close()
				return
			}
			if e != nil {
				t.Fatal("case import failed")
			}
			pool = connect(db)
			op, e := (appsettings.Store{Pool: pool}).Operational(ctx)
			pool.Close()
			if e != nil || op.MaxArticles != tc.maximum || op.PerSource != 8 || op.Days != 1 || op.DeliveryEnabled || op.ProcessingEnabled != tc.processing || op.Revision < 1 {
				t.Fatal("recovered operational API contract invalid")
			}
		})
	}

}
