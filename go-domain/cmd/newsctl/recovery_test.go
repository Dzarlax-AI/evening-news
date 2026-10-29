package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/crypto/argon2"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/recovery"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/jackc/pgx/v5"
)

func TestNativeBrowserRecoveryFailsClosed(t *testing.T) {
	native := managedsetup.NativeBrowserOptions{
		ExtractorValues: map[string]string{
			"LISTEN_ADDR": ":8080", "EXTRACTOR_HTTP_ONLY": "false", "EXTRACTOR_BROWSER_BACKEND": "native",
			"EXTRACTOR_NATIVE_NETWORK_ISOLATED": "true", "CHROME_ENDPOINT": "http://10.77.0.2:9222",
			"EXTRACTOR_NATIVE_PROXY_URL": "http://10.77.0.5:8081", "EXTRACTOR_NATIVE_PROXY_TOKEN": "fixture-proxy-token-01",
			"EXTRACTOR_MAX_CONCURRENT": "2", "EXTRACTOR_BROWSER_CONCURRENT": "2",
		},
		EgressValues:    map[string]string{"EGRESS_LISTEN_ADDR": "10.77.0.5:8081", "EGRESS_TOKEN": "fixture-proxy-token-01", "EGRESS_MAX_CONCURRENT": "4"},
		ExtractorLimits: managedsetup.ContainerLimits{Init: true, ReadOnly: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true"}, PidsLimit: 128, MemLimit: "256m", CPUs: 1, StopGracePeriod: "55s"},
		EgressLimits:    managedsetup.ContainerLimits{Init: true, ReadOnly: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true"}, PidsLimit: 64, MemLimit: "128m", CPUs: 0.25, StopGracePeriod: "10s"},
		ControlNetwork:  "control", ControlName: "fixture-control", OutboundNetwork: "outbound", OutboundName: "fixture-outbound",
		ExtractorIP: "10.77.0.4", EgressIP: "10.77.0.5",
	}
	state, err := managedsetup.NewState("http://localhost:8000", managedsetup.Options{NativeBrowser: &native})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	setup := filepath.Join(root, "setup")
	if err = os.Mkdir(setup, 0700); err != nil {
		t.Fatal(err)
	}
	writeState := func(dir string) {
		t.Helper()
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(state)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "installation.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	layout := managedsetup.Layout{Setup: setup}
	writeState(setup)
	if err = backupInstallation(context.Background(), layout, filepath.Join(root, "backup.age"), "synthetic-passphrase", "127.0.0.1:5432"); err == nil || !strings.Contains(err.Error(), "native browser recovery bundle is not supported") {
		t.Fatalf("native backup was not refused: %v", err)
	}
	if err = resumeInstallation(context.Background(), layout); err == nil || !strings.Contains(err.Error(), "native browser recovery resume is not supported") {
		t.Fatalf("native resume was not refused: %v", err)
	}
	stage := filepath.Join(root, "stage")
	writeState(stage)
	if err = stageRestore(managedsetup.Layout{Setup: filepath.Join(root, "target")}, stage, recovery.Manifest{InstallationID: state.InstallationID}); err == nil || !strings.Contains(err.Error(), "native browser recovery bundle is not supported") {
		t.Fatalf("native restore was not refused: %v", err)
	}
}

func testRecoveryRoundTrip(t *testing.T, ctx context.Context, l managedsetup.Layout, s managedsetup.State, endpoint string, admin *pgx.Conn) {
	t.Helper()
	news, err := connectAt(ctx, s, "newsdb", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	key, err := base64.RawStdEncoding.DecodeString(s.CredentialKey)
	if err != nil {
		t.Fatal(err)
	}
	credentials := credentialstore.Store{Pool: news, Key: key, KeyID: s.CredentialKeyID, InstallationID: s.InstallationID}
	if _, err = credentials.Put(ctx, "gemini", 0, []byte("synthetic-restorable-key")); err != nil {
		t.Fatal(err)
	}
	// Keep a saved PHC string and a queued receipt to check persistence. No real
	// administrator data, model calls, or message delivery are involved.
	salt := make([]byte, 16)
	derived := argon2.IDKey([]byte("synthetic-administrator-password"), salt, 2, 19456, 1, 32)
	hash := fmt.Sprintf("$argon2id$v=19$m=19456,t=2,p=1$%s$%s", base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(derived))
	if _, err = news.Exec(ctx, `INSERT INTO managed_admin(id,username,password_hash) VALUES(1,'fixture-admin',$1)`, hash); err != nil {
		t.Fatal(err)
	}
	if _, err = news.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state) VALUES(gen_random_uuid(),'backup','{}','restore-fixture','queued')`); err != nil {
		t.Fatal(err)
	}
	news.Close()
	sched, err := connectAt(ctx, s, "schedulerdb", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_, here, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(here), "../../../go-scheduler/migrations/001_initial.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ReplaceAll(string(raw), "{{SCHEMA}}", `"scheduler_service"`)
	sql = strings.ReplaceAll(sql, "{{START_PAUSED}}", "FALSE")
	if _, err = sched.Exec(ctx, sql); err != nil {
		t.Fatal(err)
	}
	sched.Close()
	output := filepath.Join(t.TempDir(), "recovery.age")
	password := "synthetic-backup-passphrase"
	if err = backupInstallation(ctx, l, output, password, endpoint); err != nil {
		t.Fatal(err)
	}
	stage, manifest, err := recovery.Read(output, password, "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(stage)
	// Only this dedicated test server is eligible: the parent test verified its
	// loopback identity and absence of application databases before provisioning.
	for _, db := range []string{"newsdb", "schedulerdb"} {
		if _, err = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{db}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Fatal(err)
		}
	}
	roles := []string{"news_ingestion_fence_owner", "news_processing_fence_owner"}
	for _, role := range roleNames {
		roles = append(roles, role)
	}
	for _, role := range roles {
		if _, err = admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{role}.Sanitize()); err != nil {
			t.Fatal(err)
		}
	}
	root := t.TempDir()
	target := managedsetup.Layout{Setup: filepath.Join(root, "setup"), Web: filepath.Join(root, "web"), Domain: filepath.Join(root, "domain"), Background: filepath.Join(root, "background"), Extractor: filepath.Join(root, "extractor"), Postgres: filepath.Join(root, "pg"), PostgresSecrets: filepath.Join(root, "pg-secret"), WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1}
	if err = stageRestore(target, stage, manifest); err != nil {
		t.Fatal(err)
	}
	interruptedRestore := func(ctx context.Context, state managedsetup.State, database, host, input string) error {
		if database == "schedulerdb" {
			return errors.New("synthetic scheduler restore interruption")
		}
		return restoreDatabase(ctx, state, database, host, input)
	}
	if err = provisionAtWithRestorer(ctx, target, endpoint, interruptedRestore); err == nil {
		t.Fatal("fault injection did not interrupt recovery")
	}
	partial, err := connectAt(ctx, s, "newsdb", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	var safelyPaused bool
	if partial.QueryRow(ctx, `SELECT maintenance_mode AND NOT provisioning_complete FROM news.installation_state WHERE id=1`).Scan(&safelyPaused) != nil || !safelyPaused {
		partial.Close()
		t.Fatal("partial recovery did not remain paused")
	}
	partial.Close()
	for _, change := range [][2]string{{"GRANT news_owner TO news_web_runtime", "REVOKE news_owner FROM news_web_runtime"}, {"ALTER ROLE news_ingestion_fence_owner LOGIN", "ALTER ROLE news_ingestion_fence_owner NOLOGIN"}} {
		if _, err = admin.Exec(ctx, change[0]); err != nil {
			t.Fatal("fixture privilege mutation failed")
		}
		blocked := provisionAt(ctx, target, endpoint)
		if _, err = admin.Exec(ctx, change[1]); err != nil {
			t.Fatal("fixture privilege cleanup failed")
		}
		if blocked == nil {
			t.Fatal("recovery admitted altered runtime privileges")
		}
	}
	unrelated, err := connectAt(ctx, s, "schedulerdb", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = unrelated.Exec(ctx, `CREATE SCHEMA scheduler_service;CREATE TABLE scheduler_service.schedules(id int);CREATE TABLE scheduler_service.occurrences(id int);CREATE TABLE scheduler_service.service_state(id int);CREATE TABLE scheduler_service.control(id int,paused boolean);INSERT INTO scheduler_service.control VALUES(1,false)`); err != nil {
		t.Fatal(err)
	}
	if err = provisionAt(ctx, target, endpoint); err == nil {
		t.Fatal("unrelated scheduler schema accepted")
	}
	var unchanged bool
	if unrelated.QueryRow(ctx, `SELECT NOT paused FROM scheduler_service.control WHERE id=1`).Scan(&unchanged) != nil || !unchanged {
		t.Fatal("unrelated scheduler data mutated")
	}
	if _, err = unrelated.Exec(ctx, `DROP SCHEMA scheduler_service CASCADE`); err != nil {
		t.Fatal(err)
	}
	unrelated.Close()
	afterSchedulerCommit := func(ctx context.Context, state managedsetup.State, database, host, input string) error {
		if e := restoreDatabase(ctx, state, database, host, input); e != nil {
			return e
		}
		if database == "schedulerdb" {
			return errors.New("synthetic interruption after scheduler SQL commit")
		}
		return nil
	}
	if err = provisionAtWithRestorer(ctx, target, endpoint, afterSchedulerCommit); err == nil {
		t.Fatal("second fault injection did not interrupt")
	}
	if err = provisionAt(ctx, target, endpoint); err != nil {
		t.Fatal(err)
	}
	restored, err := connectAt(ctx, s, "newsdb", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var paused bool
	var revision int64
	var restoredHash, state string
	if err = restored.QueryRow(ctx, `SELECT maintenance_mode FROM installation_state WHERE id=1`).Scan(&paused); err != nil || !paused {
		t.Fatal("restored worker was not paused", err)
	}
	if err = restored.QueryRow(ctx, `SELECT revision,password_hash FROM managed_admin WHERE id=1`).Scan(&revision, &restoredHash); err != nil || revision != 2 || restoredHash != hash {
		t.Fatal("administrator persistence or session revocation failed", err)
	}
	if err = restored.QueryRow(ctx, `SELECT state FROM background_jobs WHERE idempotency_key='restore-fixture'`).Scan(&state); err != nil || state != "queued" {
		t.Fatal("queued receipt changed", err)
	}
	credentials.Pool = restored
	secret, _, err := credentials.Resolve(ctx, "gemini")
	if err != nil || string(secret) != "synthetic-restorable-key" {
		t.Fatal("credential recovery failed")
	}
	restoredScheduler, err := connectAt(ctx, s, "schedulerdb", endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer restoredScheduler.Close()
	if err = restoredScheduler.QueryRow(ctx, `SELECT paused FROM scheduler_service.control WHERE id=1`).Scan(&paused); err != nil || !paused {
		t.Fatal("restored schedule dispatch was not paused", err)
	}
	if _, err = os.Stat(filepath.Join(target.Setup, "recovery-pending.json")); !os.IsNotExist(err) {
		t.Fatal("recovery staging retained")
	}
	if err = stageRestore(target, stage, manifest); err == nil {
		t.Fatal("restore overwrote an initialized installation")
	}
}
