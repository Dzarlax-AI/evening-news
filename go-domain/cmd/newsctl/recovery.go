package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/recovery"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/term"
)

func recoveryPassphrase(confirm bool) (string, error) {
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return "", errors.New("interactive terminal required for recovery passphrase")
	}
	defer tty.Close()
	fmt.Fprint(tty, "Recovery passphrase (at least 12 characters): ")
	value, err := term.ReadPassword(int(tty.Fd()))
	fmt.Fprintln(tty)
	if err != nil || len(value) < 12 || len(value) > 1024 {
		return "", errors.New("invalid recovery passphrase")
	}
	if confirm {
		fmt.Fprint(tty, "Repeat passphrase: ")
		again, e := term.ReadPassword(int(tty.Fd()))
		fmt.Fprintln(tty)
		if e != nil || string(value) != string(again) {
			return "", errors.New("passphrases do not match")
		}
	}
	return string(value), nil
}
func backupCommand(l managedsetup.Layout, args []string) error {
	flags := flag.NewFlagSet("backup", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	output := flags.String("output", "", "new encrypted bundle path")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *output == "" {
		return errors.New("usage: newsctl backup --output PATH")
	}
	pass, err := recoveryPassphrase(true)
	if err != nil {
		return err
	}
	return backupInstallation(context.Background(), l, *output, pass, "postgres:5432")
}
func backupInstallation(ctx context.Context, l managedsetup.Layout, output, passphrase, endpoint string) error {
	s, err := managedsetup.ReadState(l.Setup)
	if err != nil {
		return err
	}
	if s.ExternalDB {
		return errors.New("external database backup requires an explicit external target mapping and is not supported")
	}
	if _, err = managedsetup.Prepare(l, s.Site); err != nil {
		return err
	}
	owner, err := connectAt(ctx, s, "postgres", endpoint)
	if err != nil {
		return err
	}
	defer owner.Close()
	var active int
	if owner.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename IN ('news_web_runtime','news_domain_runtime','news_worker_runtime','news_execution_runtime','news_ingestion_runtime','news_processing_runtime','scheduler_runtime')`).Scan(&active) != nil || active != 0 {
		return errors.New("stop web, background and executor before offline recovery backup")
	}
	stage, err := os.MkdirTemp("", "news-backup-")
	if err != nil {
		return errors.New("private staging unavailable")
	}
	defer os.RemoveAll(stage)
	for name, path := range map[string]string{"installation.json": filepath.Join(l.Setup, "installation.json"), "web.json": filepath.Join(l.Web, "runtime.json"), "domain.json": filepath.Join(l.Domain, "runtime.json"), "background.json": filepath.Join(l.Background, "runtime.json"), "extractor.json": filepath.Join(l.Extractor, "runtime.json"), "postgres-password": filepath.Join(l.PostgresSecrets, "postgres-password")} {
		raw, e := os.ReadFile(path)
		if e != nil || len(raw) > 65536 {
			return errors.New("private state unavailable")
		}
		if e = os.WriteFile(filepath.Join(stage, name), raw, 0600); e != nil {
			return errors.New("private staging unavailable")
		}
	}
	for db, file := range map[string]string{"newsdb": "news.sql", "schedulerdb": "scheduler.sql"} {
		if err = dumpDatabase(ctx, s, db, endpoint, filepath.Join(stage, file)); err != nil {
			return err
		}
	}
	if owner.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename IN ('news_web_runtime','news_domain_runtime','news_worker_runtime','news_execution_runtime','news_ingestion_runtime','news_processing_runtime','scheduler_runtime')`).Scan(&active) != nil || active != 0 {
		return errors.New("runtime connected during offline backup; no bundle published")
	}
	if err = recovery.Write(output, s.InstallationID, stage, passphrase); err != nil {
		return err
	}
	fmt.Println("Encrypted recovery bundle created; keep its passphrase separately")
	return nil
}
func dumpDatabase(ctx context.Context, s managedsetup.State, database, endpoint, output string) error {
	cfg, err := pgx.ParseConfig(ownerDSNAt(s, database, endpoint))
	if err != nil {
		return errors.New("database configuration unavailable")
	}
	pool, err := connectAt(ctx, s, database, endpoint)
	if err != nil {
		return err
	}
	defer pool.Close()
	var version int
	if pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer`).Scan(&version) != nil {
		return errors.New("database version unavailable")
	}
	binary := "pg_dump"
	candidate := filepath.Join("/usr/libexec", "postgresql"+strconv.Itoa(version/10000), "pg_dump")
	if info, e := os.Stat(candidate); e == nil && info.Mode().IsRegular() {
		binary = candidate
	}
	out, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("private dump unavailable")
	}
	defer out.Close()
	schema := "news"
	if database == "schedulerdb" {
		schema = "scheduler_service"
	}
	cmd := exec.CommandContext(ctx, binary, "-h", cfg.Host, "-p", strconv.Itoa(int(cfg.Port)), "-U", cfg.User, "--no-owner", "--no-acl", "--schema="+schema, cfg.Database)
	cmd.Env = append(cleanPasswordEnv(), "PGPASSWORD="+cfg.Password)
	cmd.Stdout = out
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	if cmd.Run() != nil {
		return errors.New("database dump failed")
	}
	return out.Close()
}
func cleanPasswordEnv() []string {
	out := []string{}
	for _, value := range os.Environ() {
		if len(value) < 11 || value[:11] != "PGPASSWORD=" {
			out = append(out, value)
		}
	}
	return out
}

func restoreCommand(l managedsetup.Layout, args []string) error {
	if current, err := managedsetup.ReadState(l.Setup); err == nil && current.ExternalDB {
		return errors.New("external database restore requires an explicit external target mapping and is not supported")
	}
	flags := flag.NewFlagSet("restore", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	input := flags.String("input", "", "encrypted bundle path")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *input == "" {
		return errors.New("usage: newsctl restore --input PATH (empty state and database volumes only)")
	}
	pass, err := recoveryPassphrase(false)
	if err != nil {
		return err
	}
	stage, manifest, err := recovery.Read(*input, pass, "")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	return stageRestore(l, stage, manifest)
}
func stageRestore(l managedsetup.Layout, stage string, manifest recovery.Manifest) error {
	s, err := managedsetup.ReadState(stage)
	if err != nil || s.InstallationID != manifest.InstallationID {
		return errors.New("recovery identity mismatch")
	}
	if err = managedsetup.RestoreState(l, stage); err != nil {
		return err
	}
	fmt.Println("Recovery staged. Start PostgreSQL and the one-shot provisioner; runtime and schedule dispatch remain paused until newsctl resume")
	return nil
}

func resumeInstallation(ctx context.Context, l managedsetup.Layout) error {
	s, err := managedsetup.ReadState(l.Setup)
	if err != nil {
		return err
	}
	if s.ExternalDB {
		return errors.New("external database recovery requires an explicit external target mapping and is not supported")
	}
	news, err := connect(ctx, s, "newsdb")
	if err != nil {
		return err
	}
	defer news.Close()
	var id string
	var complete bool
	if news.QueryRow(ctx, `SELECT installation_id,provisioning_complete FROM news.installation_state WHERE id=1`).Scan(&id, &complete) != nil || id != s.InstallationID || !complete {
		return errors.New("installation is not ready")
	}
	scheduler, err := connect(ctx, s, "schedulerdb")
	if err != nil {
		return err
	}
	defer scheduler.Close()
	if _, err = scheduler.Exec(ctx, `UPDATE scheduler_service.control SET paused=false,revision=revision+1,updated_at=now() WHERE id=1`); err != nil {
		return errors.New("scheduler resume failed")
	}
	if _, err = news.Exec(ctx, `UPDATE news.installation_state SET maintenance_mode=false,revision=revision+1,updated_at=now() WHERE id=1 AND installation_id=$1`, s.InstallationID); err != nil {
		return errors.New("worker resume failed")
	}
	fmt.Println("Runtime recovery released; enabled schedules and retained queued work may now run")
	return nil
}

type pendingRecovery struct {
	InstallationID string `json:"installation_id"`
	NewsSHA        string `json:"news_sha256"`
	SchedulerSHA   string `json:"scheduler_sha256"`
}

func readRecovery(l managedsetup.Layout) (*pendingRecovery, error) {
	raw, err := os.ReadFile(filepath.Join(l.Setup, "recovery-pending.json"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New("recovery marker unavailable")
	}
	var p pendingRecovery
	if json.Unmarshal(raw, &p) != nil || p.InstallationID == "" {
		return nil, errors.New("recovery marker invalid")
	}
	for file, expected := range map[string]string{"news.sql": p.NewsSHA, "scheduler.sql": p.SchedulerSHA} {
		f, e := os.Open(filepath.Join(l.Setup, "recovery", file))
		if e != nil {
			return nil, errors.New("recovery dump unavailable")
		}
		h := sha256.New()
		_, e = io.Copy(h, f)
		f.Close()
		if e != nil {
			return nil, errors.New("recovery dump unavailable")
		}
		if hex.EncodeToString(h.Sum(nil)) != expected {
			return nil, errors.New("recovery dump damaged")
		}
	}
	return &p, nil
}

func restoreDatabase(ctx context.Context, s managedsetup.State, database, endpoint, input string) error {
	cfg, err := pgx.ParseConfig(ownerDSNAt(s, database, endpoint))
	if err != nil {
		return errors.New("restore database configuration invalid")
	}
	in, err := os.Open(input)
	if err != nil {
		return errors.New("recovery dump unavailable")
	}
	defer in.Close()
	cmd := exec.CommandContext(ctx, "psql", "-X", "--set=ON_ERROR_STOP=1", "--single-transaction", "-h", cfg.Host, "-p", strconv.Itoa(int(cfg.Port)), "-U", cfg.User, "-d", cfg.Database)
	cmd.Env = append(cleanPasswordEnv(), "PGPASSWORD="+cfg.Password)
	cmd.Stdin = in
	if database == "schedulerdb" {
		digest := sha256.New()
		if _, err = io.Copy(digest, in); err != nil {
			return errors.New("recovery dump unavailable")
		}
		if _, err = in.Seek(0, io.SeekStart); err != nil {
			return errors.New("recovery dump unavailable")
		}
		hash := hex.EncodeToString(digest.Sum(nil))
		// Commit the bundle identity in the same SQL transaction as the dump.
		marker := fmt.Sprintf("\nCREATE TABLE IF NOT EXISTS scheduler_service.news_recovery_identity(id smallint PRIMARY KEY CHECK(id=1),installation_id text NOT NULL,bundle_sha text NOT NULL); INSERT INTO scheduler_service.news_recovery_identity VALUES(1,'%s','%s') ON CONFLICT(id) DO UPDATE SET installation_id=excluded.installation_id,bundle_sha=excluded.bundle_sha;\n", s.InstallationID, hash)
		cmd.Stdin = io.MultiReader(in, strings.NewReader(marker))
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	cmd.WaitDelay = 2 * time.Second
	if cmd.Run() != nil {
		return errors.New("database recovery failed; runtimes must remain stopped")
	}
	return nil
}
func prepareRecoveredNews(ctx context.Context, pool *pgxpool.Pool, s managedsetup.State) error {
	return prepareRecoveredNewsOnce(ctx, pool, s, "")
}
func prepareRecoveredNewsOnce(ctx context.Context, pool *pgxpool.Pool, s managedsetup.State, bundleHash string) error {
	var id string
	if pool.QueryRow(ctx, `SELECT installation_id FROM news.installation_state WHERE id=1`).Scan(&id) != nil || id != s.InstallationID {
		return errors.New("restored database identity mismatch")
	}
	key, err := base64.RawStdEncoding.DecodeString(s.CredentialKey)
	if err != nil {
		return errors.New("recovery key unavailable")
	}
	store := credentialstore.Store{Pool: pool, Key: key, KeyID: s.CredentialKeyID, InstallationID: s.InstallationID}
	for _, provider := range []string{"gemini", "openai", "telegram"} {
		if _, _, err = store.Resolve(ctx, provider); err != nil {
			return errors.New("restored integration credential verification failed")
		}
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.New("recovery finalization unavailable")
	}
	defer tx.Rollback(ctx)
	if bundleHash != "" {
		var previous string
		err = tx.QueryRow(ctx, `SELECT value #>> '{}' FROM news.settings WHERE key='managed_recovery_activation'`).Scan(&previous)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return errors.New("recovery activation state unavailable")
		}
		if previous == bundleHash {
			return nil
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE news.installation_state SET provisioning_complete=false,maintenance_mode=true,revision=revision+1,updated_at=now() WHERE id=1;
 UPDATE news.managed_admin SET revision=revision+1,updated_at=now(); DELETE FROM news.setup_tokens;
 UPDATE news.background_worker_status SET owner_id=NULL,lock_backend_pid=NULL,heartbeat_at=NULL,status='stopped';
 UPDATE news.schedule_settings SET is_running=false`); err != nil {
		return errors.New("recovery finalization failed")
	}
	if bundleHash != "" {
		if _, err = tx.Exec(ctx, `INSERT INTO news.settings(key,value,updated_at) VALUES('managed_recovery_activation',to_jsonb($1::text),now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, bundleHash); err != nil {
			return errors.New("recovery activation state unavailable")
		}
	}
	return tx.Commit(ctx)
}
func pauseRecoveredScheduler(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.New("scheduler recovery unavailable")
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `ALTER SCHEMA scheduler_service OWNER TO scheduler_runtime`); err != nil {
		return errors.New("scheduler schema recovery failed")
	}
	for _, table := range []string{"schedules", "occurrences", "control", "service_state", "news_recovery_identity"} {
		if _, err = tx.Exec(ctx, "ALTER TABLE "+pgx.Identifier{"scheduler_service", table}.Sanitize()+" OWNER TO scheduler_runtime"); err != nil {
			return errors.New("scheduler ownership recovery failed")
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE scheduler_service.control SET paused=true,revision=revision+1,updated_at=now() WHERE id=1`); err != nil {
		return errors.New("scheduler recovery pause failed")
	}
	return tx.Commit(ctx)
}
func finishRecoveryStaging(l managedsetup.Layout) error {
	pending := filepath.Join(l.Setup, "recovery-pending.json")
	completed := filepath.Join(l.Setup, "recovery-completed.json")
	if err := os.Rename(pending, completed); err != nil {
		return errors.New("recovery completion marker failed")
	}
	if err := os.RemoveAll(filepath.Join(l.Setup, "recovery")); err != nil {
		return errors.New("recovery staging cleanup failed")
	}
	if err := os.Remove(completed); err != nil {
		return errors.New("recovery marker cleanup failed")
	}
	return nil
}
