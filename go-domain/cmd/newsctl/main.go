// newsctl operates on the installer's private local state. It must not run as a
// long-lived service or be mounted into runtime containers.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "newsctl:", err)
		os.Exit(1)
	}
}
func layout() managedsetup.Layout {
	return managedsetup.Layout{Setup: "/state/setup", Web: "/state/web", Domain: "/state/domain", Background: "/state/background", Extractor: "/state/extractor", Egress: "/state/egress", Postgres: "/state/pg", PostgresSecrets: "/state/postgres-secret", WebUID: 65532, DomainUID: 10001, BackgroundUID: 65532, ExtractorUID: 65532, EgressUID: 65532}
}
func run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: newsctl prepare|provision|setup-token|backup|restore|resume|adopt")
	}
	l := layout()
	switch args[0] {
	case "prepare":
		return prepareCommand(context.Background(), l, args[1:])
	case "provision":
		if len(args) != 1 {
			return errors.New("invalid provision arguments")
		}
		return provision(context.Background(), l)
	case "setup-token":
		if len(args) != 1 {
			return errors.New("invalid setup-token arguments")
		}
		return issueToken(context.Background(), l)
	case "backup":
		return backupCommand(l, args[1:])
	case "restore":
		return restoreCommand(l, args[1:])
	case "resume":
		if len(args) != 1 {
			return errors.New("invalid resume arguments")
		}
		return resumeInstallation(context.Background(), l)
	case "reset-admin":
		if len(args) != 1 {
			return errors.New("invalid reset-admin arguments")
		}
		return resetAdmin(context.Background(), l)
	case "adopt":
		return adoptionCommand(context.Background(), l, args[1:])
	default:
		return errors.New("unknown command")
	}
}
func ownerDSN(s managedsetup.State, database string) string {
	return ownerDSNAt(s, database, "postgres:5432")
}
func ownerDSNAt(s managedsetup.State, database, endpoint string) string {
	u := url.URL{Scheme: "postgres", User: url.UserPassword("news_owner", s.Passwords["owner"]), Host: endpoint, Path: database}
	q := u.Query()
	q.Set("sslmode", "disable")
	if database == "newsdb" {
		q.Set("search_path", "news")
	}
	u.RawQuery = q.Encode()
	return u.String()
}
func connect(ctx context.Context, s managedsetup.State, db string) (*pgxpool.Pool, error) {
	return connectAt(ctx, s, db, "postgres:5432")
}
func connectAt(ctx context.Context, s managedsetup.State, db, endpoint string) (*pgxpool.Pool, error) {
	p, err := pgxpool.New(ctx, ownerDSNAt(s, db, endpoint))
	if err != nil {
		return nil, errors.New("database unavailable")
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, errors.New("database unavailable")
	}
	return p, nil
}

var roleNames = map[string]string{"web": "news_web_runtime", "domain": "news_domain_runtime", "worker": "news_worker_runtime", "execution": "news_execution_runtime", "ingestion": "news_ingestion_runtime", "processing": "news_processing_runtime", "scheduler": "scheduler_runtime"}

func provision(ctx context.Context, l managedsetup.Layout) error {
	return provisionAt(ctx, l, "postgres:5432")
}
func provisionAt(ctx context.Context, l managedsetup.Layout, endpoint string) error {
	return provisionAtWithRestorer(ctx, l, endpoint, restoreDatabase)
}
func provisionAtWithRestorer(ctx context.Context, l managedsetup.Layout, endpoint string, restore recoveryRestorer) error {
	s, err := managedsetup.ReadState(l.Setup)
	if err != nil {
		return err
	}
	if s.ExternalDB {
		return errors.New("external database installations are pre-provisioned; role or schema provisioning is not supported")
	}
	// Validate every role file before touching the database; no automatic repair.
	if _, err = managedsetup.Prepare(l, s.Site); err != nil {
		return err
	}
	pending, err := readRecovery(l)
	if err != nil {
		return err
	}
	if pending != nil && pending.InstallationID != s.InstallationID {
		return errors.New("recovery identity mismatch")
	}
	admin, err := connectAt(ctx, s, "postgres", endpoint)
	if err != nil {
		return err
	}
	defer admin.Close()
	var exists bool
	if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='newsdb')`).Scan(&exists); err != nil {
		return errors.New("database inspection failed")
	}
	if exists {
		if pending != nil {
			return resumeRecoveryProvisionWithRestorer(ctx, l, s, endpoint, pending, restore)
		}
		if err = verifyProvisionedAt(ctx, s, endpoint); err != nil {
			return err
		}
		if pending != nil {
			return finishRecoveryStaging(l)
		}
		return nil
	}
	for key, name := range roleNames {
		var found bool
		if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, name).Scan(&found); err != nil || found {
			return errors.New("unexpected existing role; recovery required")
		}
		// Generated passwords use only URL-safe ASCII and contain no SQL quotes.
		if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '"+s.Passwords[key]+"'"); err != nil {
			return errors.New("role provisioning failed")
		}
	}
	for _, name := range []string{"news_ingestion_fence_owner", "news_processing_fence_owner"} {
		var found bool
		if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, name).Scan(&found); err != nil || found {
			return errors.New("unexpected existing role; recovery required")
		}
		if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{name}.Sanitize()+" NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"); err != nil {
			return errors.New("role provisioning failed")
		}
	}
	for _, db := range []string{"newsdb", "schedulerdb"} {
		if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{db}.Sanitize()); err != nil {
			return errors.New("database provisioning failed")
		}
	}
	if _, err = admin.Exec(ctx, `REVOKE ALL ON DATABASE newsdb FROM PUBLIC; REVOKE ALL ON DATABASE schedulerdb FROM PUBLIC; GRANT CONNECT ON DATABASE newsdb TO news_web_runtime,news_domain_runtime,news_worker_runtime,news_execution_runtime,news_ingestion_runtime,news_processing_runtime; GRANT CONNECT ON DATABASE schedulerdb TO scheduler_runtime`); err != nil {
		return errors.New("database access provisioning failed")
	}
	if pending != nil {
		return resumeRecoveryProvisionWithRestorer(ctx, l, s, endpoint, pending, restore)
	}
	news, err := connectAt(ctx, s, "newsdb", endpoint)
	if err != nil {
		return err
	}
	defer news.Close()
	if pending != nil {
		if err = restoreDatabase(ctx, s, "newsdb", endpoint, filepath.Join(l.Setup, "recovery", "news.sql")); err != nil {
			return err
		}
	} else if _, err = news.Exec(ctx, `CREATE SCHEMA news`); err != nil {
		return errors.New("schema provisioning failed")
	}
	result, err := migration.Run(ctx, news, true, false)
	if err != nil || len(result.Errors) > 0 {
		return errors.New("schema migration failed")
	}
	if pending == nil {
		if _, err = news.Exec(ctx, `INSERT INTO news.installation_state(id,installation_id) VALUES(1,$1)`, s.InstallationID); err != nil {
			return errors.New("installation identity failed")
		}
	} else if err = prepareRecoveredNews(ctx, news, s); err != nil {
		return err
	}
	// This script is baked into the installer image and uses fixed identifiers.
	if _, err = news.Exec(ctx, managedsetup.GrantPolicy); err != nil {
		return errors.New("privilege provisioning failed")
	}
	sched, err := connectAt(ctx, s, "schedulerdb", endpoint)
	if err != nil {
		return err
	}
	defer sched.Close()
	if pending != nil {
		if err = restoreDatabase(ctx, s, "schedulerdb", endpoint, filepath.Join(l.Setup, "recovery", "scheduler.sql")); err != nil {
			return err
		}
		if err = pauseRecoveredScheduler(ctx, sched); err != nil {
			return err
		}
	} else if _, err = sched.Exec(ctx, `CREATE SCHEMA scheduler_service AUTHORIZATION scheduler_runtime`); err != nil {
		return errors.New("scheduler provisioning failed")
	}
	if _, err = news.Exec(ctx, `UPDATE news.installation_state SET provisioning_complete=true WHERE id=1 AND installation_id=$1`, s.InstallationID); err != nil {
		return errors.New("provisioning finalization failed")
	}
	if pending != nil {
		if err = finishRecoveryStaging(l); err != nil {
			return err
		}
	}
	fmt.Println("Database provisioning completed")
	return nil
}
func verifyProvisionedAt(ctx context.Context, s managedsetup.State, endpoint string) error {
	news, err := connectAt(ctx, s, "newsdb", endpoint)
	if err != nil {
		return err
	}
	defer news.Close()
	var id string
	var complete bool
	if err = news.QueryRow(ctx, `SELECT installation_id,provisioning_complete FROM news.installation_state WHERE id=1`).Scan(&id, &complete); err != nil || id != s.InstallationID || !complete {
		return errors.New("existing database does not match installation; recovery required")
	}
	scheduler, err := connectAt(ctx, s, "schedulerdb", endpoint)
	if err != nil {
		return err
	}
	defer scheduler.Close()
	var schemaOK bool
	if err = scheduler.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname='scheduler_service' AND r.rolname='scheduler_runtime')`).Scan(&schemaOK); err != nil || !schemaOK {
		return errors.New("scheduler provisioning incomplete")
	}
	fmt.Println("Database provisioning already completed")
	return nil
}
func issueToken(ctx context.Context, l managedsetup.Layout) error {
	s, err := managedsetup.ReadState(l.Setup)
	if err != nil {
		return err
	}
	news, err := connectInstallerState(ctx, l, s)
	if err != nil {
		return err
	}
	defer news.Close()
	tx, err := news.Begin(ctx)
	if err != nil {
		return errors.New("setup unavailable")
	}
	defer tx.Rollback(ctx)
	var id string
	if err = tx.QueryRow(ctx, `SELECT installation_id FROM news.installation_state WHERE id=1 FOR UPDATE`).Scan(&id); err != nil || id != s.InstallationID {
		return errors.New("installation identity mismatch")
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM news.managed_admin WHERE id=1)`).Scan(&exists); err != nil || exists {
		return errors.New("setup unavailable")
	}
	bytes := make([]byte, 32)
	if _, err = rand.Read(bytes); err != nil {
		return errors.New("token generation failed")
	}
	token := base64.RawURLEncoding.EncodeToString(bytes)
	hash := sha256.Sum256([]byte(token))
	expires := time.Now().Add(15 * time.Minute)
	if _, err = tx.Exec(ctx, `INSERT INTO news.setup_tokens(id,token_hash,expires_at,consumed_at) VALUES(1,$1,$2,NULL) ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,expires_at=excluded.expires_at,consumed_at=NULL`, hash[:], expires); err != nil {
		return errors.New("setup unavailable")
	}
	if err = tx.Commit(ctx); err != nil {
		return errors.New("setup unavailable")
	}
	fmt.Println(strings.TrimSpace(token))
	return nil
}
