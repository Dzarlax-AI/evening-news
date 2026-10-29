package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const externalInputLimit = 16384

type externalInput struct {
	Version      int               `json:"version"`
	BootstrapDSN string            `json:"bootstrap_dsn"`
	RuntimeDSNs  map[string]string `json:"runtime_dsns"`
}

var externalRoleEnv = map[string]string{
	"web": "DATABASE_URL", "domain": "DATABASE_URL", "worker": "WORKER_DATABASE_URL",
	"execution": "EXECUTION_DATABASE_URL", "ingestion": "INGESTION_DATABASE_URL",
	"processing": "PROCESSING_DATABASE_URL", "scheduler": "SCHEDULER_DATABASE_URL",
}

// normalizePostgresDSN is used only for pgx validation and connection setup.
// The exact legacy string remains in the per-role manifest for compatible
// runtime readers that perform their own normalization.
func normalizePostgresDSN(value string) string {
	return strings.Replace(value, "postgresql+asyncpg://", "postgresql://", 1)
}

func prepareCommand(ctx context.Context, l managedsetup.Layout, args []string) error {
	flags := flagSet("prepare")
	externalPath := flags.String("external-db", "", "protected JSON with a bootstrap DSN and seven pre-provisioned runtime DSNs")
	optionsPath := flags.String("options", "", "protected JSON installer options")
	browserPath := flags.String("browser-config", "", "protected rendered Compose JSON with the existing native browser topology")
	composeOutput := flags.String("compose-output", "", "private Compose override output path for native browser networking")
	if flags.Parse(args) != nil || flags.NArg() != 0 {
		return errors.New("usage: newsctl prepare [--options PATH] [--external-db PATH]")
	}
	options := managedsetup.Options{}
	var err error
	if *optionsPath != "" {
		options, err = managedsetup.ReadOptions(*optionsPath)
		if err != nil {
			return err
		}
	}
	if *browserPath != "" {
		if options.NativeBrowser != nil {
			return errors.New("choose either --options native_browser or --browser-config")
		}
		browser, readErr := managedsetup.ReadNativeBrowserOptions(*browserPath)
		if readErr != nil {
			return readErr
		}
		options.NativeBrowser = &browser
	}
	site := os.Getenv("SITE_BASE_URL")
	if site == "" {
		site = "http://localhost:8000"
	}
	if *optionsPath == "" && *browserPath == "" {
		if existing, readErr := managedsetup.ReadState(l.Setup); readErr == nil {
			options = existing.Options
		}
	}
	if options.NativeBrowser != nil && *composeOutput == "" {
		return errors.New("--compose-output is required for the native browser profile")
	}
	if options.NativeBrowser == nil && *composeOutput != "" {
		return errors.New("--compose-output requires a native browser profile")
	}
	if err = managedsetup.ValidateWebTransport(site, options, managedsetup.IsLoopbackHTTPOrigin(site)); err != nil {
		return err
	}
	if *externalPath == "" {
		created, e := managedsetup.PrepareWithOptions(l, site, options)
		if e != nil {
			return e
		}
		if options.NativeBrowser != nil {
			if err = writeNativeBrowserOverride(*composeOutput, *options.NativeBrowser); err != nil {
				return err
			}
		}
		if created {
			fmt.Println("Installation state created")
		} else {
			fmt.Println("Installation state verified")
		}
		return nil
	}
	input, _, err := readExternalInput(*externalPath)
	if err != nil {
		return err
	}
	if existing, readErr := managedsetup.ReadState(l.Setup); readErr == nil {
		if !existing.ExternalDB || !sameExternalRuntimeValues(existing, input.RuntimeDSNs) || !externalBootstrapMatches(l, input.BootstrapDSN) || !reflect.DeepEqual(existing.Options, options) {
			return errors.New("external installer state conflicts with provided input")
		}
		if err = validateExternalBundle(ctx, input); err != nil {
			return err
		}
		bootstrap, connectErr := poolForDSN(ctx, input.BootstrapDSN)
		if connectErr != nil {
			return errors.New("external bootstrap connection unavailable")
		}
		defer bootstrap.Close()
		var dbID string
		var complete bool
		if bootstrap.QueryRow(ctx, `SELECT installation_id,provisioning_complete FROM news.installation_state WHERE id=1`).Scan(&dbID, &complete) != nil || dbID != existing.InstallationID || !complete {
			return errors.New("external database installation identity mismatch")
		}
		_, verifyErr := managedsetup.PrepareWithOptions(l, site, options)
		if verifyErr != nil {
			return verifyErr
		}
		fmt.Println("External installation state verified")
		return nil
	} else if _, statErr := os.Lstat(filepath.Join(l.Setup, "installation.json")); statErr == nil {
		return errors.New("existing installer state is invalid; inspect before retrying")
	} else if !os.IsNotExist(statErr) {
		return errors.New("installer state unavailable")
	}
	if err = installExternal(ctx, l, site, options, input); err != nil {
		return err
	}
	if options.NativeBrowser != nil {
		if err = writeNativeBrowserOverride(*composeOutput, *options.NativeBrowser); err != nil {
			return err
		}
	}
	fmt.Println("External database installation initialized; no database, role, grant, or scheduler schema was created by the installer")
	return nil
}

func writeNativeBrowserOverride(path string, options managedsetup.NativeBrowserOptions) error {
	contents, err := managedsetup.NativeBrowserComposeOverride(options)
	if err != nil {
		return errors.New("native browser Compose override invalid")
	}
	if path == "" || err != nil {
		return errors.New("native browser Compose output unavailable")
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return errors.New("native browser Compose output unavailable")
	}
	if existing, statErr := os.Lstat(path); statErr == nil {
		if !existing.Mode().IsRegular() || existing.Mode()&os.ModeSymlink != 0 || existing.Mode().Perm()&0077 != 0 {
			return errors.New("native browser Compose output conflicts")
		}
		old, readErr := os.ReadFile(path)
		if readErr != nil || string(old) != string(contents) {
			return errors.New("native browser Compose output conflicts")
		}
		return nil
	} else if !os.IsNotExist(statErr) {
		return errors.New("native browser Compose output unavailable")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("native browser Compose output unavailable")
	}
	if _, err = f.Write(contents); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
		return errors.New("native browser Compose output unavailable")
	}
	return nil
}

func installExternal(ctx context.Context, l managedsetup.Layout, site string, options managedsetup.Options, input externalInput) error {
	if err := externalDirectoriesClean(l); err != nil {
		return err
	}
	s, manifests, err := managedsetup.NewExternalState(site, options, input.RuntimeDSNs)
	if err != nil {
		return err
	}
	if err = validateExternalBundle(ctx, input); err != nil {
		return err
	}
	bootstrap, err := poolForDSN(ctx, input.BootstrapDSN)
	if err != nil {
		return errors.New("external bootstrap connection unavailable")
	}
	defer bootstrap.Close()
	if err = requirePreparedExternalNews(ctx, bootstrap); err != nil {
		return err
	}
	if err = requireEmptySchedulerSchema(ctx, input.RuntimeDSNs["scheduler"]); err != nil {
		return err
	}
	if err = preflightExternalInstallerPrivileges(ctx, bootstrap); err != nil {
		return err
	}
	result, err := migration.Run(ctx, bootstrap, false, true)
	if err != nil || len(result.Errors) != 0 || len(result.MigrationsRun) != 0 {
		return errors.New("external News schema is not fully migrated; apply schema migrations and grants before retrying")
	}
	if err = ensureExternalIdentity(ctx, bootstrap, s.InstallationID); err != nil {
		return err
	}
	if err = managedsetup.InstallExternalState(l, s, manifests, input.BootstrapDSN); err != nil {
		return errors.New("database initialized but local activation did not complete; preserve the state and inspect before retrying")
	}
	return nil
}

func sameExternalRuntimeValues(s managedsetup.State, dsns map[string]string) bool {
	for role, key := range externalRoleEnv {
		service := role
		if role == "worker" || role == "execution" || role == "ingestion" || role == "processing" || role == "scheduler" {
			service = "background"
		}
		if s.RuntimeValues[service][key] != dsns[role] {
			return false
		}
	}
	return true
}

func externalBootstrapMatches(l managedsetup.Layout, want string) bool {
	path := filepath.Join(l.Setup, "external-bootstrap.json")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return false
	}
	b, err := os.ReadFile(path)
	return err == nil && string(b) == want
}

func readExternalInput(path string) (externalInput, string, error) {
	var input externalInput
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || info.Size() > externalInputLimit {
		return input, "", errors.New("external database input must be a private regular file no larger than 16 KiB")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return input, "", errors.New("external database input unavailable")
	}
	if runtimeconfig.DecodeStrict(raw, &input) != nil || input.Version != 1 || strings.TrimSpace(input.BootstrapDSN) == "" || len(input.RuntimeDSNs) != len(externalRoleEnv) {
		return externalInput{}, "", errors.New("invalid external database input")
	}
	for role := range externalRoleEnv {
		if strings.TrimSpace(input.RuntimeDSNs[role]) == "" {
			return externalInput{}, "", errors.New("external runtime role bundle is incomplete")
		}
	}
	h := sha256.Sum256(raw)
	return input, hex.EncodeToString(h[:]), nil
}

func externalDirectoriesClean(l managedsetup.Layout) error {
	directories := []struct {
		path string
		uid  int
	}{{l.Setup, -1}, {l.Web, l.WebUID}, {l.Domain, l.DomainUID}, {l.Background, l.BackgroundUID}, {l.Extractor, l.ExtractorUID}}
	// The egress runtime volume exists only for the native browser profile.
	// HTTP-only installs must not require an unused path, while layouts that
	// provide the optional volume still receive the same private-directory
	// checks as the other runtime roles.
	if l.Egress != "" {
		directories = append(directories, struct {
			path string
			uid  int
		}{l.Egress, l.EgressUID})
	}
	for _, d := range directories {
		if err := os.MkdirAll(d.path, 0700); err != nil {
			return errors.New("external installer state directories unavailable")
		}
		info, err := os.Lstat(d.path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("external installer state directories unavailable")
		}
		if err = os.Chmod(d.path, 0700); err != nil {
			return errors.New("external installer state directories must be private")
		}
		if d.uid >= 0 {
			if err = os.Chown(d.path, d.uid, d.uid); err != nil {
				return errors.New("external installer state directories must be private")
			}
		}
	}
	paths := []string{filepath.Join(l.Setup, "installation.json"), filepath.Join(l.Setup, "external-bootstrap.json"), filepath.Join(l.Web, "runtime.json"), filepath.Join(l.Domain, "runtime.json"), filepath.Join(l.Background, "runtime.json"), filepath.Join(l.Extractor, "runtime.json")}
	if l.Egress != "" {
		paths = append(paths, filepath.Join(l.Egress, "runtime.json"))
	}
	for _, p := range paths {
		if _, err := os.Lstat(p); err == nil {
			return errors.New("partial installer state exists; inspect before retrying")
		} else if !os.IsNotExist(err) {
			return errors.New("installer state unavailable")
		}
	}
	dirs := []string{l.Setup, l.Web, l.Domain, l.Background, l.Extractor}
	if l.Egress != "" {
		dirs = append(dirs, l.Egress)
	}
	for _, d := range dirs {
		entries, err := os.ReadDir(d)
		if err != nil {
			return errors.New("external installer state unavailable")
		}
		if len(entries) != 0 {
			return errors.New("unexpected files in external installer state directories; inspect before continuing")
		}
	}
	return nil
}

func validateExternalBundle(ctx context.Context, input externalInput) error {
	if err := validateExternalBundleTargets(input); err != nil {
		return err
	}
	for role := range externalRoleEnv {
		if err := checkExternalRole(ctx, role, input.RuntimeDSNs[role]); err != nil {
			return fmt.Errorf("%s runtime role preflight failed: %w", role, err)
		}
	}
	if err := schedulerCannotAccessNews(ctx, input); err != nil {
		return err
	}
	bootstrap, err := poolForDSN(ctx, input.BootstrapDSN)
	if err != nil {
		return errors.New("bootstrap privilege preflight failed")
	}
	defer bootstrap.Close()
	var canConnect, canCreate bool
	if err = bootstrap.QueryRow(ctx, `SELECT has_database_privilege(current_user,current_database(),'CONNECT'),has_schema_privilege(current_user,'news','USAGE')`).Scan(&canConnect, &canCreate); err != nil || !canConnect || !canCreate {
		return errors.New("bootstrap role requires CONNECT and USAGE on the pre-provisioned news schema")
	}
	if err = preflightExternalRuntimePrivileges(ctx, input); err != nil {
		return err
	}
	return nil
}

func validateExternalBundleTargets(input externalInput) error {
	base, err := pgx.ParseConfig(normalizePostgresDSN(input.RuntimeDSNs["domain"]))
	if err != nil || base.Database == "" || base.RuntimeParams["search_path"] != "news" {
		return errors.New("external News database DSNs must target the news schema")
	}
	bootstrap, err := pgx.ParseConfig(normalizePostgresDSN(input.BootstrapDSN))
	if err != nil || bootstrap.Host != base.Host || bootstrap.Port != base.Port || bootstrap.Database != base.Database || bootstrap.RuntimeParams["search_path"] != "news" || bootstrap.User == "" {
		return errors.New("bootstrap DSN must target the same News database and news schema")
	}
	users := map[string]bool{}
	for role := range externalRoleEnv {
		cfg, e := pgx.ParseConfig(normalizePostgresDSN(input.RuntimeDSNs[role]))
		if e != nil || cfg.User == "" {
			return fmt.Errorf("%s DSN is invalid", role)
		}
		if role != "scheduler" && (cfg.Host != base.Host || cfg.Port != base.Port || cfg.Database != base.Database || cfg.RuntimeParams["search_path"] != "news") {
			return errors.New("all News runtime roles must target the same database and news schema")
		}
		if role == "scheduler" && cfg.RuntimeParams["search_path"] != "scheduler_service" {
			return errors.New("scheduler DSN must target the scheduler_service schema")
		}
		if role == "scheduler" && (cfg.Host != base.Host || cfg.Port != base.Port || cfg.Database == base.Database) {
			return errors.New("scheduler DSN must use a separate database on the same PostgreSQL endpoint")
		}
		if cfg.User == bootstrap.User {
			return errors.New("bootstrap identity must be separate from runtime roles")
		}
		if users[cfg.User] {
			return errors.New("each runtime DSN must use a distinct least-privilege role")
		}
		users[cfg.User] = true
	}
	return nil
}

func schedulerCannotAccessNews(ctx context.Context, input externalInput) error {
	news, err := pgx.ParseConfig(normalizePostgresDSN(input.RuntimeDSNs["domain"]))
	if err != nil {
		return errors.New("News database DSN is invalid")
	}
	u, err := url.Parse(normalizePostgresDSN(input.RuntimeDSNs["scheduler"]))
	if err != nil {
		return errors.New("scheduler database DSN is invalid")
	}
	u.Path = "/" + news.Database
	query := u.Query()
	query.Set("search_path", "news")
	u.RawQuery = query.Encode()
	p, err := poolForDSN(ctx, u.String())
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "42501" || pgErr.Code == "3D000") {
			return nil
		}
		return errors.New("scheduler News database isolation preflight unavailable")
	}
	defer p.Close()
	var schemaAccess bool
	if err = p.QueryRow(ctx, `SELECT has_schema_privilege(current_user,'news','USAGE')`).Scan(&schemaAccess); err != nil {
		return errors.New("scheduler News database isolation preflight unavailable")
	}
	if schemaAccess {
		return errors.New("scheduler runtime role can access the News schema")
	}
	return nil
}

func poolForDSN(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(normalizePostgresDSN(dsn))
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 1
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return p, nil
}

func checkExternalRole(ctx context.Context, role, dsn string) error {
	p, err := poolForDSN(ctx, dsn)
	if err != nil {
		return errors.New("runtime database connection unavailable")
	}
	defer p.Close()
	var superuser, bypass, createdb, createrole, ownsDatabase, ownsObjectsOutsideScheduler, member, schemaUsage, schemaCreate bool
	ownedOutside := `n.nspname NOT IN ('pg_catalog','information_schema')`
	if role == "scheduler" {
		ownedOutside = `n.nspname NOT IN ('pg_catalog','information_schema','scheduler_service')`
	}
	query := `SELECT r.rolsuper,r.rolbypassrls,r.rolcreatedb,r.rolcreaterole,
	pg_has_role(current_user,d.datdba,'MEMBER'),
	(EXISTS(SELECT 1 FROM pg_namespace n WHERE ` + ownedOutside + ` AND pg_has_role(current_user,n.nspowner,'MEMBER')) OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE ` + ownedOutside + ` AND pg_has_role(current_user,c.relowner,'MEMBER'))),
		EXISTS(SELECT 1 FROM pg_roles i WHERE pg_has_role(current_user,i.oid,'MEMBER') AND (i.rolsuper OR i.rolbypassrls OR i.rolcreatedb OR i.rolcreaterole)),
	(current_schema() IS NOT NULL AND has_schema_privilege(current_user,current_schema(),'USAGE')),
	(current_schema()='scheduler_service' AND has_schema_privilege(current_user,current_schema(),'CREATE'))
		FROM pg_roles r CROSS JOIN pg_database d WHERE r.rolname=current_user AND d.datname=current_database()`
	err = p.QueryRow(ctx, query).Scan(&superuser, &bypass, &createdb, &createrole, &ownsDatabase, &ownsObjectsOutsideScheduler, &member, &schemaUsage, &schemaCreate)
	if err != nil {
		return errors.New("privilege inspection unavailable")
	}
	if superuser || bypass || createdb || createrole || ownsDatabase || ownsObjectsOutsideScheduler || member || !schemaUsage || (role == "scheduler" && !schemaCreate) {
		return errors.New("role is elevated or owns target objects")
	}
	return nil
}

func requirePreparedExternalNews(ctx context.Context, p *pgxpool.Pool) error {
	var ready bool
	if err := p.QueryRow(ctx, `SELECT to_regclass('news.installation_state') IS NOT NULL AND to_regclass('news.integration_credentials') IS NOT NULL AND to_regclass('news.managed_admin') IS NOT NULL`).Scan(&ready); err != nil || !ready {
		return errors.New("external News schema must be fully migrated before installation")
	}
	rows, err := p.Query(ctx, `SELECT tablename FROM pg_tables WHERE schemaname='news' ORDER BY tablename`)
	if err != nil {
		return errors.New("external News schema inspection failed")
	}
	var tables []string
	for rows.Next() {
		var name string
		if rows.Scan(&name) != nil {
			rows.Close()
			return errors.New("external News schema inspection failed")
		}
		tables = append(tables, name)
	}
	if rows.Err() != nil {
		rows.Close()
		return errors.New("external News schema inspection failed")
	}
	rows.Close()
	for _, table := range []string{"articles", "sources", "categories", "category_mapping", "daily_summaries", "settings", "schedule_settings", "installation_state", "managed_admin", "setup_tokens", "integration_credentials", "background_jobs", "background_job_executions", "article_ingestion_runs", "article_processing_runs"} {
		found := false
		for _, name := range tables {
			if name == table {
				found = true
				break
			}
		}
		if !found {
			return errors.New("external News schema is incomplete; apply reviewed schema migrations before retrying")
		}
	}
	for _, table := range tables {
		var count int64
		query := `SELECT count(*) FROM news.` + pgx.Identifier{table}.Sanitize()
		if err := p.QueryRow(ctx, query).Scan(&count); err != nil {
			return errors.New("external News schema inspection failed")
		}
		if table == "background_worker_status" && count == 1 {
			// Migration 009 creates the known singleton in a stopped state.
			// Any changed ownership/generation/heartbeat means this is not a
			// pristine installation and must be handled through adoption.
			var pristine bool
			if err := p.QueryRow(ctx, `SELECT id=1 AND generation=0 AND owner_id IS NULL AND lock_backend_pid IS NULL AND heartbeat_at IS NULL AND status='stopped' FROM news.background_worker_status`).Scan(&pristine); err != nil || !pristine {
				return errors.New("external News schema contains data; use adopt for an existing installation")
			}
			continue
		}
		if count != 0 {
			return errors.New("external News schema contains data; use adopt for an existing installation")
		}
	}
	return nil
}

func requireEmptySchedulerSchema(ctx context.Context, dsn string) error {
	p, err := poolForDSN(ctx, dsn)
	if err != nil {
		return errors.New("pre-provisioned scheduler schema unavailable")
	}
	defer p.Close()
	var empty bool
	if err = p.QueryRow(ctx, `SELECT current_schema()='scheduler_service' AND NOT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='scheduler_service' AND c.relkind IN ('r','p','v','m','S','f'))`).Scan(&empty); err != nil || !empty {
		return errors.New("scheduler_service must be pre-provisioned and empty")
	}
	return nil
}

func preflightExternalRuntimePrivileges(ctx context.Context, input externalInput) error {
	if err := verifyExternalGrantPolicy(ctx, input); err != nil {
		return err
	}
	for role, tables := range map[string][]string{"web": {"integration_credentials"}, "domain": {"managed_admin", "setup_tokens"}, "worker": {"integration_credentials", "managed_admin", "setup_tokens"}, "execution": {"integration_credentials", "managed_admin", "setup_tokens"}, "ingestion": {"integration_credentials", "managed_admin", "setup_tokens"}, "processing": {"integration_credentials", "managed_admin", "setup_tokens"}} {
		pool, err := poolForDSN(ctx, input.RuntimeDSNs[role])
		if err != nil {
			return fmt.Errorf("%s runtime grants are unavailable", role)
		}
		for _, table := range tables {
			for _, priv := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
				allowed, checkErr := roleHasAnyPrivilege(ctx, pool, "news."+table, priv)
				if checkErr != nil || allowed {
					pool.Close()
					return fmt.Errorf("%s runtime role has forbidden %s access to %s", role, priv, table)
				}
			}
		}
		pool.Close()
	}
	return nil
}

func roleHasAnyPrivilege(ctx context.Context, p *pgxpool.Pool, table, privilege string) (bool, error) {
	var allowed bool
	if privilege == "DELETE" {
		err := p.QueryRow(ctx, `SELECT has_table_privilege(current_user,$1,$2)`, table, privilege).Scan(&allowed)
		return allowed, err
	}
	err := p.QueryRow(ctx, `SELECT has_table_privilege(current_user,$1,$2) OR EXISTS (
		SELECT 1 FROM pg_attribute a WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped
		AND has_column_privilege(current_user,a.attrelid,a.attname,$2))`, table, privilege).Scan(&allowed)
	return allowed, err
}

func roleHasPrivilege(ctx context.Context, p *pgxpool.Pool, table, privilege string) (bool, error) {
	return roleHasAnyPrivilege(ctx, p, table, privilege)
}

func preflightExternalInstallerPrivileges(ctx context.Context, p *pgxpool.Pool) error {
	checks := []struct{ table, privilege string }{
		{"news.installation_state", "SELECT"}, {"news.installation_state", "INSERT"},
		{"news.managed_admin", "SELECT"}, {"news.managed_admin", "UPDATE"},
		{"news.setup_tokens", "SELECT"}, {"news.setup_tokens", "INSERT"}, {"news.setup_tokens", "UPDATE"},
	}
	for _, check := range checks {
		allowed, err := roleHasPrivilege(ctx, p, check.table, check.privilege)
		if err != nil || !allowed {
			return errors.New("bootstrap role lacks required installer table privileges")
		}
	}
	return nil
}

func ensureExternalIdentity(ctx context.Context, p *pgxpool.Pool, id string) error {
	tx, err := p.Begin(ctx)
	if err != nil {
		return errors.New("external installation identity write failed")
	}
	defer tx.Rollback(ctx)
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM news.installation_state)`).Scan(&exists); err != nil || exists {
		return errors.New("external installation identity is ambiguous")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO news.installation_state(id,installation_id,revision,provisioning_complete,maintenance_mode,onboarding_step,onboarding_completed) VALUES(1,$1,1,true,false,'storage',false)`, id); err != nil {
		return errors.New("external installation identity write failed")
	}
	if err = tx.Commit(ctx); err != nil {
		return errors.New("external installation identity commit failed")
	}
	return nil
}

func connectInstallerState(ctx context.Context, l managedsetup.Layout, s managedsetup.State) (*pgxpool.Pool, error) {
	if !s.ExternalDB {
		return connect(ctx, s, "newsdb")
	}
	path := filepath.Join(l.Setup, "external-bootstrap.json")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || info.Size() > 8192 {
		return nil, errors.New("external installer connection unavailable")
	}
	b, err := os.ReadFile(path)
	if err != nil || strings.TrimSpace(string(b)) == "" {
		return nil, errors.New("external installer connection unavailable")
	}
	bootstrap, err := pgx.ParseConfig(normalizePostgresDSN(strings.TrimSpace(string(b))))
	roleDSN := s.RuntimeValues["domain"]["DATABASE_URL"]
	runtime, parseErr := pgx.ParseConfig(normalizePostgresDSN(roleDSN))
	if err != nil || parseErr != nil || bootstrap.Host != runtime.Host || bootstrap.Port != runtime.Port || bootstrap.Database != runtime.Database || bootstrap.RuntimeParams["search_path"] != "news" || bootstrap.User == runtime.User {
		return nil, errors.New("external installer connection identity mismatch")
	}
	return poolForDSN(ctx, strings.TrimSpace(string(b)))
}
