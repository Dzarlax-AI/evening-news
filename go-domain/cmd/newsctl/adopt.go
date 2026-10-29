package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/argon2"
)

const adoptMaxInput = 8192

type adoptionBootstrap struct {
	Version   int    `json:"version"`
	Bootstrap string `json:"bootstrap_dsn"`
}

type adoptionSecrets struct {
	AdminHash     string                              `json:"admin_hash"`
	AdminUsername string                              `json:"admin_username"`
	Credentials   map[string]credentialstore.Envelope `json:"credentials"`
}

type adoptionPlan struct {
	id, source       string
	roles            map[string]map[string]string
	services         map[string]string
	secrets          adoptionSecrets
	keys             map[string][]string
	unsupported      map[string][]string
	legacy           map[string]map[string]string
	localHTTPPortsOK bool
}

var adoptionPolicies = map[string]struct{ Allowed, Required []string }{
	"web":        {[]string{"DATABASE_URL", "LOCAL_HTTP_SETUP", "WEB_LISTEN_ADDRESS", "ADMIN_USERNAME", "JWT_SECRET", "DEVELOPMENT", "TRUST_FORWARD_AUTH", "TRUSTED_PROXY_CIDRS", "BACKGROUND_EXECUTION_MODE", "SCHEDULE_DISPATCH_MODE", "SCHEDULED_JOBS_TOKEN", "SCHEDULED_JOBS_RECEIPT_LIMIT", "WEB_BACKEND_URL", "WEB_BACKEND_TOKEN", "SCHEDULER_API_URL", "SCHEDULER_API_TOKEN", "SCHEDULER_API_ALLOW_HTTP", "SITE_BASE_URL", "ALLOWED_ORIGINS", "TRUSTED_HOSTS"}, []string{"DATABASE_URL", "JWT_SECRET", "WEB_BACKEND_URL", "WEB_BACKEND_TOKEN", "SCHEDULED_JOBS_TOKEN", "SCHEDULER_API_URL", "SCHEDULER_API_TOKEN"}},
	"domain":     {[]string{"DATABASE_URL", "DB_POOL_SIZE", "DB_MAX_OVERFLOW", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "EXECUTION_SERVICE_URL", "EXTRACTION_SERVICE_URL", "EXTRACTION_SERVICE_TOKEN", "EXTRACTION_TEXT_MAX_CHARS", "EXTRACTION_HTTP_MAX_BYTES", "ALLOW_CREATE_ALL", "BACKGROUND_EXECUTION_MODE", "WEB_BACKEND_TOKEN", "JOB_EXECUTOR_HOST", "JOB_EXECUTOR_PORT", "GEMINI_API_ENDPOINT", "SUMMARIZATION_MODEL", "LOCAL_CATEGORY", "AI_PROVIDER", "OPENAI_ARTICLE_MODEL", "OPENAI_DIGEST_MODEL", "OPENAI_TEXT_MODEL", "TEXT_MODEL", "TELEGRAM_CHAT_ID", "TELEGRAM_SERVICE_CHAT_ID", "DIGEST_MODEL", "SITE_BASE_URL", "TELEGRAM_RICH_MESSAGES_ENABLED", "RPS", "NEWS_AI_DB_MAX_CONNS", "AI_MODEL_PRICES", "AI_INPUT_COST_PER_1M", "AI_OUTPUT_COST_PER_1M", "AI_CACHED_INPUT_COST_PER_1M", "NEWS_CREDENTIAL_KEY", "NEWS_CREDENTIAL_KEY_ID"}, []string{"DATABASE_URL", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "EXTRACTION_SERVICE_URL", "EXTRACTION_SERVICE_TOKEN", "WEB_BACKEND_TOKEN", "NEWS_CREDENTIAL_KEY", "NEWS_CREDENTIAL_KEY_ID"}},
	"background": {[]string{"ARTICLE_AI_TOKEN", "ARTICLE_AI_URL", "ARTICLE_INGESTION_DB_MAX_CONNS", "ARTICLE_INGESTION_LISTEN", "ARTICLE_INGESTION_TOKEN", "ARTICLE_PROCESSING_DB_MAX_CONNS", "ARTICLE_PROCESSING_LISTEN", "ARTICLE_PROCESSING_TOKEN", "BACKGROUND_HEALTH_LISTEN", "BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS", "BACKGROUND_WORKER_BACKOFF_MAX", "BACKGROUND_WORKER_BACKOFF_MIN", "DOMAIN_SERVICE_URL", "EXECUTION_DATABASE_URL", "EXECUTION_DB_SCHEMA", "EXECUTION_MAX_CONNECTIONS", "EXTRACTION_BROWSER_MAX_BYTES", "EXTRACTION_HTTP_MAX_BYTES", "EXTRACTION_SERVICE_NATIVE_BROWSER", "EXTRACTION_SERVICE_TIMEOUT_SECONDS", "EXTRACTION_SERVICE_TOKEN", "EXTRACTION_SERVICE_TOTAL_BUDGET_SECONDS", "EXTRACTION_SERVICE_URL", "EXTRACTION_TEXT_MAX_CHARS", "INGESTION_DATABASE_URL", "JOB_EXECUTOR_LISTEN", "JOB_EXECUTOR_REQUEST_TIMEOUT_SECONDS", "JOB_EXECUTOR_TOKEN", "JOB_EXECUTOR_URL", "MAX_WORKERS", "NEWS_LIMIT_DAYS", "NEWS_LIMIT_ENABLED", "NEWS_LIMIT_MAX_ARTICLES", "NEWS_LIMIT_NEWEST_DATE", "NEWS_LIMIT_PER_SOURCE", "PROCESSING_ATTEMPT_TIMEOUT_SECONDS", "PROCESSING_BATCH_SIZE", "PROCESSING_DATABASE_URL", "PROCESSING_MAX_ATTEMPTS", "PROCESSING_RETRY_INITIAL_SECONDS", "PROCESSING_RETRY_LATER_SECONDS", "SCHEDULER_ALLOW_HTTP_TARGETS", "SCHEDULER_API_TOKEN", "SCHEDULER_DATABASE_URL", "SCHEDULER_DB_MAX_CONNS", "SCHEDULER_DB_SCHEMA", "SCHEDULER_HTTP_TIMEOUT", "SCHEDULER_LISTEN_ADDR", "SCHEDULER_MAX_INFLIGHT", "SCHEDULER_MAX_OUTBOX", "SCHEDULER_POLL_INTERVAL", "SCHEDULER_START_PAUSED", "SCHEDULER_TARGET_ALIASES", "SCHEDULER_TARGET_NEWS_WORKER_URL", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN", "SELECTOR_SERVICE_URL", "WORKER_DATABASE_URL", "WORKER_JOB_RETENTION_DAYS", "WORKER_MAX_JOB_ATTEMPTS", "WORKER_POLL_INTERVAL_SECONDS", "WORKER_SHUTDOWN_DRAIN_SECONDS"}, []string{"WORKER_DATABASE_URL", "EXECUTION_DATABASE_URL", "INGESTION_DATABASE_URL", "PROCESSING_DATABASE_URL", "SCHEDULER_DATABASE_URL", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "ARTICLE_INGESTION_TOKEN", "ARTICLE_PROCESSING_TOKEN", "SCHEDULER_API_TOKEN", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN", "EXTRACTION_SERVICE_TOKEN"}},
	"extractor":  {[]string{"LISTEN_ADDR", "EXTRACTOR_SERVICE_TOKEN", "EXTRACTOR_MAX_CONCURRENT", "EXTRACTOR_BROWSER_CONCURRENT", "CHROME_ENDPOINT", "EXTRACTOR_HTTP_ONLY", "EXTRACTOR_BROWSER_BACKEND", "EXTRACTOR_NATIVE_PROXY_URL", "EXTRACTOR_NATIVE_PROXY_TOKEN", "EXTRACTOR_NATIVE_NETWORK_ISOLATED", "EXTRACTOR_NATIVE_READY_SELECTOR", "EXTRACTOR_NATIVE_DIAGNOSTICS"}, []string{"EXTRACTOR_SERVICE_TOKEN"}},
	"egress":     {[]string{"EGRESS_LISTEN_ADDR", "EGRESS_TOKEN", "EGRESS_ALLOW_WILDCARD_BIND", "EGRESS_MAX_CONCURRENT", "EGRESS_MAX_TUNNEL_BYTES", "EGRESS_MAX_LIFETIME_SECONDS", "EGRESS_TEST_MODE", "EGRESS_TEST_ALLOWLIST"}, []string{"EGRESS_TOKEN"}},
}

var secretConfigKeys = map[string]string{"GEMINI_API_KEY": "gemini", "OPENAI_API_KEY": "openai", "TELEGRAM_TOKEN": "telegram"}

func adoptionPlanFromCompose(c managedsetup.RenderedCompose, id string) (adoptionPlan, error) {
	webService := "app"
	if len(c.Services[webService]) == 0 {
		webService = "web"
	}
	p := adoptionPlan{id: id, source: c.Digest, roles: map[string]map[string]string{}, services: map[string]string{"web": webService, "domain": "executor", "background": "background", "extractor": "extractor"}, keys: c.Keys(), unsupported: map[string][]string{}, legacy: c.Services, localHTTPPortsOK: c.HasLoopbackPublishedPorts(webService)}
	if c.ServiceNames["migrate"] {
		p.services["migrate"] = "migrate"
	}
	services := map[string]string{webService: "web", "executor": "domain", "background": "background", "extractor": "extractor"}
	if p.services["egress"] != "" {
		services["egress"] = "egress"
	}
	allowed := map[string]bool{}
	for role, policy := range adoptionPolicies {
		for _, key := range policy.Allowed {
			allowed[role+"\x00"+key] = true
		}
	}
	for service, role := range services {
		values := c.Services[service]
		p.roles[role] = map[string]string{}
		for key, value := range values {
			if key == "ADMIN_PASSWORD" || secretConfigKeys[key] != "" {
				continue
			}
			if role == "web" && retiredLegacyAppKey(key) {
				continue
			}
			if !allowed[role+"\x00"+key] {
				p.unsupported[service] = append(p.unsupported[service], key)
				continue
			}
			p.roles[role][key] = value
		}
	}
	if p.legacy[webService]["ADMIN_PASSWORD"] == "" && p.legacy["executor"]["ADMIN_PASSWORD"] == "" {
		return adoptionPlan{}, errors.New("legacy administrator credential unavailable")
	}
	if _, ok := p.roles["web"]["DATABASE_URL"]; !ok {
		return adoptionPlan{}, errors.New("required web configuration unavailable")
	}
	if _, ok := p.roles["domain"]["DATABASE_URL"]; !ok {
		return adoptionPlan{}, errors.New("required executor configuration unavailable")
	}
	if p.roles["extractor"]["EXTRACTOR_HTTP_ONLY"] == "false" || p.roles["extractor"]["EXTRACTOR_BROWSER_BACKEND"] == "native" {
		if _, err := managedsetup.NativeBrowserOptionsFromCompose(c); err != nil {
			return adoptionPlan{}, errors.New("native browser adoption requires a supported isolated rendered Compose contract")
		}
		if c.ServiceNames["egress"] || len(c.Services["egress"]) != 0 {
			p.services["egress"] = "egress"
			p.roles["egress"] = map[string]string{}
			for key, value := range c.Services["egress"] {
				if !allowed["egress\x00"+key] {
					p.unsupported["egress"] = append(p.unsupported["egress"], key)
					continue
				}
				p.roles["egress"][key] = value
			}
		}
		if err := validateNativeAdoption(p); err != nil {
			return adoptionPlan{}, err
		}
	}
	if err := validateAdoptionParity(p); err != nil {
		return adoptionPlan{}, err
	}
	for role, policy := range adoptionPolicies {
		if role == "domain" {
			continue
		} // the two NEWS_CREDENTIAL values are generated on apply
		if role == "egress" && p.services["egress"] == "" {
			continue
		}
		for _, key := range policy.Required {
			service := role
			if role == "domain" {
				service = "executor"
			}
			if strings.TrimSpace(p.roles[role][key]) == "" {
				return adoptionPlan{}, fmt.Errorf("required %s configuration key %s unavailable", service, key)
			}
		}
	}
	return p, nil
}

func retiredLegacyAppKey(key string) bool {
	switch key {
	case "BROWSER_WS_ENDPOINT", "BROWSER_RESTART_MARKER_PATH", "BROWSER_PROXY_ENABLED", "BROWSER_PROXY_URL", "BROWSER_PROXY_TOKEN", "EXTRACTION_SERVICE_MODE", "EXTRACTION_SERVICE_URL", "EXTRACTION_SERVICE_TOKEN", "EXTRACTION_SERVICE_NATIVE_BROWSER", "EXTRACTION_SERVICE_PYTHON_FALLBACK", "TELEGRAM_SOURCE_MODE", "PAGE_MONITOR_SOURCE_MODE":
		return true
	default:
		return false
	}
}

func validateNativeAdoption(p adoptionPlan) error {
	app, extractor, egress := p.legacy[p.services["web"]], p.legacy[p.services["extractor"]], p.roles["egress"]
	background := p.roles["background"]
	if background["EXTRACTION_SERVICE_NATIVE_BROWSER"] == "" && app["EXTRACTION_SERVICE_NATIVE_BROWSER"] == "true" {
		background["EXTRACTION_SERVICE_NATIVE_BROWSER"] = "true"
	}
	if background["EXTRACTION_SERVICE_NATIVE_BROWSER"] != "true" || (app["EXTRACTION_SERVICE_NATIVE_BROWSER"] != "" && app["EXTRACTION_SERVICE_NATIVE_BROWSER"] != "true") {
		return errors.New("native browser adoption requires the existing background native-extractor mode")
	}
	if extractor["EXTRACTOR_HTTP_ONLY"] != "false" || extractor["EXTRACTOR_BROWSER_BACKEND"] != "native" || extractor["EXTRACTOR_NATIVE_NETWORK_ISOLATED"] != "true" {
		return errors.New("native browser adoption requires native backend, HTTP fallback disabled, and isolated network mode")
	}
	for _, key := range []string{"EXTRACTOR_SERVICE_TOKEN", "CHROME_ENDPOINT", "EXTRACTOR_NATIVE_PROXY_URL", "EXTRACTOR_NATIVE_PROXY_TOKEN", "EXTRACTOR_MAX_CONCURRENT", "EXTRACTOR_BROWSER_CONCURRENT"} {
		if strings.TrimSpace(extractor[key]) == "" {
			return fmt.Errorf("native browser adoption requires extractor setting %s", key)
		}
	}
	if extractor["EXTRACTOR_SERVICE_TOKEN"] != background["EXTRACTION_SERVICE_TOKEN"] || (p.roles["domain"]["EXTRACTION_SERVICE_TOKEN"] != "" && p.roles["domain"]["EXTRACTION_SERVICE_TOKEN"] != extractor["EXTRACTOR_SERVICE_TOKEN"]) {
		return errors.New("native browser extractor service credentials do not match across services")
	}
	if len(egress) == 0 || egress["EGRESS_TOKEN"] == "" || egress["EGRESS_TOKEN"] != extractor["EXTRACTOR_NATIVE_PROXY_TOKEN"] {
		return errors.New("native browser adoption requires the matching isolated egress service")
	}
	return nil
}

func validateAdoptionParity(p adoptionPlan) error {
	web, domain, bg, extract := p.roles["web"], p.roles["domain"], p.roles["background"], p.roles["extractor"]
	if domain["AI_PROVIDER"] == "" {
		domain["AI_PROVIDER"] = "gemini"
	}
	if web["ADMIN_USERNAME"] == "" {
		web["ADMIN_USERNAME"] = "admin"
	}
	if web["JWT_SECRET"] == "" {
		fallback := p.legacy[p.services["web"]]["ADMIN_PASSWORD"]
		if fallback == "" {
			fallback = p.legacy["executor"]["ADMIN_PASSWORD"]
		}
		if fallback == "" {
			return errors.New("effective legacy signing key unavailable")
		}
		web["JWT_SECRET"] = fallback
	}
	// Cross-service purpose credentials must be identical before switching to
	// service-scoped manifests. This detects accidental role-map drift.
	pairs := [][4]string{{"web", "WEB_BACKEND_TOKEN", "domain", "WEB_BACKEND_TOKEN"}, {"web", "SCHEDULER_API_TOKEN", "background", "SCHEDULER_API_TOKEN"}, {"web", "SCHEDULED_JOBS_TOKEN", "background", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN"}, {"domain", "JOB_EXECUTOR_TOKEN", "background", "JOB_EXECUTOR_TOKEN"}, {"domain", "ARTICLE_AI_TOKEN", "background", "ARTICLE_AI_TOKEN"}, {"domain", "EXTRACTION_SERVICE_TOKEN", "background", "EXTRACTION_SERVICE_TOKEN"}, {"background", "EXTRACTION_SERVICE_TOKEN", "extractor", "EXTRACTOR_SERVICE_TOKEN"}}
	roles := map[string]map[string]string{"web": web, "domain": domain, "background": bg, "extractor": extract}
	for _, pair := range pairs {
		a, b := roles[pair[0]][pair[1]], roles[pair[2]][pair[3]]
		if a == "" || b == "" || a != b {
			return fmt.Errorf("purpose token parity failed for %s/%s", pair[0], pair[1])
		}
	}
	schedulerSchema := bg["SCHEDULER_DB_SCHEMA"]
	scheduler, err := pgx.ParseConfig(normalizePostgresDSN(bg["SCHEDULER_DATABASE_URL"]))
	if err != nil || schedulerSchema != "scheduler_service" || scheduler.RuntimeParams["search_path"] != schedulerSchema {
		return errors.New("scheduler database must use the dedicated scheduler_service schema")
	}
	return sameNewsDatabase(p.roles)
}

func validateAdoptionWebTransport(p adoptionPlan) error {
	web := p.roles["web"]
	site := strings.TrimSpace(web["SITE_BASE_URL"])
	if site == "" {
		return errors.New("adoption --apply requires SITE_BASE_URL in the canonical web configuration")
	}
	var proxies []string
	for _, value := range strings.Split(web["TRUSTED_PROXY_CIDRS"], ",") {
		if value = strings.TrimSpace(value); value != "" {
			proxies = append(proxies, value)
		}
	}
	localHTTP := web["LOCAL_HTTP_SETUP"] == "true"
	if managedsetup.IsLoopbackHTTPOrigin(site) && localHTTP && !p.localHTTPPortsOK {
		return errors.New("loopback HTTP adoption requires LOCAL_HTTP_SETUP=true and every published web port bound to loopback; otherwise use HTTPS with trusted proxy CIDRs")
	}
	return managedsetup.ValidateWebTransport(site, managedsetup.Options{TrustedProxyCIDRs: proxies}, localHTTP)
}

func sameNewsDatabase(roles map[string]map[string]string) error {
	dsns := []string{roles["web"]["DATABASE_URL"], roles["domain"]["DATABASE_URL"], roles["background"]["WORKER_DATABASE_URL"], roles["background"]["EXECUTION_DATABASE_URL"], roles["background"]["INGESTION_DATABASE_URL"], roles["background"]["PROCESSING_DATABASE_URL"]}
	var first *pgx.ConnConfig
	for _, dsn := range dsns {
		cfg, err := pgx.ParseConfig(normalizePostgresDSN(dsn))
		if err != nil {
			return errors.New("News database role DSN is invalid")
		}
		if first == nil {
			first = cfg
			continue
		}
		if cfg.Host != first.Host || cfg.Port != first.Port || cfg.Database != first.Database {
			return errors.New("News runtime roles target different databases")
		}
	}
	return nil
}

func adoptionCommand(ctx context.Context, l managedsetup.Layout, args []string) error {
	flags := flagSet("adopt")
	configPath := flags.String("config", "", "protected rendered Compose JSON")
	bootstrapPath := flags.String("bootstrap-input", "", "protected JSON containing bootstrap_dsn")
	composeOutput := flags.String("compose-output", "", "new Compose override output path")
	dryRun := flags.Bool("dry-run", false, "inspect without mutation (the default)")
	apply := flags.Bool("apply", false, "apply adoption to an isolated compatible database")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *configPath == "" || (*apply && *dryRun) {
		return errors.New("usage: newsctl adopt --config PATH [--dry-run | --apply --bootstrap-input PATH --compose-output PATH]")
	}
	c, err := managedsetup.ReadRenderedCompose(*configPath)
	if err != nil {
		return err
	}
	id, err := adoptionID(l, c.Digest)
	if err != nil {
		return err
	}
	plan, err := adoptionPlanFromCompose(c, id)
	if err != nil {
		return err
	}
	printAdoptionNames(plan)
	if len(plan.unsupported) != 0 {
		return errors.New("unsupported runtime key; adoption remains dry-run only")
	}
	if !*apply {
		fmt.Println("Runtime role privileges and current schema parity: deferred until --apply; no network connections made")
		fmt.Println("Bootstrap role: provide protected input for migration DDL preflight")
		fmt.Println("No values were printed; no files or database rows were changed")
		return nil
	}
	if *bootstrapPath == "" || *composeOutput == "" {
		return errors.New("--bootstrap-input and --compose-output are required for --apply")
	}
	if err = validateAdoptionWebTransport(plan); err != nil {
		return err
	}
	if err = preflightAdoptionRoles(ctx, plan); err != nil {
		return err
	}
	bootstrapDSN, err := readAdoptionBootstrap(*bootstrapPath)
	if err != nil {
		return err
	}
	if err = sameBootstrapDatabase(bootstrapDSN, plan.roles); err != nil {
		return err
	}
	if err = verifyAdoptionSchemaParity(ctx, bootstrapDSN, plan.roles); err != nil {
		return err
	}
	return applyAdoption(ctx, l, plan, bootstrapDSN, *composeOutput)
}

func flagSet(name string) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	return f
}

func printAdoptionNames(p adoptionPlan) {
	services := make([]string, 0, len(p.keys))
	for service := range p.keys {
		services = append(services, service)
	}
	sort.Strings(services)
	fmt.Println("Adoption groups and keys:")
	for _, service := range services {
		fmt.Printf("  %s: %s\n", service, strings.Join(p.keys[service], ", "))
	}
	if len(p.unsupported) > 0 {
		fmt.Println("Unsupported runtime keys:")
		for service, keys := range p.unsupported {
			sort.Strings(keys)
			fmt.Printf("  %s: %s\n", service, strings.Join(keys, ", "))
		}
	}
}

func adoptionID(l managedsetup.Layout, digest string) (string, error) {
	root := filepath.Join(l.Setup, "adoptions")
	entries, err := os.ReadDir(root)
	if err != nil && !os.IsNotExist(err) {
		return "", errors.New("adoption state unavailable")
	}
	var found string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var b managedsetup.AdoptionBundle
		raw, e := os.ReadFile(filepath.Join(root, entry.Name(), "bundle.json"))
		if e != nil || json.Unmarshal(raw, &b) != nil || b.InstallationID != entry.Name() || b.SourceDigest != digest {
			return "", errors.New("existing adoption stage is ambiguous; inspect or recover it before retrying")
		}
		if b.SourceDigest == digest {
			if found != "" {
				return "", errors.New("multiple adoption stages match input")
			}
			found = b.InstallationID
		}
	}
	if found != "" {
		return found, nil
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", errors.New("adoption identity generation failed")
	}
	return hex.EncodeToString(b), nil
}

func readAdoptionBootstrap(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 || info.Size() <= 0 || info.Size() > adoptMaxInput {
		return "", errors.New("bootstrap input must be a protected regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", errors.New("bootstrap input unavailable")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return "", errors.New("bootstrap input changed while opening")
	}
	raw, err := io.ReadAll(io.LimitReader(f, adoptMaxInput+1))
	if err != nil || len(raw) > adoptMaxInput {
		return "", errors.New("bootstrap input unavailable")
	}
	var value adoptionBootstrap
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if d.Decode(&value) != nil || d.Decode(new(any)) != io.EOF || value.Version != 1 || strings.TrimSpace(value.Bootstrap) == "" {
		return "", errors.New("invalid bootstrap input")
	}
	if _, err = pgx.ParseConfig(normalizePostgresDSN(value.Bootstrap)); err != nil {
		return "", errors.New("invalid bootstrap database connection")
	}
	return value.Bootstrap, nil
}

func preflightAdoptionRoles(ctx context.Context, p adoptionPlan) error {
	dsns := map[string]string{"web": p.roles["web"]["DATABASE_URL"], "domain": p.roles["domain"]["DATABASE_URL"], "worker": p.roles["background"]["WORKER_DATABASE_URL"], "execution": p.roles["background"]["EXECUTION_DATABASE_URL"], "ingestion": p.roles["background"]["INGESTION_DATABASE_URL"], "processing": p.roles["background"]["PROCESSING_DATABASE_URL"], "scheduler": p.roles["background"]["SCHEDULER_DATABASE_URL"]}
	for role, dsn := range dsns {
		if dsn == "" {
			return fmt.Errorf("%s database role is missing; apply requires a complete runtime role bundle", role)
		}
		cfg, err := pgxpool.ParseConfig(normalizePostgresDSN(dsn))
		if err != nil {
			return errors.New("runtime database role configuration invalid")
		}
		cfg.MaxConns = 1
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pool, err := pgxpool.NewWithConfig(probeCtx, cfg)
		if err != nil {
			cancel()
			return errors.New("runtime database role preflight failed")
		}
		var superuser, bypass, createDB, createRole, ownsTarget, privilegedMember bool
		err = pool.QueryRow(probeCtx, `SELECT r.rolsuper,r.rolbypassrls,r.rolcreatedb,r.rolcreaterole,
			pg_has_role(current_user,d.datdba,'MEMBER'),
			EXISTS(SELECT 1 FROM pg_roles inherited WHERE pg_has_role(current_user,inherited.oid,'MEMBER') AND (inherited.rolsuper OR inherited.rolbypassrls OR inherited.rolcreatedb OR inherited.rolcreaterole))
			FROM pg_roles r CROSS JOIN pg_database d WHERE r.rolname=current_user AND d.datname=current_database()`).Scan(&superuser, &bypass, &createDB, &createRole, &ownsTarget, &privilegedMember)
		if err == nil {
			var ownsObjects bool
			ownsObjects, err = adoptionRoleOwnsForbiddenObjects(probeCtx, pool, role == "scheduler")
			ownsTarget = ownsTarget || ownsObjects
		}
		pool.Close()
		cancel()
		if err != nil {
			return fmt.Errorf("%s role privilege preflight unavailable", role)
		}
		if superuser || bypass || createDB || createRole || ownsTarget || privilegedMember {
			return fmt.Errorf("%s runtime role is privileged; required grants must be corrected before adoption", role)
		}
	}
	return nil
}

func adoptionRoleOwnsForbiddenObjects(ctx context.Context, pool *pgxpool.Pool, scheduler bool) (bool, error) {
	ownerPredicate := `EXISTS (SELECT 1 FROM pg_namespace n WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND pg_has_role(current_user,n.nspowner,'MEMBER')) OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN ('pg_catalog','information_schema') AND pg_has_role(current_user,c.relowner,'MEMBER'))`
	if scheduler {
		ownerPredicate = `EXISTS (
			SELECT 1 FROM pg_namespace n
			WHERE n.nspname NOT IN ('pg_catalog','information_schema')
			  AND pg_has_role(current_user,n.nspowner,'MEMBER')
			  AND (n.nspname <> 'scheduler_service' OR n.nspowner<>(SELECT oid FROM pg_roles WHERE rolname=current_user))
		) OR EXISTS (
			SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
			WHERE n.nspname NOT IN ('pg_catalog','information_schema')
			  AND pg_has_role(current_user,c.relowner,'MEMBER')
			  AND (n.nspname <> 'scheduler_service' OR c.relowner<>(SELECT oid FROM pg_roles WHERE rolname=current_user))
			  AND NOT (n.nspname='pg_toast' AND EXISTS (
				SELECT 1 FROM pg_class parent JOIN pg_namespace pn ON pn.oid=parent.relnamespace
				WHERE pn.nspname='scheduler_service'
				  AND parent.relowner=(SELECT oid FROM pg_roles WHERE rolname=current_user)
				  AND (parent.reltoastrelid=c.oid OR EXISTS (
					SELECT 1 FROM pg_index toast_index
					WHERE toast_index.indexrelid=c.oid AND toast_index.indrelid=parent.reltoastrelid
				  ))
			  ))
		)`
	}
	var owns bool
	err := pool.QueryRow(ctx, `SELECT `+ownerPredicate).Scan(&owns)
	return owns, err
}

func sameBootstrapDatabase(bootstrap string, roles map[string]map[string]string) error {
	base, err := pgx.ParseConfig(normalizePostgresDSN(bootstrap))
	if err != nil {
		return errors.New("bootstrap database configuration invalid")
	}
	main, err := pgx.ParseConfig(normalizePostgresDSN(roles["domain"]["DATABASE_URL"]))
	if err != nil || base.Host != main.Host || base.Port != main.Port || base.Database != main.Database {
		return errors.New("bootstrap connection must target the same News database")
	}
	for _, role := range []string{"web", "domain"} {
		cfg, e := pgx.ParseConfig(normalizePostgresDSN(roles[role]["DATABASE_URL"]))
		if e != nil || cfg.User == base.User {
			return errors.New("bootstrap identity must be separate from runtime database roles")
		}
	}
	for _, key := range []string{"WORKER_DATABASE_URL", "EXECUTION_DATABASE_URL", "INGESTION_DATABASE_URL", "PROCESSING_DATABASE_URL"} {
		cfg, e := pgx.ParseConfig(normalizePostgresDSN(roles["background"][key]))
		if e != nil || cfg.User == base.User {
			return errors.New("bootstrap identity must be separate from runtime database roles")
		}
	}
	return nil
}

func applyAdoption(ctx context.Context, l managedsetup.Layout, p adoptionPlan, bootstrapDSN, composeOutput string) error {
	// Keep integration secrets out of all runtime manifests and temporary files.
	legacy := map[string]string{}
	for key := range secretConfigKeys {
		legacy[key] = p.legacy["executor"][key]
	}
	legacy["ADMIN_PASSWORD"] = p.legacy[p.services["web"]]["ADMIN_PASSWORD"]
	if legacy["ADMIN_PASSWORD"] == "" {
		legacy["ADMIN_PASSWORD"] = p.legacy["executor"]["ADMIN_PASSWORD"]
	}
	if legacy["ADMIN_PASSWORD"] == "" {
		return errors.New("legacy administrator credential unavailable")
	}
	if len(legacy["ADMIN_PASSWORD"]) < 1 || len(legacy["ADMIN_PASSWORD"]) > 1024 {
		return errors.New("legacy administrator credential invalid")
	}

	dir := filepath.Join(l.Setup, "adoptions", p.id)
	var secrets adoptionSecrets
	var domainManifest runtimeconfig.Manifest
	var key []byte
	var keyID string
	if _, e := os.Stat(dir); e == nil {
		rawDomain, e := os.ReadFile(filepath.Join(dir, "domain.json"))
		if e != nil || json.Unmarshal(rawDomain, &domainManifest) != nil {
			return errors.New("staged domain state unavailable")
		}
		for role, file := range map[string]string{"web": "web.json", "domain": "domain.json", "background": "background.json", "extractor": "extractor.json", "egress": "egress.json"} {
			raw, e := os.ReadFile(filepath.Join(dir, file))
			if role == "egress" && os.IsNotExist(e) && p.services[role] == "" {
				continue
			}
			var manifest runtimeconfig.Manifest
			if e != nil || json.Unmarshal(raw, &manifest) != nil || manifest.Service != role || manifest.InstallationID != p.id {
				return errors.New("staged runtime state is ambiguous")
			}
			p.roles[role] = manifest.Values
		}
		if _, _, e := managedsetup.StageAdoption(l.Setup, p.id, p.source, p.roles, adoptionPolicies, nil); e != nil {
			return errors.New("staged adoption integrity check failed")
		}
		rawSecrets, e := os.ReadFile(filepath.Join(dir, "import.json"))
		if e != nil || json.Unmarshal(rawSecrets, &secrets) != nil || secrets.AdminHash == "" || secrets.AdminUsername == "" {
			return errors.New("staged import is ambiguous")
		}
	} else if !os.IsNotExist(e) {
		return errors.New("adoption staging unavailable")
	} else {
		key = make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return errors.New("adoption key generation failed")
		}
		keyIDBytes := make([]byte, 16)
		if _, err := rand.Read(keyIDBytes); err != nil {
			return errors.New("adoption key generation failed")
		}
		keyID = base64.RawURLEncoding.EncodeToString(keyIDBytes)
		p.roles["domain"]["NEWS_CREDENTIAL_KEY"] = base64.RawStdEncoding.EncodeToString(key)
		p.roles["domain"]["NEWS_CREDENTIAL_KEY_ID"] = keyID
		salt := make([]byte, 16)
		if _, err := rand.Read(salt); err != nil {
			return errors.New("administrator import unavailable")
		}
		passwordHash := argon2.IDKey([]byte(legacy["ADMIN_PASSWORD"]), salt, 2, 19456, 1, 32)
		username := p.roles["web"]["ADMIN_USERNAME"]
		if username == "" {
			username = "admin"
		}
		secrets = adoptionSecrets{AdminHash: fmt.Sprintf("$argon2id$v=19$m=19456,t=2,p=1$%s$%s", base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(passwordHash)), AdminUsername: username, Credentials: map[string]credentialstore.Envelope{}}
		for keyName, provider := range secretConfigKeys {
			secret := legacy[keyName]
			if strings.TrimSpace(secret) == "" {
				continue
			}
			envelope, err := credentialstore.Seal(key, keyID, p.id, provider, []byte(secret))
			if err != nil {
				return errors.New("credential import preparation failed")
			}
			secrets.Credentials[provider] = envelope
		}
		secretBytes, err := json.Marshal(secrets)
		if err != nil {
			return errors.New("adoption staging failed")
		}
		override, err := managedsetup.AdoptionComposeOverride(p.services)
		if err != nil {
			return errors.New("adoption Compose generation failed")
		}
		dir, _, err = managedsetup.StageAdoption(l.Setup, p.id, p.source, p.roles, adoptionPolicies, map[string][]byte{"import.json": secretBytes, "compose-override.yml": override})
		if err != nil {
			return err
		}
		rawDomain, err := os.ReadFile(filepath.Join(dir, "domain.json"))
		if err != nil || json.Unmarshal(rawDomain, &domainManifest) != nil {
			return errors.New("staged domain state unavailable")
		}
	}
	key, err := base64.RawStdEncoding.DecodeString(domainManifest.Values["NEWS_CREDENTIAL_KEY"])
	if err != nil || len(key) != 32 {
		return errors.New("staged credential key invalid")
	}
	keyID = domainManifest.Values["NEWS_CREDENTIAL_KEY_ID"]
	for provider, envelope := range secrets.Credentials {
		if err := ensureImportedCredential(key, keyID, p.id, provider, envelope); err != nil {
			return errors.New("staged credential integrity check failed")
		}
	}

	bootstrapConfig, err := pgxpool.ParseConfig(normalizePostgresDSN(bootstrapDSN))
	if err != nil {
		return errors.New("bootstrap database configuration invalid")
	}
	bootstrapConfig.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, bootstrapConfig)
	if err != nil {
		return errors.New("bootstrap database unavailable")
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return errors.New("bootstrap database unavailable")
	}
	if err = verifyAdoptionSchemaParity(ctx, bootstrapDSN, p.roles); err != nil {
		return err
	}
	if err = requireAdoptionOffline(ctx, p.roles); err != nil {
		return err
	}
	before, err := adoptionPreservationSnapshot(ctx, pool)
	if err != nil {
		return errors.New("legacy data preservation preflight failed")
	}
	result, err := migration.RunAdoption(ctx, pool)
	if err != nil || len(result.Errors) != 0 {
		return errors.New("managed additive migration unavailable; verify schema ownership and provide the one-time bootstrap connection")
	}
	if err = verifyManagedRuntimeGrants(ctx, p.roles); err != nil {
		return err
	}
	after, err := adoptionPreservationSnapshot(ctx, pool)
	if err != nil || !equalStringMaps(before, after) {
		return errors.New("legacy data parity failed; managed activation refused")
	}
	if err = importAdoption(ctx, pool, p.id, secrets, p.roles["domain"], p.roles["background"]); err != nil {
		return err
	}
	// The committed marker is written only after the DB import and byte-parity
	// validation. Runtime volumes are populated with O_EXCL semantics.
	if err = managedsetup.ActivateAdoption(l, dir, p.id, composeOutput, adoptionPolicies); err != nil {
		return err
	}
	fmt.Println("Adoption imported and role files activated. Start the News services with the same canonical Compose files used to create the protected input, plus the generated override.")
	return nil
}

func verifyManagedRuntimeGrants(ctx context.Context, roles map[string]map[string]string) error {
	type need struct{ table, privilege, column string }
	needs := map[string][]need{
		"web": {
			{"installation_state", "SELECT", ""},
			{"managed_admin", "SELECT", ""}, {"managed_admin", "INSERT", ""}, {"managed_admin", "UPDATE", ""}, {"managed_admin", "DELETE", ""},
			{"setup_tokens", "SELECT", ""}, {"setup_tokens", "INSERT", ""}, {"setup_tokens", "UPDATE", ""}, {"setup_tokens", "DELETE", ""},
			{"installation_state", "UPDATE", "onboarding_step"}, {"installation_state", "UPDATE", "onboarding_completed"}, {"installation_state", "UPDATE", "revision"}, {"installation_state", "UPDATE", "updated_at"},
		},
		"domain": {
			{"installation_state", "SELECT", ""},
			{"integration_credentials", "SELECT", ""}, {"integration_credentials", "INSERT", ""}, {"integration_credentials", "UPDATE", ""},
		},
	}
	var missing []string
	for service, required := range needs {
		key := "DATABASE_URL"
		if roles[service][key] == "" {
			return fmt.Errorf("%s managed runtime database configuration unavailable", service)
		}
		pool, err := poolForDSN(ctx, roles[service][key])
		if err != nil {
			return fmt.Errorf("%s managed runtime role could not be verified", service)
		}
		var schema, roleName string
		if err = pool.QueryRow(ctx, `SELECT current_schema(), current_user`).Scan(&schema, &roleName); err != nil || schema == "" || roleName == "" {
			pool.Close()
			return fmt.Errorf("%s managed runtime role could not be verified", service)
		}
		for _, n := range required {
			var allowed bool
			qualified := pgx.Identifier{schema, n.table}.Sanitize()
			if n.column == "" {
				err = pool.QueryRow(ctx, `SELECT has_table_privilege(current_user,$1,$2)`, qualified, n.privilege).Scan(&allowed)
			} else {
				err = pool.QueryRow(ctx, `SELECT has_column_privilege(current_user,$1,$2,$3)`, qualified, n.column, n.privilege).Scan(&allowed)
			}
			if err != nil {
				pool.Close()
				return fmt.Errorf("%s managed runtime grants could not be verified", service)
			}
			if !allowed {
				if n.column == "" {
					missing = append(missing, fmt.Sprintf("GRANT %s ON %s TO %s;", n.privilege, qualified, pgx.Identifier{roleName}.Sanitize()))
				} else {
					missing = append(missing, fmt.Sprintf("GRANT UPDATE (%s) ON %s TO %s;", pgx.Identifier{n.column}.Sanitize(), qualified, pgx.Identifier{roleName}.Sanitize()))
				}
			}
		}
		pool.Close()
	}
	if len(missing) != 0 {
		return fmt.Errorf("managed adoption is missing runtime grants; review and apply these scoped grants before retrying: %s", strings.Join(missing, " "))
	}
	return nil
}

func verifyAdoptionSchemaParity(ctx context.Context, bootstrapDSN string, roles map[string]map[string]string) error {
	dsns := []string{roles["web"]["DATABASE_URL"], roles["domain"]["DATABASE_URL"], roles["background"]["WORKER_DATABASE_URL"], roles["background"]["EXECUTION_DATABASE_URL"], roles["background"]["INGESTION_DATABASE_URL"], roles["background"]["PROCESSING_DATABASE_URL"], bootstrapDSN}
	want := ""
	for _, dsn := range dsns {
		pool, err := poolForDSN(ctx, dsn)
		if err != nil {
			return errors.New("News database schema preflight unavailable")
		}
		var schema string
		err = pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema)
		pool.Close()
		if err != nil || schema == "" {
			return errors.New("News database schema preflight unavailable")
		}
		if want == "" {
			want = schema
		} else if schema != want {
			return errors.New("News runtime roles resolve to different schemas")
		}
	}
	return nil
}

func requireAdoptionOffline(ctx context.Context, roles map[string]map[string]string) error {
	roleDSNs := []string{roles["web"]["DATABASE_URL"], roles["domain"]["DATABASE_URL"], roles["background"]["WORKER_DATABASE_URL"], roles["background"]["EXECUTION_DATABASE_URL"], roles["background"]["INGESTION_DATABASE_URL"], roles["background"]["PROCESSING_DATABASE_URL"], roles["background"]["SCHEDULER_DATABASE_URL"]}
	unique := make(map[string]string)
	for _, dsn := range roleDSNs {
		cfg, err := pgx.ParseConfig(normalizePostgresDSN(dsn))
		if err != nil {
			return errors.New("runtime database role configuration invalid")
		}
		unique[cfg.Host+"\x00"+fmt.Sprint(cfg.Port)+"\x00"+cfg.User] = dsn
	}
	activeTotal := 0
	for _, dsn := range unique {
		pool, err := poolForDSN(ctx, dsn)
		if err != nil {
			return errors.New("could not verify runtime services are stopped")
		}
		var active int
		err = pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename=current_user AND pid<>pg_backend_pid()`).Scan(&active)
		pool.Close()
		if err != nil {
			return errors.New("could not verify runtime services are stopped")
		}
		activeTotal += active
	}
	if activeTotal > 0 {
		return fmt.Errorf("adoption requires all runtime services stopped; %d runtime database session(s) remain", activeTotal)
	}
	return nil
}

func adoptionPreservationSnapshot(ctx context.Context, pool *pgxpool.Pool) (map[string]string, error) {
	// These whole-row snapshots prove that adoption preserved the existing
	// settings, sources, category definitions, mappings, and schedule instants.
	// The SQL identifiers are fixed; no user-provided schema/table name is used.
	queries := map[string]string{
		"sources":           `SELECT count(*)::text||':'||md5(COALESCE(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),'')) FROM sources t`,
		"categories":        `SELECT count(*)::text||':'||md5(COALESCE(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),'')) FROM categories t`,
		"category_mapping":  `SELECT count(*)::text||':'||md5(COALESCE(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),'')) FROM category_mapping t`,
		"settings":          `SELECT count(*)::text||':'||md5(COALESCE(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),'')) FROM settings t`,
		"schedule_settings": `SELECT count(*)::text||':'||md5(COALESCE(string_agg(to_jsonb(t)::text,E'\\n' ORDER BY to_jsonb(t)::text),'')) FROM schedule_settings t`,
	}
	out := make(map[string]string, len(queries))
	for name, query := range queries {
		var digest string
		if err := pool.QueryRow(ctx, query).Scan(&digest); err != nil {
			return nil, err
		}
		out[name] = digest
	}
	return out, nil
}

func equalStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func (p adoptionPlan) keysValue(service, key string) string {
	if p.roles[service] != nil {
		return p.roles[service][key]
	}
	return ""
}

func importAdoption(ctx context.Context, pool *pgxpool.Pool, id string, s adoptionSecrets, domain, background map[string]string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.New("adoption database transaction unavailable")
	}
	defer tx.Rollback(ctx)
	var current string
	err = tx.QueryRow(ctx, `SELECT installation_id FROM installation_state WHERE id=1 FOR UPDATE`).Scan(&current)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return errors.New("adoption database state unavailable")
	}
	if current != "" && current != id {
		return errors.New("database belongs to another installation")
	}
	if current == "" {
		if _, err = tx.Exec(ctx, `INSERT INTO installation_state(id,installation_id,revision,provisioning_complete,maintenance_mode,onboarding_step,onboarding_completed) VALUES(1,$1,1,true,false,'finish',true)`, id); err != nil {
			return errors.New("adoption state import failed")
		}
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM managed_admin WHERE id=1`).Scan(&count); err != nil {
		return errors.New("adoption administrator state unavailable")
	}
	if count == 0 {
		if _, err = tx.Exec(ctx, `INSERT INTO managed_admin(id,username,password_hash,revision) VALUES(1,$1,$2,1)`, s.AdminUsername, s.AdminHash); err != nil {
			return errors.New("adoption administrator import failed")
		}
	} else {
		var name, hash string
		var revision int64
		if err = tx.QueryRow(ctx, `SELECT username,password_hash,revision FROM managed_admin WHERE id=1`).Scan(&name, &hash, &revision); err != nil || name != s.AdminUsername || hash != s.AdminHash || revision != 1 {
			return errors.New("existing administrator state conflicts with adoption")
		}
	}
	for provider, envelope := range s.Credentials {
		var revision int64
		err = tx.QueryRow(ctx, `SELECT revision FROM integration_credentials WHERE provider=$1 FOR UPDATE`, provider).Scan(&revision)
		if err == nil {
			var version int
			var keyID string
			var nonce, ciphertext []byte
			if err = tx.QueryRow(ctx, `SELECT envelope_version,key_id,nonce,ciphertext FROM integration_credentials WHERE provider=$1`, provider).Scan(&version, &keyID, &nonce, &ciphertext); err != nil || version != envelope.Version || keyID != envelope.KeyID || !bytes.Equal(nonce, envelope.Nonce) || !bytes.Equal(ciphertext, envelope.Ciphertext) {
				return errors.New("existing integration credential conflicts with adoption")
			}
			continue
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return errors.New("credential state unavailable")
		}
		if _, err = tx.Exec(ctx, `INSERT INTO integration_credentials(provider,revision,envelope_version,key_id,nonce,ciphertext) VALUES($1,1,$2,$3,$4,$5)`, provider, envelope.Version, envelope.KeyID, envelope.Nonce, envelope.Ciphertext); err != nil {
			return errors.New("integration credential import failed")
		}
	}
	if err = importEffectiveSettings(ctx, tx, domain, background); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return errors.New("adoption database commit unconfirmed")
	}
	return nil
}

func importEffectiveSettings(ctx context.Context, tx pgx.Tx, domain, background map[string]string) error {
	// Preserve existing DB overrides. Seed only keys whose old effective value
	// came from the Compose environment so managed mode can remove that source.
	for _, pair := range [][2]string{{"telegram_chat_id", domain["TELEGRAM_CHAT_ID"]}, {"telegram_service_chat_id", domain["TELEGRAM_SERVICE_CHAT_ID"]}} {
		if err := importLegacyStringSetting(ctx, tx, pair[0], pair[1]); err != nil {
			return err
		}
	}
	// Operational bounds are independent from AI provider settings and must be
	// captured even when an installation already has a DB-backed AI override.
	if err := importOperationalSettings(ctx, tx, background); err != nil {
		return err
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM settings WHERE key='ai_settings')`).Scan(&exists); err != nil {
		return errors.New("AI settings state unavailable")
	}
	if exists {
		return nil
	}
	provider := domain["AI_PROVIDER"]
	if provider == "" {
		provider = "gemini"
	}
	gemArticle := domain["SUMMARIZATION_MODEL"]
	if gemArticle == "" {
		gemArticle = "gemini-3.5-flash-lite"
	}
	gemDigest := domain["DIGEST_MODEL"]
	if gemDigest == "" {
		gemDigest = gemArticle
	}
	gemText := domain["TEXT_MODEL"]
	if gemText == "" {
		gemText = gemArticle
	}
	profiles := map[string]any{
		"gemini": map[string]any{"models": map[string]string{"article": gemArticle, "digest": gemDigest, "text": gemText}},
		"openai": map[string]any{"models": map[string]string{"article": domain["OPENAI_ARTICLE_MODEL"], "digest": domain["OPENAI_DIGEST_MODEL"], "text": domain["OPENAI_TEXT_MODEL"]}},
	}
	if provider == "openai" && (domain["OPENAI_ARTICLE_MODEL"] == "" || domain["OPENAI_DIGEST_MODEL"] == "" || domain["OPENAI_TEXT_MODEL"] == "") {
		return errors.New("active OpenAI model profile is incomplete")
	}
	raw, err := json.Marshal(map[string]any{"active_provider": provider, "profiles": profiles})
	if err != nil {
		return errors.New("AI settings import failed")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value) VALUES('ai_settings',$1::jsonb) ON CONFLICT(key) DO NOTHING`, raw); err != nil {
		return errors.New("AI settings import failed")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value) VALUES('ai_settings_revision',to_jsonb(1::bigint)) ON CONFLICT(key) DO NOTHING`); err != nil {
		return errors.New("AI settings revision import failed")
	}
	return nil
}

func importLegacyStringSetting(ctx context.Context, tx pgx.Tx, key, envValue string) error {
	var raw []byte
	err := tx.QueryRow(ctx, `SELECT value FROM settings WHERE key=$1 FOR UPDATE`, key).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		if strings.TrimSpace(envValue) == "" {
			return nil
		}
		if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value) VALUES($1,to_jsonb($2::text))`, key, envValue); err != nil {
			return errors.New("effective setting import failed")
		}
		return nil
	}
	if err != nil {
		return errors.New("effective setting import failed")
	}
	var existing string
	if string(raw) != "null" {
		if err = json.Unmarshal(raw, &existing); err != nil {
			return errors.New("effective setting import conflict")
		}
	}
	if strings.TrimSpace(existing) != "" || strings.TrimSpace(envValue) == "" {
		return nil
	}
	if _, err = tx.Exec(ctx, `UPDATE settings SET value=to_jsonb($2::text) WHERE key=$1`, key, envValue); err != nil {
		return errors.New("effective setting import failed")
	}
	return nil
}

func importOperationalSettings(ctx context.Context, tx pgx.Tx, env map[string]string) error {
	const revisionKey = "operational_settings_revision"
	keys := []string{"operational_limits_enabled", "operational_max_articles", "operational_per_source", "operational_days", "operational_newest_date", "operational_processing_enabled", "operational_delivery_enabled"}
	var hasRevision bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM settings WHERE key=$1)`, revisionKey).Scan(&hasRevision); err != nil {
		return errors.New("operational settings state unavailable")
	}
	var present int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM settings WHERE key=ANY($1)`, keys).Scan(&present); err != nil {
		return errors.New("operational settings state unavailable")
	}
	if hasRevision {
		return nil
	}
	if present != 0 {
		return errors.New("partial operational settings state conflicts with adoption")
	}
	parseBool := func(key string, fallback bool) (bool, error) {
		value, ok := env[key]
		if !ok || strings.TrimSpace(value) == "" {
			return fallback, nil
		}
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		return parsed, err
	}
	parseInt := func(key string, fallback int) (int, error) {
		value, ok := env[key]
		if !ok || strings.TrimSpace(value) == "" {
			return fallback, nil
		}
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		return parsed, err
	}
	enabled, err := parseBool("NEWS_LIMIT_ENABLED", false)
	if err != nil {
		return errors.New("legacy operational limit is invalid")
	}
	maxArticles, err := parseInt("NEWS_LIMIT_MAX_ARTICLES", 50)
	if err != nil {
		return errors.New("legacy operational limit is invalid")
	}
	perSource, err := parseInt("NEWS_LIMIT_PER_SOURCE", 50)
	if err != nil {
		return errors.New("legacy operational limit is invalid")
	}
	days, err := parseInt("NEWS_LIMIT_DAYS", 1)
	if err != nil {
		return errors.New("legacy operational limit is invalid")
	}
	newestDate := strings.TrimSpace(env["NEWS_LIMIT_NEWEST_DATE"])
	if maxArticles < 1 || maxArticles > 100000 || perSource < 1 || perSource > 100000 || days < 0 || days > 36500 {
		return errors.New("legacy operational limit is out of range")
	}
	if newestDate != "" {
		if _, err = time.Parse("2006-01-02", newestDate); err != nil {
			return errors.New("legacy operational date is invalid")
		}
	}
	values := []any{enabled, maxArticles, perSource, days, newestDate, true, true}
	for i, key := range keys {
		encoded, err := json.Marshal(values[i])
		if err != nil {
			return errors.New("operational settings import failed")
		}
		if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value) VALUES($1,$2::jsonb)`, key, encoded); err != nil {
			return errors.New("operational settings import failed")
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value) VALUES($1,to_jsonb(1::bigint))`, revisionKey); err != nil {
		return errors.New("operational settings revision import failed")
	}
	return nil
}

// ensureImportedCredential verifies the envelope was sealed with the staged key.
func ensureImportedCredential(key []byte, keyID, installation, provider string, e credentialstore.Envelope) error {
	plain, err := credentialstore.Open(key, keyID, installation, provider, e)
	for i := range plain {
		plain[i] = 0
	}
	return err
}
