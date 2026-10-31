// news-domain serves the private domain operations and callbacks.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/backup"
	"github.com/Dzarlax-AI/evening-news/go-domain/callbacks"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/digestworkflow"
	"github.com/Dzarlax-AI/evening-news/go-domain/domainops"
	"github.com/Dzarlax-AI/evening-news/go-domain/extractorclient"
	"github.com/Dzarlax-AI/evening-news/go-domain/learning"
	"github.com/Dzarlax-AI/evening-news/go-domain/media"
	"github.com/Dzarlax-AI/evening-news/go-domain/selectors"
	"github.com/Dzarlax-AI/evening-news/go-domain/sourceprobe"
	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
	"github.com/Dzarlax-AI/evening-news/go-domain/terminalevents"
	"github.com/Dzarlax-AI/evening-news/go-domain/webbackend"
	"github.com/Dzarlax-AI/evening-news/go-tools/incidentfilter"
	"github.com/jackc/pgx/v5/pgxpool"
)

func tokenOK(s string) bool {
	if len(s) < 16 || len(s) > 4096 {
		return false
	}
	for _, b := range []byte(s) {
		if b < 0x21 || b > 0x7e {
			return false
		}
	}
	return true
}
func poolBudget() (int32, error) { return poolBudgetFrom(os.Getenv) }
func poolBudgetFrom(get func(string) string) (int32, error) {
	size, overflow := 5, 10
	var err error
	if v := get("DB_POOL_SIZE"); v != "" {
		size, err = strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
	}
	if v := get("DB_MAX_OVERFLOW"); v != "" {
		overflow, err = strconv.Atoi(v)
		if err != nil {
			return 0, err
		}
	}
	if size <= 0 || overflow < 0 || size > 15 || overflow > 15 || size+overflow < 4 {
		return 0, errors.New("invalid database budget")
	}
	if size+overflow > 15 {
		return 14, nil
	}
	// pg_dump opens one connection outside pgx. Keep it inside the original
	// total budget even while the singleton guard occupies a pool connection.
	return int32(size + overflow - 1), nil
}

type requests struct {
	mu       sync.Mutex
	count    int
	draining bool
	done     chan struct{}
}

func (s *requests) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.draining {
			s.mu.Unlock()
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		s.count++
		s.mu.Unlock()
		defer func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.count--
			if s.draining && s.count == 0 {
				close(s.done)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
func (s *requests) beginDrain() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.draining {
		s.draining = true
		if s.count == 0 {
			close(s.done)
		}
	}
	return s.done
}
func main() {
	if len(os.Args) > 1 && os.Args[1] == "incident-log-filter" {
		os.Exit(incidentLogFilter(os.Args[2:], os.Stdin, os.Stdout))
	}
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := healthcheck(); err != nil {
			log.Print("healthcheck failed")
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 1 {
		log.Print("unknown command")
		os.Exit(1)
	}
	if err := run(); err != nil {
		log.Print("news-domain stopped: ", err)
		os.Exit(1)
	}
}

func incidentLogFilter(args []string, in io.Reader, out io.Writer) int {
	if len(args) != 2 || args[0] != "--role" {
		return 2
	}
	if err := incidentfilter.Filter(in, out, args[1]); err != nil {
		if err.Error() == "log_overflow" {
			return 3
		}
		return 2
	}
	return 0
}
func run() error {
	source, err := loadSource()
	if err != nil {
		return errors.New("invalid runtime manifest")
	}
	config, err := airuntime.DomainConfigFromLookup(source.get, source.lookup)
	if err != nil {
		return errors.New("invalid runtime configuration")
	}
	jobToken, aiToken := source.get("JOB_EXECUTOR_TOKEN"), source.get("ARTICLE_AI_TOKEN")
	if !tokenOK(jobToken) || !tokenOK(aiToken) {
		return errors.New("invalid private service tokens")
	}
	origin := source.get("EXECUTION_SERVICE_URL")
	if origin == "" {
		origin = "http://background:8101"
	}
	execution, err := authority.NewContextClient(origin, jobToken)
	if err != nil {
		return errors.New("invalid execution authority configuration")
	}
	defer execution.Close()
	maxConns, err := poolBudgetFrom(source.get)
	if err != nil {
		return err
	}
	limit, err := textLimitFrom(source.get)
	if err != nil {
		return err
	}
	mediaLimit, err := mediaByteLimitFrom(source.get)
	if err != nil {
		return err
	}
	cfg, err := pgxpool.ParseConfig(config.DatabaseURL)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	cfg.MaxConns = maxConns
	root, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	pool, err := pgxpool.NewWithConfig(root, cfg)
	if err != nil {
		return errors.New("database unavailable")
	}
	abandoned := false
	defer func() {
		if !abandoned {
			pool.Close()
		}
	}()
	if source.managed {
		check, cancel := context.WithTimeout(root, 3*time.Second)
		var id string
		var complete bool
		err := pool.QueryRow(check, `SELECT installation_id,provisioning_complete FROM installation_state WHERE id=1`).Scan(&id, &complete)
		cancel()
		if err != nil || id != source.installation || !complete {
			return errors.New("managed installation is not activated")
		}
	}
	guard, err := authority.AcquireGuard(root, pool)
	if err != nil {
		return errors.New("domain singleton unavailable")
	}
	defer func() {
		if abandoned {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = guard.Close(ctx)
	}()
	runtime := airuntime.NewServer(config, pool)
	var credentials *credentialstore.Store
	if source.managed {
		credentials = &credentialstore.Store{Pool: pool, Key: source.key, KeyID: source.keyID, InstallationID: source.installation}
		runtime.SetCredentialStore(credentials)
	}
	attention, err := runtime.StartupAISettings(root)
	if err != nil {
		return errors.New("invalid effective AI settings")
	}
	if attention {
		log.Print("AI settings require attention; generation unavailable until repaired")
	}
	runtime.StartUsageWorkers(1)
	appCtx, cancelApp := context.WithCancel(root)
	defer cancelApp()
	mux := http.NewServeMux()
	instance, err := digestworkflow.NewUUID()
	if err != nil {
		return errors.New("domain identity unavailable")
	}
	ops := domainops.New(appCtx, jobToken, instance, guard, execution, pool, runtime)
	backupService := backup.New(".", config.DatabaseURL)
	backupService.Managed = source.managed
	ops.Backup = backupService
	mediaService := media.New()
	mediaService.MaxBytes = mediaLimit
	ops.Media = mediaService
	extractorURL, extractorToken := source.get("EXTRACTION_SERVICE_URL"), source.get("EXTRACTION_SERVICE_TOKEN")
	if !tokenOK(extractorToken) {
		return errors.New("invalid extractor token")
	}
	extractorClient, e := extractorclient.New(extractorURL, extractorToken)
	if e != nil {
		return errors.New("invalid extractor configuration")
	}
	defer extractorClient.Close()
	ops.Source = &sourceprobe.Probe{Client: extractorClient}
	memory := learning.New(pool)
	memory.Guard = guard
	mux.Handle("POST /internal/v1/article-ai", &callbacks.Handler{Token: aiToken, Guard: guard, Authority: execution, Pool: pool, Admission: callbacks.NewAdmission(16, 64, 1024*1024), Compute: &callbacks.ArticleCompute{Runtime: runtime, Learning: memory, MaxTextChars: limit}})
	mux.Handle("POST /internal/v1/ingestion/selectors", &selectors.Handler{Token: jobToken, Guard: guard, Authority: execution, Pool: pool, Store: memory, Analyzer: selectors.NewAnalyzer(runtime)})
	allowCreateAll := true
	if raw, present := source.lookup("ALLOW_CREATE_ALL"); present && raw != "" {
		allowCreateAll, err = strconv.ParseBool(raw)
	}
	if err != nil {
		return errors.New("invalid ALLOW_CREATE_ALL")
	}
	worker := source.get("BACKGROUND_EXECUTION_MODE") != "embedded"
	web := &webbackend.Server{Credentials: credentials, Guard: guard, Pool: pool, Runtime: runtime, Worker: worker, AllowCreateAll: allowCreateAll, TelegramToken: config.TelegramToken, ChatID: config.TelegramMain, ServiceChatID: config.TelegramService, ViberSiteBase: config.SiteBase}
	mux.HandleFunc("GET /internal/v1/runtime-configuration", func(w http.ResponseWriter, r *http.Request) {
		expected := "Bearer " + jobToken
		given := r.Header.Get("Authorization")
		if len(given) != len(expected) || subtle.ConstantTimeCompare([]byte(given), []byte(expected)) != 1 {
			http.Error(w, "Unauthorized", 401)
			return
		}
		if guard.Check(r.Context()) != nil {
			http.Error(w, "Unavailable", 503)
			return
		}
		web.RuntimeConfiguration(w, r)
	})
	mux.Handle("/internal/v1/job-configuration", jobConfigurationHandler{pool: pool, guard: guard, token: jobToken})
	mux.Handle("/internal/web/v1/", webbackend.Boundary{Token: source.get("WEB_BACKEND_TOKEN"), Worker: worker, Next: web.Handler()})
	terminal := terminalevents.New(jobToken, pool, guard, execution, func(ctx context.Context, title, message string) bool {
		result, err := runtime.SendServiceTest(ctx, "🚨 <b>"+title+"</b>\n\n"+message)
		return err == nil && result.State == telegramdelivery.Sent
	})
	mux.Handle("POST /internal/v1/domain-events/finalized", terminal)
	mux.HandleFunc("POST /internal/v1/domain-operations", ops.Post)
	mux.HandleFunc("GET /internal/v1/domain-operations/{job}/{attempt}/{operation}/{id}", ops.Get)
	mux.HandleFunc("GET /internal/v1/status", func(w http.ResponseWriter, r *http.Request) {
		expected := "Bearer " + jobToken
		given := r.Header.Get("Authorization")
		if len(given) != len(expected) || subtle.ConstantTimeCompare([]byte(given), []byte(expected)) != 1 {
			http.Error(w, `{"detail":"Unauthorized"}`, http.StatusUnauthorized)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		active, accepting := ops.Status()
		if !accepting || !runtime.DirectReady() || guard.Check(ctx) != nil || pool.Ping(ctx) != nil {
			http.Error(w, `{"detail":"Executor runtime is unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"domain_instance_id": instance, "active_operations": active, "ready": true})
	})
	host := source.get("JOB_EXECUTOR_HOST")
	if host == "" {
		host = "0.0.0.0"
	}
	port := source.get("JOB_EXECUTOR_PORT")
	if port == "" {
		port = "8100"
	}
	if _, err = strconv.ParseUint(port, 10, 16); err != nil {
		return errors.New("invalid listen port")
	}
	tracker := &requests{done: make(chan struct{})}
	server := &http.Server{Addr: net.JoinHostPort(host, port), Handler: tracker.wrap(mux), ReadHeaderTimeout: 3 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: func(net.Listener) context.Context { return appCtx }}
	errCh := make(chan error, 1)
	go func() { errCh <- server.ListenAndServe() }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var serveErr error
loop:
	for {
		select {
		case <-root.Done():
			break loop
		case e := <-errCh:
			if !errors.Is(e, http.ErrServerClosed) {
				serveErr = e
			}
			break loop
		case <-ticker.C:
			if guard.Check(root) != nil {
				if root.Err() == nil {
					serveErr = errors.New("domain singleton lost")
				}
				break loop
			}
		}
	}
	done := tracker.beginDrain()
	cancelApp()
	runtime.BeginDrain()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err = server.Shutdown(shutdown); err != nil {
		_ = server.Close()
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		// Do not unlock while a cancellation-resistant operation may still run.
		// main exits the process, which closes the guarded PostgreSQL session.
		abandoned = true
		return errors.New("active domain operations failed to drain")
	}
	drainCtx, stopDrain := context.WithTimeout(context.Background(), 5*time.Second)
	err = ops.Drain(drainCtx)
	stopDrain()
	if err != nil {
		abandoned = true
		return errors.New("domain operations failed to drain")
	}
	runtime.CloseUsage(5 * time.Second)
	return serveErr
}
func textLimit() (int, error) { return textLimitFrom(os.Getenv) }
func textLimitFrom(get func(string) string) (int, error) {
	v := strings.TrimSpace(get("EXTRACTION_TEXT_MAX_CHARS"))
	if v == "" {
		return 100000, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, errors.New("invalid text limit")
	}
	return n, nil
}
func mediaByteLimit() (int64, error) { return mediaByteLimitFrom(os.Getenv) }
func mediaByteLimitFrom(get func(string) string) (int64, error) {
	v := strings.TrimSpace(get("EXTRACTION_HTTP_MAX_BYTES"))
	if v == "" {
		return 5 * 1024 * 1024, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 1 || n > int64(^uint(0)>>1)/4 {
		return 0, errors.New("invalid media byte limit")
	}
	return n, nil
}
