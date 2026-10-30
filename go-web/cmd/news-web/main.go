package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/httpui"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/jobs"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/management"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/schedules"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/transport"
)

func main() {
	if err := run(); err != nil {
		slog.Error("news web stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	db, err := repositories.OpenPostgres(ctx, cfg.DatabaseURL, 8)
	if err == nil {
		err = db.CheckSchema(ctx)
	}
	cancel()
	if err != nil {
		if db != nil {
			db.Close()
		}
		return err
	}
	defer db.Close()
	if cfg.Managed {
		check, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		var id string
		var complete bool
		err := db.Pool().QueryRow(check, `SELECT installation_id,provisioning_complete FROM installation_state WHERE id=1`).Scan(&id, &complete)
		cancel()
		if err != nil || id != cfg.InstallationID || !complete {
			return errors.New("managed installation is not activated")
		}
	}
	authService := auth.New(auth.Config{Username: cfg.AdminUsername, Password: cfg.AdminPassword, JWTSecret: cfg.JWTSecret, Development: cfg.Development || cfg.LocalHTTPSetup, TrustForwardAuth: cfg.TrustForwardAuth})
	if cfg.Managed {
		authService.UseManaged(auth.NewManagedStore(db.Pool(), cfg.InstallationID))
	}
	started := time.Now().UTC()
	ui := httpui.New(httpui.Config{Reader: db, PublicAdmin: func(r *http.Request) bool { _, ok := authService.PublicAdmin(r); return ok }, TemplatesDir: "templates", StaticDir: "../web/static", StartedAt: started, Version: "2.0.0", SiteBaseURL: cfg.SiteBaseURL})
	mux := http.NewServeMux()
	if err := httpui.RegisterDocs(mux, "contracts/openapi.json"); err != nil {
		return err
	}
	ui.Register(mux)
	if cfg.Managed {
		httpui.RegisterSetup(mux, authService, db.Pool(), cfg.InstallationID)
	}
	authService.Register(mux)
	httpui.RegisterAdmin(mux, authService, ui)
	jobStore := repositories.NewJobRepository(db.Pool())
	jobService := jobs.New(jobStore)
	managementRepository := repositories.NewManagementRepository(db.Pool())
	privateOps, err := backend.NewClient(backend.Config{BaseURL: cfg.WebBackendURL, Token: cfg.WebBackendToken, Timeout: cfg.WebBackendTimeout})
	if err != nil {
		return err
	}
	managementService := management.New(managementRepository, jobService, privateOps)
	if cfg.Managed {
		managementService.SetJobPreflight(management.NewConfigurationPreflight(privateOps))
	}
	httpui.RegisterManagement(mux, httpui.ManagementConfig{
		Service: managementService, Auth: authService,
		PublicAdmin:               func(r *http.Request) bool { _, ok := authService.PublicAdmin(r); return ok },
		ScheduledJobsToken:        cfg.ScheduledJobsToken,
		WorkerMode:                cfg.BackgroundExecutionMode == "worker",
		DispatchMode:              cfg.ScheduleDispatchMode,
		ScheduledJobsReceiptLimit: cfg.ScheduledJobsReceiptLimit,
		EditorialDigestStatus:     db.EditorialDigestStatus,
	})
	httpui.RegisterBackend(mux, authService, privateOps)
	if cfg.Managed {
		client, err := schedules.NewClient(schedules.ClientConfig{Endpoint: cfg.SchedulerURL, Token: cfg.SchedulerToken, AllowHTTP: cfg.SchedulerAllowHTTP, Timeout: 5 * time.Second})
		if err != nil {
			return err
		}
		schedules.NewManagedHandler(schedules.NewRoot(client, repositories.NewScheduleJobsSnapshot(db.Pool()), nil)).Register(mux, authService)
	} else {
		schedules.NewHandlerFromEnv(repositories.NewScheduleJobsSnapshot(db.Pool())).Register(mux, authService)
	}
	trustedProxies := make([]*net.IPNet, 0, len(cfg.TrustedProxyCIDRs))
	for _, cidr := range cfg.TrustedProxyCIDRs {
		_, block, e := net.ParseCIDR(cidr)
		if e != nil {
			return errors.New("invalid trusted proxy configuration")
		}
		trustedProxies = append(trustedProxies, block)
	}
	var routes http.Handler = mux
	if cfg.Managed {
		routes = auth.ProtectManagedCookies(routes)
	}
	handler, err := (transport.Policy{TrustedHosts: cfg.TrustedHosts, AllowedOrigins: cfg.AllowedOrigins, TrustedProxies: trustedProxies, PublicOrigin: cfg.SiteBaseURL, Development: cfg.Development, LocalHTTPSetup: cfg.LocalHTTPSetup, RequestTimeout: 30 * time.Second}).Wrap(routes)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: cfg.ListenAddress, Handler: handler, ReadHeaderTimeout: cfg.ReadHeaderTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout, IdleTimeout: cfg.IdleTimeout, MaxHeaderBytes: 32 << 10}
	serveErr := make(chan error, 1)
	go func() {
		if e := server.ListenAndServe(); !errors.Is(e, http.ErrServerClosed) {
			serveErr <- e
		} else {
			serveErr <- nil
		}
	}()
	stop, stopCancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopCancel()
	select {
	case err := <-serveErr:
		return err
	case <-stop.Done():
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return <-serveErr
}
