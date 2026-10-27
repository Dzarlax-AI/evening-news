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
	authService := auth.New(auth.Config{Username: cfg.AdminUsername, Password: cfg.AdminPassword, JWTSecret: cfg.JWTSecret, Development: cfg.Development, TrustForwardAuth: cfg.TrustForwardAuth})
	started := time.Now().UTC()
	ui := httpui.New(httpui.Config{Reader: db, PublicAdmin: func(r *http.Request) bool { _, ok := authService.PublicAdmin(r); return ok }, TemplatesDir: "templates", StaticDir: "../web/static", StartedAt: started, Version: "2.0.0"})
	mux := http.NewServeMux()
	if err := httpui.RegisterDocs(mux, "contracts/openapi.json"); err != nil {
		return err
	}
	ui.Register(mux)
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
	schedules.NewHandlerFromEnv(repositories.NewScheduleJobsSnapshot(db.Pool())).Register(mux, authService)
	trustedProxies := make([]*net.IPNet, 0, len(cfg.TrustedProxyCIDRs))
	for _, cidr := range cfg.TrustedProxyCIDRs {
		_, block, e := net.ParseCIDR(cidr)
		if e != nil {
			return errors.New("invalid trusted proxy configuration")
		}
		trustedProxies = append(trustedProxies, block)
	}
	handler, err := (transport.Policy{TrustedHosts: cfg.TrustedHosts, AllowedOrigins: cfg.AllowedOrigins, TrustedProxies: trustedProxies, PublicOrigin: cfg.SiteBaseURL, Development: cfg.Development, RequestTimeout: 30 * time.Second}).Wrap(mux)
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
