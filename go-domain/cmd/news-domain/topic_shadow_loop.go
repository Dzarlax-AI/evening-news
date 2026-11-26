package main

import (
	"context"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/topicshadow"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	topicShadowInterval = 15 * time.Minute
	topicShadowTimeout  = 12 * time.Minute
	topicShadowBatch    = 50
)

type topicShadowSettings struct {
	enabled           bool
	dailyRequestLimit int
	reason            string
	semantic          *semanticShadowSettings
}

func topicShadowSettingsFrom(get func(string) string) topicShadowSettings {
	rawEnabled := strings.TrimSpace(get("NEWS_TOPIC_SHADOW_ENABLED"))
	if rawEnabled == "" {
		return topicShadowSettings{}
	}
	enabled, err := strconv.ParseBool(rawEnabled)
	if err != nil {
		return topicShadowSettings{reason: "invalid enable setting"}
	}
	if !enabled {
		return topicShadowSettings{}
	}
	limitText := strings.TrimSpace(get("NEWS_TOPIC_SHADOW_DAILY_REQUEST_LIMIT"))
	limit, err := strconv.Atoi(limitText)
	if err != nil || limit < 2 {
		return topicShadowSettings{reason: "enabled without a valid daily request limit"}
	}
	settings := topicShadowSettings{enabled: true, dailyRequestLimit: limit}
	switch strings.TrimSpace(get("NEWS_TOPIC_SHADOW_MODE")) {
	case "", "luna":
	case "qdrant-jev":
		semantic, err := readSemanticShadowSettings(get)
		if err != nil {
			return topicShadowSettings{reason: "invalid Qdrant/Jev setting"}
		}
		settings.semantic = &semantic
	default:
		return topicShadowSettings{reason: "unknown topic shadow mode"}
	}
	return settings
}

// topicShadowReport emits a private operator report for the domain database.
// With no cutoff it exports the latest saved snapshot; otherwise the cutoff
// must be an exact RFC3339 timestamp.
func topicShadowReport(args []string, out io.Writer) error {
	if len(args) > 1 {
		return errors.New("invalid report arguments")
	}
	cutoff := time.Time{}
	if len(args) == 1 {
		parsed, err := time.Parse(time.RFC3339, args[0])
		if err != nil {
			return errors.New("invalid report cutoff")
		}
		cutoff = parsed
	}
	source, err := loadSource()
	if err != nil {
		return errors.New("invalid runtime manifest")
	}
	databaseURL := strings.TrimSpace(source.get("DATABASE_URL"))
	if databaseURL == "" {
		return errors.New("database unavailable")
	}
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return errors.New("invalid database configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return errors.New("database unavailable")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return errors.New("database unavailable")
	}
	return topicshadow.WriteReport(ctx, pool, topicshadow.DefaultPolicyVersion, cutoff, out)
}

// runTopicShadowLoop is intentionally isolated from the request server and
// other domain jobs. All errors are reduced to safe operational messages.
func runTopicShadowLoop(ctx context.Context, pool *pgxpool.Pool, owner string, credentials *credentialstore.Store, standaloneKey string, guard *authority.Guard, settings topicShadowSettings) {
	if !settings.enabled {
		if settings.reason != "" {
			log.Print("topic shadow unavailable: ", settings.reason)
		}
		return
	}
	ticker := time.NewTicker(topicShadowInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if guard == nil || guard.Check(ctx) != nil {
				continue
			}
			apiKey := strings.TrimSpace(standaloneKey)
			if credentials != nil {
				resolveCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				raw, meta, err := credentials.Resolve(resolveCtx, "openai")
				cancel()
				if err != nil || !meta.Configured || len(raw) == 0 {
					log.Print("topic shadow unavailable: OpenAI credential not configured")
					continue
				}
				apiKey = string(raw)
				for i := range raw {
					raw[i] = 0
				}
			}
			if apiKey == "" {
				log.Print("topic shadow unavailable: OpenAI credential not configured")
				continue
			}
			runner := topicshadow.Runner{
				Pool: pool, Version: topicshadow.DefaultPolicyVersion, Owner: owner,
				Client: topicshadow.NewDecisionsClient(apiKey, nil), DailyRequestLimit: settings.dailyRequestLimit,
			}
			passCtx, cancel := context.WithTimeout(ctx, topicShadowTimeout)
			if settings.semantic != nil {
				if credentials == nil {
					log.Print("semantic topic shadow unavailable: managed credentials required")
					cancel()
					continue
				}
				resolveCtx, stop := context.WithTimeout(passCtx, 3*time.Second)
				raw, meta, err := credentials.Resolve(resolveCtx, "jev")
				stop()
				if err != nil || !meta.Configured || len(raw) == 0 {
					clear(raw)
					log.Print("semantic topic shadow unavailable: Jev credential not configured")
					cancel()
					continue
				}
				jevKey := string(raw)
				clear(raw)
				if err := settings.semantic.run(passCtx, runner, apiKey, jevKey); err != nil {
					log.Printf("semantic topic shadow pass paused: reason=%s", semanticShadowError(err))
				}
				cancel()
				continue
			}
			report, err := runner.RunPass(passCtx, time.Now().UTC(), topicShadowBatch)
			cancel()
			if err != nil && ctx.Err() == nil {
				log.Printf("topic shadow pass unavailable: reason=%s", topicshadow.PassErrorCode(err))
			} else if report.Deferred && ctx.Err() == nil {
				log.Printf("topic shadow pass deferred: reason=%s processed=%d reviewed=%d scanned=%t",
					report.DeferredReason, report.Processed, report.Reviewed, report.Scanned)
			}
		}
	}
}
