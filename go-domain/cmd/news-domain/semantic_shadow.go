package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/topicshadow"
	"github.com/jackc/pgx/v5/pgxpool"
)

type semanticShadowSettings struct {
	url, collection, qdrantKeyFile string
	embeddingDailyLimit            int
}

func readSemanticShadowSettings(get func(string) string) (semanticShadowSettings, error) {
	s := semanticShadowSettings{
		url:           strings.TrimSpace(get("NEWS_TOPIC_SHADOW_QDRANT_URL")),
		collection:    strings.TrimSpace(get("NEWS_TOPIC_SHADOW_QDRANT_COLLECTION")),
		qdrantKeyFile: strings.TrimSpace(get("NEWS_TOPIC_SHADOW_QDRANT_KEY_FILE")),
	}
	limit, err := strconv.Atoi(strings.TrimSpace(get("NEWS_TOPIC_SHADOW_EMBEDDING_DAILY_LIMIT")))
	if err != nil || limit < 1 || s.url == "" || s.collection == "" ||
		s.qdrantKeyFile != "" && !filepath.IsAbs(s.qdrantKeyFile) {
		return semanticShadowSettings{}, errors.New("invalid semantic shadow configuration")
	}
	s.embeddingDailyLimit = limit
	return s, nil
}

func readSemanticSecret(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("semantic credential unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 8192 {
		return "", errors.New("invalid semantic credential file")
	}
	buf, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil || len(buf) > 8192 {
		return "", errors.New("invalid semantic credential file")
	}
	defer clear(buf)
	key := strings.TrimSpace(string(buf))
	if key == "" || strings.ContainsAny(key, "\r\n") {
		return "", errors.New("invalid semantic credential")
	}
	return key, nil
}

func (s semanticShadowSettings) run(ctx context.Context, runner topicshadow.Runner, openAIKey, jevKey string) error {
	var err error
	qdrantKey := ""
	if s.qdrantKeyFile != "" {
		qdrantKey, err = readSemanticSecret(s.qdrantKeyFile)
		if err != nil {
			return err
		}
	}
	runner.Version = topicshadow.SemanticPolicyVersion
	checkpoint := topicshadow.IndexCheckpoint{Pool: runner.Pool, Policy: runner.Version, Collection: s.collection,
		Owner: runner.Owner, EmbeddingLimit: s.embeddingDailyLimit}
	index, err := topicshadow.NewSemanticIndex(topicshadow.SemanticIndexConfig{QdrantURL: s.url,
		OpenAIURL: "https://api.openai.com/v1", Collection: s.collection, OpenAIKey: openAIKey, QdrantKey: qdrantKey,
		ReserveEmbedding: checkpoint.ReserveEmbedding, RecordEmbeddingTokens: checkpoint.RecordEmbeddingTokens})
	if err != nil {
		return err
	}
	progress, err := checkpoint.Advance(ctx, index, topicShadowBatch)
	if err != nil || !progress.Complete {
		return err
	}
	runner.Client = topicshadow.NewJevClient(jevKey, nil)
	runner.Semantic = index
	runner.WarmupFrom = progress.StartedAt.AddDate(0, 0, -30)
	report, err := runner.RunPass(ctx, time.Now().UTC(), topicShadowBatch)
	if err != nil {
		return err
	}
	log.Printf("semantic topic shadow pass: processed=%d reviewed=%d deferred=%t scanned=%t", report.Processed, report.Reviewed, report.Deferred, report.Scanned)
	return checkpoint.Reconcile(ctx, index, 100)
}

func semanticShadowError(err error) string {
	if errors.Is(err, topicshadow.ErrEmbeddingBudget) {
		return "embedding_daily_limit"
	}
	return topicshadow.PassErrorCode(err)
}

// initializeSemanticCollection is an explicit operator action. Normal service
// startup only checks an existing pilot collection and never recreates it.
func initializeSemanticCollection() error {
	source, err := loadSource()
	if err != nil {
		return err
	}
	settings, err := readSemanticShadowSettings(source.get)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	key := source.get("OPENAI_API_KEY")
	if source.managed {
		pool, err := pgxpool.New(ctx, source.get("DATABASE_URL"))
		if err != nil {
			return err
		}
		defer pool.Close()
		store := credentialstore.Store{Pool: pool, Key: source.key, KeyID: source.keyID, InstallationID: source.installation}
		raw, meta, err := store.Resolve(ctx, "openai")
		if err != nil || !meta.Configured || len(raw) == 0 {
			return errors.New("embedding credential unavailable")
		}
		key = string(raw)
		clear(raw)
	}
	qdrantKey := ""
	if settings.qdrantKeyFile != "" {
		qdrantKey, err = readSemanticSecret(settings.qdrantKeyFile)
		if err != nil {
			return err
		}
	}
	index, err := topicshadow.NewSemanticIndex(topicshadow.SemanticIndexConfig{QdrantURL: settings.url,
		OpenAIURL: "https://api.openai.com/v1", Collection: settings.collection, QdrantKey: qdrantKey, OpenAIKey: key,
		ReserveEmbedding: func(context.Context) error { return errors.New("embedding disabled during collection initialization") }})
	if err != nil {
		return err
	}
	if err = index.CreateCollection(ctx); err != nil {
		return err
	}
	return index.CheckCollection(ctx)
}
