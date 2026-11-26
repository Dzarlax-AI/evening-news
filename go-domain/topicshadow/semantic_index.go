package topicshadow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const semanticModel = "text-embedding-3-small"
const semanticDimensions = 1536

var semanticCollectionName = regexp.MustCompile(`^news_topic_shadow_[a-z0-9_]{1,48}$`)

type SemanticIndexConfig struct {
	QdrantURL, OpenAIURL, Collection, QdrantKey, OpenAIKey string
	HTTPClient                                             *http.Client
	ReserveEmbedding                                       func(context.Context) error
	RecordEmbeddingTokens                                  func(context.Context, int) error
}
type IndexedArticle struct {
	ArticleID              int64
	Claim, Title, Summary  string
	FetchedAt, AvailableAt time.Time
}
type SemanticNeighbor struct {
	VersionID              string
	ArticleID              int64
	Claim                  string
	FetchedAt, AvailableAt time.Time
	Score                  float64
}
type SemanticIndex struct{ cfg SemanticIndexConfig }

func NewSemanticIndex(c SemanticIndexConfig) (*SemanticIndex, error) {
	if !semanticCollectionName.MatchString(c.Collection) || c.OpenAIKey == "" || c.ReserveEmbedding == nil {
		return nil, errors.New("invalid semantic index configuration")
	}
	q, e := url.Parse(c.QdrantURL)
	if e != nil || q.Hostname() == "" || q.User != nil || q.RawQuery != "" || q.Fragment != "" || (q.Scheme != "http" && q.Scheme != "https") {
		return nil, errors.New("invalid private Qdrant URL")
	}
	host := strings.ToLower(q.Hostname())
	ip := net.ParseIP(host)
	if !(host == "localhost" || host == "qdrant" || host == "infra-qdrant" || strings.HasSuffix(host, ".internal") || strings.HasSuffix(host, ".local") || ip != nil && (ip.IsPrivate() || ip.IsLoopback())) {
		return nil, errors.New("Qdrant URL must be private")
	}
	o, e := url.Parse(c.OpenAIURL)
	if e != nil || o.Hostname() == "" || o.User != nil || o.RawQuery != "" || o.Fragment != "" || (o.Scheme != "https" && !(o.Scheme == "http" && net.ParseIP(o.Hostname()) != nil && net.ParseIP(o.Hostname()).IsLoopback())) {
		return nil, errors.New("invalid embedding URL")
	}
	c.QdrantURL = strings.TrimRight(c.QdrantURL, "/")
	c.OpenAIURL = strings.TrimRight(c.OpenAIURL, "/")
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	clientCopy := *c.HTTPClient
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.HTTPClient = &clientCopy
	return &SemanticIndex{cfg: c}, nil
}
func (s *SemanticIndex) call(ctx context.Context, method, endpoint, key string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		b, e := json.Marshal(body)
		if e != nil {
			return errors.New("invalid request")
		}
		reader = bytes.NewReader(b)
	}
	req, e := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if e != nil {
		return errors.New("invalid provider request")
	}
	req.Header.Set("Content-Type", "application/json")
	if strings.HasPrefix(endpoint, s.cfg.OpenAIURL+"/") {
		req.Header.Set("Authorization", "Bearer "+key)
	} else if key != "" {
		req.Header.Set("api-key", key)
	}
	resp, e := s.cfg.HTTPClient.Do(req)
	if e != nil {
		return errors.New("provider request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("provider returned HTTP %d", resp.StatusCode)
	}
	if out != nil {
		if json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(out) != nil {
			return errors.New("invalid provider response")
		}
	}
	return nil
}
func (s *SemanticIndex) collection() string {
	return s.cfg.QdrantURL + "/collections/" + s.cfg.Collection
}
func (s *SemanticIndex) CreateCollection(ctx context.Context) error {
	return s.call(ctx, http.MethodPut, s.collection(), s.cfg.QdrantKey, map[string]any{"vectors": map[string]any{"size": semanticDimensions, "distance": "Cosine"}}, nil)
}
func (s *SemanticIndex) CheckCollection(ctx context.Context) error {
	var r struct {
		Result struct {
			Config struct {
				Params struct {
					Vectors struct {
						Size     int    `json:"size"`
						Distance string `json:"distance"`
					} `json:"vectors"`
				} `json:"params"`
			} `json:"config"`
		} `json:"result"`
	}
	if e := s.call(ctx, http.MethodGet, s.collection(), s.cfg.QdrantKey, nil, &r); e != nil {
		return e
	}
	if r.Result.Config.Params.Vectors.Size != semanticDimensions || !strings.EqualFold(r.Result.Config.Params.Vectors.Distance, "Cosine") {
		return errors.New("incompatible collection")
	}
	return nil
}
func semanticText(a IndexedArticle) string {
	return strings.TrimSpace(a.Title) + "\n\n" + strings.TrimSpace(a.Summary)
}
func validateSemantic(a IndexedArticle) error {
	if a.ArticleID <= 0 || a.Claim == "" || len(a.Claim) > 128 || strings.TrimSpace(a.Title) == "" || strings.TrimSpace(a.Summary) == "" || len(a.Title)+len(a.Summary) > maxArticleTextBytes || a.FetchedAt.IsZero() || a.AvailableAt.IsZero() {
		return errors.New("invalid index article")
	}
	return nil
}
func semanticID(a IndexedArticle) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%s", a.ArticleID, a.Claim, semanticText(a))))
	h[6] = (h[6] & 15) | 80
	h[8] = (h[8] & 63) | 128
	x := hex.EncodeToString(h[:16])
	return x[:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:]
}

// PointID identifies the exact article claim and indexed text version.
func (a IndexedArticle) PointID() string { return semanticID(a) }
func (s *SemanticIndex) embed(ctx context.Context, a IndexedArticle) ([]float64, error) {
	if e := s.cfg.ReserveEmbedding(ctx); e != nil {
		return nil, e
	}
	var r struct {
		Data []struct {
			Embedding []float64 `json:"embedding"`
		} `json:"data"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	e := s.call(ctx, http.MethodPost, s.cfg.OpenAIURL+"/embeddings", s.cfg.OpenAIKey, map[string]any{"model": semanticModel, "input": semanticText(a), "dimensions": semanticDimensions}, &r)
	if e != nil {
		return nil, e
	}
	if len(r.Data) != 1 || len(r.Data[0].Embedding) != semanticDimensions {
		return nil, errors.New("invalid embedding dimensions")
	}
	if s.cfg.RecordEmbeddingTokens != nil && r.Usage.TotalTokens > 0 {
		if err := s.cfg.RecordEmbeddingTokens(ctx, r.Usage.TotalTokens); err != nil {
			return nil, err
		}
	}
	return r.Data[0].Embedding, nil
}

// Index uses immutable version-specific IDs. The caller must filter candidates
// against authoritative current News versions and remove obsolete points later.
func (s *SemanticIndex) Index(ctx context.Context, a IndexedArticle) (bool, error) {
	if e := validateSemantic(a); e != nil {
		return false, e
	}
	id := semanticID(a)
	var found struct {
		Result []struct {
			Payload struct {
				ArticleID   int64  `json:"article_id"`
				Claim       string `json:"claim"`
				Model       string `json:"model"`
				Dimensions  int    `json:"dimensions"`
				FetchedAt   string `json:"fetched_at"`
				AvailableAt string `json:"available_at"`
				TextHash    string `json:"text_sha256"`
			} `json:"payload"`
		} `json:"result"`
	}
	e := s.call(ctx, http.MethodPost, s.collection()+"/points", s.cfg.QdrantKey, map[string]any{"ids": []string{id}, "with_payload": true}, &found)
	if e != nil {
		return false, e
	}
	h := sha256.Sum256([]byte(semanticText(a)))
	if len(found.Result) == 1 {
		p := found.Result[0].Payload
		if p.ArticleID == a.ArticleID && p.Claim == a.Claim && p.Model == semanticModel && p.Dimensions == semanticDimensions && p.TextHash == hex.EncodeToString(h[:]) && p.FetchedAt == a.FetchedAt.UTC().Format(time.RFC3339Nano) && p.AvailableAt == a.AvailableAt.UTC().Format(time.RFC3339Nano) {
			return false, nil
		}
	}
	v, e := s.embed(ctx, a)
	if e != nil {
		return true, e
	}
	p := map[string]any{"article_id": a.ArticleID, "claim": a.Claim, "fetched_at": a.FetchedAt.UTC().Format(time.RFC3339Nano), "available_at": a.AvailableAt.UTC().Format(time.RFC3339Nano), "text_sha256": hex.EncodeToString(h[:]), "model": semanticModel, "dimensions": semanticDimensions}
	e = s.call(ctx, http.MethodPut, s.collection()+"/points?wait=true", s.cfg.QdrantKey, map[string]any{"points": []any{map[string]any{"id": id, "vector": v, "payload": p}}}, nil)
	return true, e
}
func (s *SemanticIndex) Neighbors(ctx context.Context, a IndexedArticle, limit int) ([]SemanticNeighbor, error) {
	if e := validateSemantic(a); e != nil {
		return nil, e
	}
	if limit < 1 || limit > 32 {
		return nil, errors.New("invalid neighbor limit")
	}
	var r struct {
		Result struct {
			Points []struct {
				ID      string  `json:"id"`
				Score   float64 `json:"score"`
				Payload struct {
					ArticleID   int64     `json:"article_id"`
					Claim       string    `json:"claim"`
					FetchedAt   time.Time `json:"fetched_at"`
					AvailableAt time.Time `json:"available_at"`
				} `json:"payload"`
			} `json:"points"`
		} `json:"result"`
	}
	filter := map[string]any{"must": []any{map[string]any{"key": "fetched_at", "range": map[string]any{"gte": a.FetchedAt.Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339Nano), "lte": a.FetchedAt.UTC().Format(time.RFC3339Nano)}}, map[string]any{"key": "available_at", "range": map[string]any{"lte": a.AvailableAt.UTC().Format(time.RFC3339Nano)}}}}
	e := s.call(ctx, http.MethodPost, s.collection()+"/points/query", s.cfg.QdrantKey, map[string]any{"query": a.PointID(), "limit": limit, "with_payload": true, "filter": filter}, &r)
	if e != nil {
		return nil, e
	}
	if len(r.Result.Points) > limit {
		return nil, errors.New("invalid neighbor response")
	}
	out := make([]SemanticNeighbor, 0, len(r.Result.Points))
	for _, p := range r.Result.Points {
		if p.ID == "" || p.Payload.ArticleID <= 0 || p.Payload.Claim == "" || p.Payload.FetchedAt.IsZero() || p.Payload.AvailableAt.IsZero() {
			return nil, errors.New("invalid neighbor response")
		}
		if p.Payload.ArticleID == a.ArticleID || (p.Payload.FetchedAt.Equal(a.FetchedAt) && p.Payload.ArticleID >= a.ArticleID) {
			continue
		}
		out = append(out, SemanticNeighbor{p.ID, p.Payload.ArticleID, p.Payload.Claim, p.Payload.FetchedAt, p.Payload.AvailableAt, p.Score})
	}
	return out, nil
}

// reconciliationPage reads only identity payload from the dedicated pilot collection.
func (s *SemanticIndex) reconciliationPage(ctx context.Context, offset string, limit int) ([]SemanticNeighbor, string, error) {
	var result struct {
		Result struct {
			Points []struct {
				ID      string `json:"id"`
				Payload struct {
					ArticleID int64  `json:"article_id"`
					Claim     string `json:"claim"`
				} `json:"payload"`
			} `json:"points"`
			Next string `json:"next_page_offset"`
		} `json:"result"`
	}
	body := map[string]any{"limit": limit, "with_payload": []string{"article_id", "claim"}, "with_vector": false}
	if offset != "" {
		body["offset"] = offset
	}
	if err := s.call(ctx, http.MethodPost, s.collection()+"/points/scroll", s.cfg.QdrantKey, body, &result); err != nil {
		return nil, "", err
	}
	if len(result.Result.Points) > limit || (offset != "" && offset == result.Result.Next) {
		return nil, "", errors.New("invalid reconciliation page")
	}
	points := make([]SemanticNeighbor, 0, len(result.Result.Points))
	for _, point := range result.Result.Points {
		if point.ID == "" || point.Payload.ArticleID <= 0 || point.Payload.Claim == "" {
			return nil, "", errors.New("invalid indexed point")
		}
		points = append(points, SemanticNeighbor{VersionID: point.ID, ArticleID: point.Payload.ArticleID, Claim: point.Payload.Claim})
	}
	return points, result.Result.Next, nil
}

func (s *SemanticIndex) removePoints(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	return s.call(ctx, http.MethodPost, s.collection()+"/points/delete?wait=true", s.cfg.QdrantKey,
		map[string]any{"points": ids}, nil)
}
