package repositories

import (
	"context"
	"encoding/json"
	"time"
)

// PublicReader is the read-only query boundary used by public HTTP handlers.
// It deliberately excludes commands such as article reprocessing.
type PublicReader interface {
	Feed(context.Context, FeedQuery) (FeedResponse, error)
	Article(context.Context, int64, bool) (map[string]any, error)
	CategoriesConfig(context.Context) (map[string]any, error)
	Categories(context.Context, StatsQuery) (map[string]any, error)
	Sources(context.Context, StatsQuery) (map[string]any, error)
	Search(context.Context, SearchQuery) (map[string]any, error)
	Digest(context.Context, time.Time) (DigestPage, error)
	APIFeed(context.Context, APIFeedQuery) (APIFeedResponse, error)
	RSS(context.Context, int) ([]RSSArticle, error)
}

type FeedQuery struct {
	Limit      int
	Offset     int
	SinceHours *int
	Category   string
	Source     string
	HideAds    bool
}

type FeedResponse struct {
	Articles []map[string]any `json:"articles"`
	Total    int              `json:"total"`
	Limit    int              `json:"limit"`
	Offset   int              `json:"offset"`
}

type StatsQuery struct {
	SinceHours *int
	HideAds    bool
}

type SearchQuery struct {
	Query      string
	Limit      int
	Offset     int
	Category   string
	SinceHours *int
	Sort       string
	HideAds    bool
}

type DigestPage struct {
	Date       time.Time
	Summaries  []DigestSummary
	Categories []DigestCategory
}
type DigestSummary struct {
	Category      string
	Text          string
	ArticlesCount int
}
type DigestCategory struct {
	Name     string
	Articles []DigestArticle
}
type DigestArticle struct {
	ID         int64
	Title      string
	Summary    *string
	URL        string
	SourceName string
	ImageURL   *string
	FetchedAt  *time.Time
}

// EditorialDigestReader is an optional extension implemented by readers that
// can load immutable editorial editions. Keeping it separate preserves the
// existing public-reader contract for lightweight adapters and legacy tests.
type EditorialDigestReader interface {
	EditorialDigest(context.Context, time.Time, string, string) (*EditorialDigestEdition, error)
}

type EditorialDigestEdition struct {
	ID           string                  `json:"id"`
	Date         string                  `json:"date"`
	Locale       string                  `json:"locale"`
	SnapshotHash string                  `json:"snapshot_hash"`
	Edition      EditorialDigestContent  `json:"edition"`
	Snapshot     EditorialDigestSnapshot `json:"snapshot"`
}

// EditorialDigestStatus exposes only ready-edition metadata to the admin
// summaries screen. The edition JSON and source snapshot remain private to the
// public renderer.
type EditorialDigestStatus struct {
	Date         string `json:"date"`
	Locale       string `json:"locale"`
	Ready        bool   `json:"ready"`
	EditionID    string `json:"edition_id,omitempty"`
	PreviewURL   string `json:"preview_url,omitempty"`
	MainCount    int    `json:"main_count"`
	BriefCount   int    `json:"brief_count"`
	StoryCount   int    `json:"story_count"`
	ArticleCount int    `json:"article_count"`
}

type EditorialDigestContent struct {
	Version            string                   `json:"version"`
	Locale             string                   `json:"locale"`
	Date               string                   `json:"date"`
	CutoffUTC          string                   `json:"cutoff_utc"`
	SiteBaseURL        string                   `json:"site_base_url"`
	EditionURL         string                   `json:"edition_url"`
	SourceSnapshotHash string                   `json:"source_snapshot_hash"`
	ContentHash        string                   `json:"content_hash"`
	Stories            []EditorialDigestStory   `json:"stories"`
	Excluded           []EditorialDigestExclude `json:"excluded"`
}

type EditorialDigestStory struct {
	ID               string                    `json:"id"`
	Kind             string                    `json:"kind"`
	Title            string                    `json:"title"`
	Summary          string                    `json:"summary"`
	PrimaryArticleID string                    `json:"primary_article_id"`
	ArticleIDs       []string                  `json:"article_ids"`
	Evidence         []EditorialDigestEvidence `json:"evidence"`
}

type EditorialDigestEvidence struct {
	ArticleID string `json:"article_id"`
	Quote     string `json:"quote"`
}

type EditorialDigestExclude struct {
	ArticleID string `json:"article_id"`
	Reason    string `json:"reason"`
}

type EditorialDigestSnapshot struct {
	Date        string                   `json:"date"`
	CutoffUTC   string                   `json:"cutoff_utc"`
	SiteBaseURL string                   `json:"site_base_url"`
	EditionURL  string                   `json:"edition_url"`
	Articles    []EditorialDigestArticle `json:"articles"`
}

type EditorialDigestArticle struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Summary      string `json:"summary"`
	Content      string `json:"content"`
	SourceName   string `json:"source_name"`
	URL          string `json:"url"`
	CategoryID   string `json:"category_id"`
	CategoryName string `json:"category_name"`
}

type APIFeedQuery struct {
	Limit, Offset int
	SinceHours    *int
	Category      string
}
type APIFeedResponse struct {
	Version    string            `json:"version"`
	Title      string            `json:"title"`
	UpdatedAt  string            `json:"updated_at"`
	Items      []map[string]any  `json:"items"`
	Pagination APIFeedPagination `json:"pagination"`
}
type APIFeedPagination struct {
	Limit    int     `json:"limit"`
	Offset   int     `json:"offset"`
	Count    int     `json:"count"`
	Category *string `json:"category"`
}

type RSSArticle struct {
	ID          int64
	Title       *string
	URL         string
	Summary     *string
	PublishedAt *time.Time
}

// RawJSON is used when a small legacy object has variable keys but still needs
// deterministic encoding at the HTTP boundary.
type RawJSON = json.RawMessage
