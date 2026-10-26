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
