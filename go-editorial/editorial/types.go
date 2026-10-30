package editorial

import "time"

const (
	EditionVersion = "1"
	RussianLocale  = "ru"
	SerbianLocale  = "sr-Latn"
	MaxMainStories = 5
	MaxStoryText   = 2000
	MaxEvidence    = 500
	MaxPlainPart   = 4096
)

// Category is display metadata. ID remains the stable classification key.
type Category struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type Article struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Summary      string `json:"summary,omitempty"`
	Content      string `json:"content,omitempty"`
	SourceName   string `json:"source_name"`
	URL          string `json:"url"`
	CategoryID   string `json:"category_id"`
	CategoryName string `json:"category_name,omitempty"`
}

type Snapshot struct {
	Date        string    `json:"date"`
	CutoffUTC   time.Time `json:"cutoff_utc"`
	SiteBaseURL string    `json:"site_base_url"`
	EditionURL  string    `json:"edition_url,omitempty"`
	Articles    []Article `json:"articles"`
}

type Evidence struct {
	ArticleID string `json:"article_id"`
	Quote     string `json:"quote"`
	Basis     string `json:"basis,omitempty"`
	Locale    string `json:"locale,omitempty"`
}

type Story struct {
	ID               string     `json:"id"`
	Kind             string     `json:"kind"` // main or brief
	Title            string     `json:"title"`
	Summary          string     `json:"summary"`
	PrimaryArticleID string     `json:"primary_article_id"`
	ArticleIDs       []string   `json:"article_ids"`
	Evidence         []Evidence `json:"evidence"`
}

type Exclusion struct {
	ArticleID string `json:"article_id"`
	Reason    string `json:"reason"`
}

type Edition struct {
	Version            string      `json:"version"`
	Locale             string      `json:"locale"`
	Date               string      `json:"date"`
	CutoffUTC          time.Time   `json:"cutoff_utc"`
	SiteBaseURL        string      `json:"site_base_url"`
	EditionURL         string      `json:"edition_url"`
	SourceSnapshotHash string      `json:"source_snapshot_hash"`
	ContentHash        string      `json:"content_hash"`
	Stories            []Story     `json:"stories"`
	Excluded           []Exclusion `json:"excluded,omitempty"`
}

type Rendered struct {
	RichHTML    string   `json:"rich_html"`
	PlainParts  []string `json:"plain_parts"`  // sendMessage-compatible HTML with only inline formatting and links
	PreviewText string   `json:"preview_text"` // unformatted text for local previews
}

type Result struct {
	Edition  Edition  `json:"edition"`
	Rendered Rendered `json:"rendered"`
}

type Request struct {
	Operation string   `json:"operation"`
	Locale    string   `json:"locale"`
	Snapshot  Snapshot `json:"snapshot"`
	Edition   *Edition `json:"edition,omitempty"`
}

type Response struct {
	OK            bool           `json:"ok"`
	Edition       *Edition       `json:"edition,omitempty"`
	Rendered      *Rendered      `json:"rendered,omitempty"`
	ProviderUsage *ProviderUsage `json:"provider_usage,omitempty"`
	ResultHash    string         `json:"result_hash,omitempty"`
	ErrorCode     string         `json:"error_code,omitempty"`
}
