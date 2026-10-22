// Package service defines the private extraction wire contract and HTTP lifecycle.
package service

import (
	"context"
	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"time"
)

const ContractVersion = "1"
const MaxBytes = 5 * 1024 * 1024
const MaxJSONBytes = 8 * 1024 * 1024
const MaxTelegramJSONBytes = 5 * 1024 * 1024

// Request deliberately excludes replay injection fields and server configuration.
type Request struct {
	RequestID       string              `json:"request_id"`
	URL             string              `json:"url"`
	Content         string              `json:"content,omitempty"`
	HTML            string              `json:"html,omitempty"`
	Title           string              `json:"title,omitempty"`
	SourceType      string              `json:"source_type,omitempty"`
	Strategy        string              `json:"strategy,omitempty"`
	Selectors       map[string][]string `json:"selectors,omitempty"`
	LearnedSelector string              `json:"learned_selector,omitempty"`
	LearnedMethod   string              `json:"learned_method,omitempty"`
	UseBrowser      *bool               `json:"use_browser,omitempty"`
	BudgetMS        int64               `json:"budget_ms,omitempty"`
	MaxBytes        int                 `json:"max_bytes,omitempty"`
	MaxChars        int                 `json:"max_chars,omitempty"`
}

type Observation struct {
	Selector string `json:"selector"`
	Method   string `json:"method"`
	Success  bool   `json:"success"`
	Reason   string `json:"reason,omitempty"`
}
type Failure struct {
	Code              string `json:"code"`
	Retryable         bool   `json:"retryable"`
	UpstreamStatus    int    `json:"upstream_status,omitempty"`
	RetryAfterSeconds *int   `json:"retry_after_seconds,omitempty"`
	Reason            string `json:"reason,omitempty"`
}
type Response struct {
	ContractVersion    string           `json:"contract_version"`
	BuildVersion       string           `json:"build_version"`
	RequestID          string           `json:"request_id"`
	Outcome            string           `json:"outcome"`
	OriginalURL        string           `json:"original_url"`
	FinalURL           string           `json:"final_url"`
	Result             extractor.Result `json:"result"`
	Observations       []Observation    `json:"observations"`
	NeedsSelectorStudy bool             `json:"needs_selector_study"`
	StudyHTML          string           `json:"study_html,omitempty"`
	Error              *Failure         `json:"error,omitempty"`
}

type Runner interface {
	Run(context.Context, Request) Response
}
type RunnerFunc func(context.Context, Request) Response

func (f RunnerFunc) Run(ctx context.Context, r Request) Response { return f(ctx, r) }

// TelegramRequest is deliberately narrower than Request: callers cannot inject
// page HTML, selectors, or browser/network configuration.
type TelegramRequest struct {
	ContractVersion string `json:"contract_version"`
	RequestID       string `json:"request_id"`
	ChannelURL      string `json:"channel_url"`
	SourceName      string `json:"source_name"`
	Limit           int    `json:"limit"`
	BudgetMS        int64  `json:"budget_ms"`
}

type TelegramResponse struct {
	ContractVersion string                      `json:"contract_version"`
	BuildVersion    string                      `json:"build_version"`
	RequestID       string                      `json:"request_id"`
	OriginalURL     string                      `json:"original_url"`
	FinalURL        string                      `json:"final_url"`
	Outcome         string                      `json:"outcome"`
	FetchMethod     string                      `json:"fetch_method,omitempty"`
	Articles        []extractor.TelegramArticle `json:"articles"`
	Error           *Failure                    `json:"error,omitempty"`
}

type TelegramRunner interface {
	RunTelegram(context.Context, TelegramRequest) TelegramResponse
}

type PageMonitorRequest struct {
	ContractVersion           string                                 `json:"contract_version"`
	RequestID                 string                                 `json:"request_id"`
	Operation                 string                                 `json:"operation"`
	URL                       string                                 `json:"url"`
	SourceName                string                                 `json:"source_name"`
	BudgetMS                  int64                                  `json:"budget_ms"`
	Limit                     int                                    `json:"limit,omitempty"`
	Offset                    int                                    `json:"offset,omitempty"`
	MinTitleLength            int                                    `json:"min_title_length,omitempty"`
	UseBrowser                *bool                                  `json:"use_browser,omitempty"`
	WaitForJS                 *bool                                  `json:"wait_for_js,omitempty"`
	ProvidedHTML              *string                                `json:"provided_html,omitempty"`
	SnapshotAt                *time.Time                             `json:"snapshot_at,omitempty"`
	ArticleSelectors          []string                               `json:"article_selectors,omitempty"`
	TitleSelectors            []string                               `json:"title_selectors,omitempty"`
	DateSelectors             []string                               `json:"date_selectors,omitempty"`
	LinkSelectors             []string                               `json:"link_selectors,omitempty"`
	LearnedContainerSelectors []string                               `json:"learned_container_selectors,omitempty"`
	LearnedTitleSelectors     []string                               `json:"learned_title_selectors,omitempty"`
	LearnedLinkSelectors      []string                               `json:"learned_link_selectors,omitempty"`
	LearnedDateSelectors      []string                               `json:"learned_date_selectors,omitempty"`
	LearnedGeneralSelectors   []extractor.PageMonitorGeneralSelector `json:"learned_general_selectors,omitempty"`
}

type PageMonitorResponse struct {
	ContractVersion    string                      `json:"contract_version"`
	BuildVersion       string                      `json:"build_version"`
	RequestID          string                      `json:"request_id"`
	Outcome            string                      `json:"outcome"`
	OriginalURL        string                      `json:"original_url"`
	FinalURL           string                      `json:"final_url"`
	FetchMethod        string                      `json:"fetch_method,omitempty"`
	SnapshotAt         *time.Time                  `json:"snapshot_at,omitempty"`
	ContentHash        string                      `json:"content_hash,omitempty"`
	SelectorsUsed      []string                    `json:"selectors_used"`
	Items              []extractor.PageMonitorItem `json:"items"`
	NextOffset         *int                        `json:"next_offset,omitempty"`
	NeedsSelectorStudy bool                        `json:"needs_selector_study"`
	StudyHTML          string                      `json:"study_html,omitempty"`
	StudyHTMLOmitted   bool                        `json:"study_html_omitted,omitempty"`
	Error              *Failure                    `json:"error,omitempty"`
}

type PageMonitorRunner interface {
	RunPageMonitor(context.Context, PageMonitorRequest) PageMonitorResponse
}
