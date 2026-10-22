// Package extractor replays captured extraction inputs without network or database access.
package extractor

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

type Input struct {
	URL             string              `json:"url"`
	Title           string              `json:"title"`
	Content         string              `json:"content"`
	HTML            string              `json:"html"`
	RenderedHTML    string              `json:"rendered_html"`
	RSSXML          string              `json:"rss_xml"`
	TelegramHTML    string              `json:"telegram_html"`
	RedditJSON      string              `json:"reddit_json"`
	LinkedHTML      string              `json:"linked_html"`
	LearnedSelector string              `json:"learned_selector"`
	LearnedMethod   string              `json:"learned_method"`
	Strategy        string              `json:"strategy"`
	SourceType      string              `json:"source_type"`
	SourceName      string              `json:"source_name"`
	MaxBytes        int                 `json:"max_bytes"`
	MaxChars        int                 `json:"max_chars"`
	EntryIndex      int                 `json:"entry_index"`
	Selectors       map[string][]string `json:"selectors"`
	AlternativeHTML map[string]string   `json:"alternative_html"`
	Limits          *Limits             `json:"limits,omitempty"`
	CancelAt        string              `json:"cancel_at,omitempty"`
	UseBrowser      *bool               `json:"use_browser,omitempty"`
	Now             string              `json:"now,omitempty"`
}

type Limits struct {
	MaxBytes int `json:"max_bytes"`
	MaxChars int `json:"max_chars"`
}

// Article metadata byte limits bound JSON expansion at the extractor service
// boundary while preserving short publisher metadata unchanged.
const (
	maxArticleTitleBytes           = 1024
	maxArticleAuthorBytes          = 1024
	maxArticleDescriptionBytes     = 4096
	maxArticlePublicationDateBytes = 128
	maxArticleImageURLBytes        = 8 * 1024
)

type Media struct {
	Type         string         `json:"type"`
	URL          string         `json:"url"`
	Title        string         `json:"title,omitempty"`
	Thumbnail    string         `json:"thumbnail,omitempty"`
	Data         map[string]any `json:"data,omitempty"`
	Source       string         `json:"source,omitempty"`
	MediaType    string         `json:"media_type,omitempty"`
	AltText      string         `json:"alt_text,omitempty"`
	Duration     string         `json:"duration,omitempty"`
	PollData     map[string]any `json:"poll_data,omitempty"`
	LocationData map[string]any `json:"location_data,omitempty"`
	ContactData  map[string]any `json:"contact_data,omitempty"`
}

func (m Media) MarshalJSON() ([]byte, error) {
	type plain Media
	b, e := json.Marshal(plain(m))
	if e != nil {
		return nil, e
	}
	if m.Type != "video" || m.Thumbnail != "" {
		return b, nil
	}
	var v map[string]any
	if e = json.Unmarshal(b, &v); e != nil {
		return nil, e
	}
	v["thumbnail"] = nil
	return json.Marshal(v)
}

func (r Result) MarshalJSON() ([]byte, error) {
	type plain Result
	b, e := json.Marshal(plain(r))
	if e != nil {
		return nil, e
	}
	var m map[string]any
	if e = json.Unmarshal(b, &m); e != nil {
		return nil, e
	}
	for _, k := range []string{"title", "publication_date", "author", "description", "image_url"} {
		if m[k] == "" {
			m[k] = nil
		}
	}
	return json.Marshal(m)
}

type Result struct {
	URL              string   `json:"url"`
	ExtractedFrom    string   `json:"extracted_from,omitempty"`
	Content          string   `json:"content"`
	Title            string   `json:"title"`
	PublicationDate  string   `json:"publication_date"`
	Author           string   `json:"author"`
	Description      string   `json:"description"`
	ImageURL         string   `json:"image_url"`
	Media            []Media  `json:"media"`
	Method           string   `json:"method"`
	MethodUsed       string   `json:"method_used"`
	SelectorUsed     string   `json:"selector_used"`
	State            string   `json:"state"`
	Error            string   `json:"error,omitempty"`
	Trace            []string `json:"trace"`
	Quality          float64  `json:"quality"`
	OriginalChars    int      `json:"original_chars"`
	Truncated        bool     `json:"truncated"`
	TruncationPolicy string   `json:"truncation_policy,omitempty"`
}

// Extract returns a terminal result for every input, including rejected and unsupported inputs.
func Extract(ctx context.Context, in Input) (r Result) {
	r = Result{URL: in.URL, Title: in.Title, State: "failed", Trace: []string{}, Media: []Media{}}
	defer func() {
		if in.SourceType == "article" || in.SourceType == "web" || in.SourceType == "" {
			if boundArticleMetadata(&r) {
				r.Trace = append(r.Trace, "metadata_truncated")
			}
		}
	}()
	if in.Limits != nil {
		in.MaxBytes = in.Limits.MaxBytes
		in.MaxChars = in.Limits.MaxChars
	}
	if in.MaxBytes == 0 {
		in.MaxBytes = 5 * 1024 * 1024
	}
	if in.MaxChars == 0 {
		in.MaxChars = 100000
	}
	if in.MaxBytes < 1 || in.MaxChars < 1 {
		r.Error = "invalid_limits"
		return r
	}
	if err := ctx.Err(); err != nil {
		r.Error = err.Error()
		return r
	}
	if in.Now == "" {
		in.Now = time.Now().UTC().Format(time.RFC3339Nano)
	} else if _, err := time.Parse(time.RFC3339Nano, in.Now); err != nil {
		r.Error = "invalid_now"
		return r
	}
	if in.CancelAt != "" {
		// This is an injected replay cancellation boundary. No live browser tab exists.
		if in.CancelAt == "browser_html_capture" && (in.Strategy == "browser_rendering" || in.Strategy == "telegram_browser" || in.Strategy == "custom_browser") {
			r.Error = "cancelled"
			r.Trace = append(r.Trace, "cancelled")
		} else {
			r.State = "unsupported"
			r.Error = "unsupported_cancellation_boundary"
		}
		return r
	}
	known := map[string]bool{"": true, "auto": true, "pipeline": true, "source_content": true, "rss": true, "atom": true, "rss_feed": true, "atom_feed": true, "telegram": true, "telegram_http": true, "telegram_browser": true, "custom": true, "custom_http": true, "custom_browser": true, "custom_selectors": true, "ai_selectors": true, "learned_pattern": true, "learned_pattern_browser_rendering": true, "enhanced_selectors": true, "readability": true, "browser_rendering": true, "browser_heuristics": true, "json_ld": true, "open_graph": true, "meta_description": true, "enhanced_heuristics": true, "fallback_extraction": true, "reddit_json": true, "reddit_linked_article": true}
	if !known[in.Strategy] {
		r.State = "unsupported"
		r.Error = "unsupported_strategy: " + in.Strategy
		return r
	}
	for _, s := range []string{in.HTML, in.RenderedHTML, in.RSSXML, in.TelegramHTML, in.RedditJSON, in.LinkedHTML, in.Content} {
		if len(s) > in.MaxBytes {
			r.State = "resource_limit"
			r.Error = "extraction_byte_limit"
			return r
		}
	}
	for _, s := range in.AlternativeHTML {
		if len(s) > in.MaxBytes {
			r.State = "resource_limit"
			r.Error = "extraction_byte_limit"
			return r
		}
	}
	if !validURL(in.URL) {
		r.State = "failed"
		r.Error = "invalid_url"
		return r
	}
	in.URL = cleanURL(in.URL)
	if r.URL != in.URL {
		r.ExtractedFrom = in.URL
	}
	if in.Strategy == "source_content" {
		r.Content = in.Content
		r.Method = "source_content"
		r.Trace = append(r.Trace, "source_content")
	} else {
		switch in.Strategy {
		case "rss", "atom", "rss_feed", "atom_feed":
			r = extractFeed(in, r)
		case "telegram", "telegram_http", "telegram_browser":
			r = extractTelegram(ctx, in, r)
		case "custom", "custom_http", "custom_browser", "custom_selectors", "ai_selectors":
			r = extractCustom(in, r)
		default:
			if in.SourceType == "custom" && in.Strategy == "browser_rendering" {
				r = extractCustom(in, r)
				break
			}
			if in.Strategy == "" || in.Strategy == "auto" {
				switch in.SourceType {
				case "rss", "atom":
					r = extractFeed(in, r)
				case "telegram":
					r = extractTelegram(ctx, in, r)
				case "custom":
					r = extractCustom(in, r)
				default:
					r = extractWeb(ctx, in, r)
				}
			} else {
				r = extractWeb(ctx, in, r)
			}
		}
	}
	if err := ctx.Err(); err != nil {
		r.Content = ""
		r.State = "failed"
		r.Error = err.Error()
		return r
	}
	if r.Content == "" && in.Content != "" && (in.Strategy == "" || in.Strategy == "auto" || in.Strategy == "pipeline") {
		r.Content = in.Content
		r.Method = "source_content"
		r.Trace = append(r.Trace, "source_content_fallback")
	}
	if runeLen(r.Content) > in.MaxChars {
		r.Content = ""
		r.State = "resource_limit"
		r.Error = "extraction_text_limit"
		return r
	}
	if r.Content != "" || len(r.Media) > 0 {
		if r.Content != "" && len(r.Media) == 0 && (in.SourceType == "article" || in.SourceType == "web" || in.SourceType == "") {
			h := in.HTML
			if strings.Contains(r.Method, "browser") {
				h = in.RenderedHTML
			}
			var mediaTruncated bool
			r.Media, mediaTruncated = boundedArticleMedia(h, in.URL)
			if mediaTruncated {
				r.Trace = append(r.Trace, "media_truncated")
			}
		}
		r.State = "extracted"
		r.Error = ""
		r.MethodUsed = r.Method
		r.Quality = qualityScore(r.Content)
		r.OriginalChars = runeLen(r.Content)
		if (in.SourceType == "article" || in.SourceType == "" || in.SourceType == "web") && r.OriginalChars > 8000 {
			r.Content = smartTruncate(r.Content, 8000)
			r.Truncated = true
			r.TruncationPolicy = "sentence_or_word_8000"
			r.Trace = append(r.Trace, "finalize_truncated")
		}
	}
	if err := ctx.Err(); err != nil {
		r.Content = ""
		r.State = "failed"
		r.Error = err.Error()
	}
	return r
}

// boundArticleMetadata caps article metadata for both successful and failed
// outcomes without changing the caller's article identity. Whole URLs are
// skipped rather than rewritten when they exceed the output contract.
func boundArticleMetadata(r *Result) bool {
	changed := false
	for _, field := range []struct {
		value *string
		limit int
	}{
		{&r.Title, maxArticleTitleBytes},
		{&r.Author, maxArticleAuthorBytes},
		{&r.Description, maxArticleDescriptionBytes},
		{&r.PublicationDate, maxArticlePublicationDateBytes},
	} {
		if len(*field.value) > field.limit {
			*field.value = truncateUTF8(*field.value, field.limit)
			changed = true
		}
	}
	if len(r.ImageURL) > maxArticleImageURLBytes {
		r.ImageURL = ""
		changed = true
	}
	return changed
}
