package httpui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
)

type publicDigestIssue struct {
	Date      string `json:"date"`
	EditionID string `json:"edition_id"`
}
type publicDigestCatalog struct {
	Version      string              `json:"version"`
	Locale       string              `json:"locale"`
	Timezone     string              `json:"timezone"`
	Month        string              `json:"month"`
	EarliestDate *string             `json:"earliest_date"`
	Latest       *publicDigestIssue  `json:"latest"`
	Issues       []publicDigestIssue `json:"issues"`
}
type publicDigestCategory struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	StoryCount int    `json:"story_count"`
}
type publicDigestSource struct {
	Name string `json:"name"`
	URL  string `json:"url"`
}
type publicDigestStory struct {
	ID               string               `json:"id"`
	Kind             string               `json:"kind"`
	Title            string               `json:"title"`
	Summary          string               `json:"summary"`
	CategoryID       string               `json:"category_id"`
	PrimaryArticleID string               `json:"primary_article_id"`
	ArticleIDs       []string             `json:"article_ids"`
	Sources          []publicDigestSource `json:"sources"`
}
type publicDigestDetail struct {
	Version    string                 `json:"version"`
	Date       string                 `json:"date"`
	Locale     string                 `json:"locale"`
	EditionID  string                 `json:"edition_id"`
	IssueURL   string                 `json:"issue_url"`
	Categories []publicDigestCategory `json:"categories"`
	Stories    []publicDigestStory    `json:"stories"`
}

func (h *Handler) publicDigests(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if _, err := url.ParseQuery(r.URL.RawQuery); err != nil {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	locale, ok := requiredSingleQuery(r, "locale")
	if !ok || (locale != "ru" && locale != "sr-Latn") {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	month, supplied := singleQuery(r, "month")
	if !supplied {
		month = ""
	} else if !validDigestMonth(month) {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	if !onlyQueryKeys(r, "locale", "month") {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	reader, ok := h.cfg.Reader.(repositories.PublicDigestCatalogReader)
	if !ok || reader == nil {
		writeDigestAPIError(w, 503, "digest_unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	data, err := reader.EditorialDigestCatalog(ctx, locale, month, time.Now().UTC())
	if err != nil {
		writeDigestAPIError(w, 503, "digest_unavailable")
		return
	}
	response := publicDigestCatalog{Version: "1", Locale: locale, Timezone: "UTC", Month: data.Month, Issues: []publicDigestIssue{}}
	if data.EarliestDate != "" {
		value := data.EarliestDate
		response.EarliestDate = &value
	}
	if data.LatestDate != "" && data.LatestID != "" {
		response.Latest = &publicDigestIssue{Date: data.LatestDate, EditionID: data.LatestID}
	}
	for _, issue := range data.Issues {
		response.Issues = append(response.Issues, publicDigestIssue{Date: issue.Date, EditionID: issue.ID})
	}
	writeJSON(w, 200, response)
}

func (h *Handler) publicDigestDetail(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if _, err := url.ParseQuery(r.URL.RawQuery); err != nil {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	dateText := r.PathValue("date")
	date, err := time.Parse("2006-01-02", dateText)
	if err != nil || date.Year() < 1 || date.Format("2006-01-02") != dateText {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	locale, ok := requiredSingleQuery(r, "locale")
	if !ok || (locale != "ru" && locale != "sr-Latn") {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	editionID, hasID := singleQuery(r, "edition")
	if hasID && !validUUID(editionID) {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	if hasID {
		editionID = strings.ToLower(editionID)
	}
	if !onlyQueryKeys(r, "locale", "edition") {
		writeDigestAPIError(w, 400, "invalid_request")
		return
	}
	if h.cfg.Reader == nil {
		writeDigestAPIError(w, 503, "digest_unavailable")
		return
	}
	reader, ok := h.cfg.Reader.(repositories.EditorialDigestReader)
	if !ok {
		writeDigestAPIError(w, 503, "digest_unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	edition, err := reader.EditorialDigest(ctx, date, editionID, locale)
	if err != nil {
		writeDigestAPIError(w, 503, "digest_unavailable")
		return
	}
	if edition == nil {
		writeDigestAPIError(w, 404, "digest_not_found")
		return
	}
	if !validUUID(edition.ID) || edition.Date != dateText || edition.Locale != locale || edition.Edition.Version != "1" || edition.Edition.Date != dateText || edition.Edition.Locale != locale || edition.Snapshot.Date != dateText || (hasID && edition.ID != editionID) {
		writeDigestAPIError(w, 503, "digest_unavailable")
		return
	}
	view := buildEditorialDigestView(date, edition)
	if len(edition.Edition.Stories) > 0 && view.MainCount+view.BriefCount == 0 {
		writeDigestAPIError(w, 503, "digest_unavailable")
		return
	}
	result := publicDigestDetail{Version: "1", Date: dateText, Locale: locale, EditionID: edition.ID, IssueURL: view.IssueURL, Categories: []publicDigestCategory{}, Stories: []publicDigestStory{}}
	categoryIndex := map[string]int{}
	appendStory := func(story DigestStoryView, kind string) {
		index, found := categoryIndex[story.CategoryID]
		if !found {
			index = len(result.Categories)
			categoryIndex[story.CategoryID] = index
			result.Categories = append(result.Categories, publicDigestCategory{ID: story.CategoryID, Name: story.Category})
		}
		result.Categories[index].StoryCount++
		item := publicDigestStory{ID: story.ID, Kind: kind, Title: story.Title, Summary: story.Summary, CategoryID: story.CategoryID, PrimaryArticleID: story.PrimaryArticleID, ArticleIDs: append([]string{}, story.ArticleIDs...), Sources: []publicDigestSource{}}
		for _, source := range story.Sources {
			item.Sources = append(item.Sources, publicDigestSource{Name: source.Name, URL: source.URL})
		}
		result.Stories = append(result.Stories, item)
	}
	for _, story := range view.Main {
		appendStory(story, "main")
	}
	for _, group := range view.BriefGroups {
		for _, story := range group.Stories {
			appendStory(story, "brief")
		}
	}
	writeJSON(w, 200, result)
}

func requiredSingleQuery(r *http.Request, key string) (string, bool) {
	value, ok := singleQuery(r, key)
	return value, ok && value != ""
}
func singleQuery(r *http.Request, key string) (string, bool) {
	values, ok := r.URL.Query()[key]
	return func() (string, bool) {
		if !ok || len(values) != 1 {
			return "", false
		}
		return values[0], true
	}()
}
func onlyQueryKeys(r *http.Request, keys ...string) bool {
	allowed := map[string]bool{}
	for _, key := range keys {
		allowed[key] = true
	}
	for key, values := range r.URL.Query() {
		if !allowed[key] || len(values) != 1 {
			return false
		}
	}
	return true
}
func validDigestMonth(value string) bool {
	if len(value) != 7 || value[4] != '-' {
		return false
	}
	parsed, err := time.Parse("2006-01", value)
	return err == nil && parsed.Year() >= 1 && parsed.Format("2006-01") == value
}
func writeDigestAPIError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code}})
}
