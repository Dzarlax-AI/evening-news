package httpui

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
)

type PublicReader interface{ repositories.PublicReader }

type Config struct {
	Reader PublicReader
	// PublicAdmin is supplied by the auth component and is intentionally limited
	// to the public article visibility check.
	PublicAdmin  func(*http.Request) bool
	TemplatesDir string
	StaticDir    string
	StartedAt    time.Time
	Version      string
	SiteBaseURL  string
}

type Handler struct {
	cfg        Config
	templateMu sync.RWMutex
	templates  map[string]*template.Template
}

func New(cfg Config) *Handler {
	if cfg.TemplatesDir == "" {
		cfg.TemplatesDir = "templates"
	}
	if cfg.StaticDir == "" {
		cfg.StaticDir = "../web/static"
	}
	if cfg.SiteBaseURL == "" {
		cfg.SiteBaseURL = "http://localhost:8000"
	}
	if cfg.Version == "" {
		cfg.Version = "2.0.0"
	}
	if cfg.StartedAt.IsZero() {
		cfg.StartedAt = time.Now().UTC()
	}
	return &Handler{cfg: cfg, templates: make(map[string]*template.Template)}
}

// Register installs only public routes. Auth endpoints are registered by the
// auth component; management routes belong to their own components.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /", h.localizedPublicAPI(h.root))
	mux.HandleFunc("GET /list", h.localizedPublicAPI(h.listPage))
	mux.HandleFunc("GET /search", h.localizedPublicAPI(h.searchPage))
	mux.HandleFunc("GET /digest/{date}", h.digestPage)
	mux.HandleFunc("GET /article/{articleID}", h.localizedPublicAPI(h.articlePage))
	for _, path := range []string{"/news", "/feed", "/cards"} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/", http.StatusMovedPermanently) })
	}
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /health/app", h.healthApp)
	mux.HandleFunc("GET /api/public/feed", h.localizedPublicAPI(h.publicFeed))
	mux.HandleFunc("GET /api/public/article/{articleID}", h.localizedPublicAPI(h.publicArticle))
	mux.HandleFunc("GET /api/public/categories/config", h.localizedPublicAPI(h.categoriesConfig))
	mux.HandleFunc("GET /api/public/categories", h.localizedPublicAPI(h.categories))
	mux.HandleFunc("GET /api/public/sources", h.localizedPublicAPI(h.sources))
	mux.HandleFunc("GET /api/public/search", h.localizedPublicAPI(h.search))
	mux.HandleFunc("GET /api/v1/", h.apiRoot)
	mux.HandleFunc("GET /api/v1/feed", h.localizedPublicAPI(h.apiFeed))
	mux.HandleFunc("GET /api/v1/feed.rss", h.localizedPublicAPI(h.rss))
	mux.Handle("GET /static/", h.static())
}

// requestLocale gives an explicit query precedence over a saved preference.
// Reading a preference never changes state; the language control writes it.
func requestLocale(r *http.Request) (string, bool) {
	values, supplied := r.URL.Query()["locale"]
	if supplied {
		if len(values) != 1 || (values[0] != "ru" && values[0] != "sr-Latn") {
			return "", false
		}
		return values[0], true
	}
	// Public permalinks stay Russian unless their URL explicitly says otherwise.
	if !(strings.HasPrefix(r.URL.Path, "/admin") || r.URL.Path == "/login" || r.URL.Path == "/setup") {
		return "ru", true
	}
	cookie, err := r.Cookie("news_locale")
	if err == nil {
		if cookie.Value != "ru" && cookie.Value != "sr-Latn" {
			return "", false
		}
		return cookie.Value, true
	}
	return "ru", true
}

func (h *Handler) localizedPublicAPI(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		locale, valid := requestLocale(r)
		if !valid {
			http.NotFound(w, r)
			return
		}
		w.Header().Add("Vary", "Cookie")
		if locale == "sr-Latn" {
			reader, ok := h.cfg.Reader.(repositories.LocalizedPublicReader)
			if !ok || !reader.SupportsPublicLocale(locale) {
				http.Error(w, "Srpski prikaz trenutno nije dostupan.", http.StatusServiceUnavailable)
				return
			}
			r = r.WithContext(repositories.WithPublicLocale(r.Context(), locale))
		}
		next(w, r)
	}
}

// Pages stay unavailable in Serbian until their template and scripts are localized.
func (h *Handler) russianOnlyUntilLocalized(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		locale, valid := requestLocale(r)
		if !valid {
			http.NotFound(w, r)
			return
		}
		w.Header().Add("Vary", "Cookie")
		if locale == "sr-Latn" {
			http.Error(w, "Srpski prikaz trenutno nije dostupan.", http.StatusServiceUnavailable)
			return
		}
		next(w, r)
	}
}

func (h *Handler) publicFeed(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, e := intQuery(q.Get("limit"), 20, 1, 1000)
	if e != nil {
		writeValidationError(w, "limit", "ensure this value is greater than or equal to 1")
		return
	}
	offset, e := intQuery(q.Get("offset"), 0, 0, 1<<31-1)
	if e != nil {
		writeValidationError(w, "offset", "ensure this value is greater than or equal to 0")
		return
	}
	since, e := optionalIntQuery(q.Get("since_hours"), 1, 168)
	if e != nil {
		writeValidationError(w, "since_hours", "ensure this value is between 1 and 168")
		return
	}
	hide, e := boolQuery(q.Get("hide_ads"), true)
	if e != nil {
		writeValidationError(w, "hide_ads", "value could not be parsed to a boolean")
		return
	}
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	out, err := h.cfg.Reader.Feed(ctx, repositories.FeedQuery{Limit: limit, Offset: offset, SinceHours: since, Category: q.Get("category"), Source: q.Get("source"), HideAds: hide})
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Handler) publicArticle(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("articleID"), 10, 64)
	if err != nil || id < 1 {
		writeProblem(w, http.StatusNotFound, "Article not found")
		return
	}
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	include := false
	if h.cfg.PublicAdmin != nil {
		include = h.cfg.PublicAdmin(r)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	article, err := h.cfg.Reader.Article(ctx, id, include)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	if article == nil {
		writeProblem(w, http.StatusNotFound, "Article not found")
		return
	}
	writeJSON(w, http.StatusOK, article)
}

func (h *Handler) categoriesConfig(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	out, err := h.cfg.Reader.CategoriesConfig(ctx)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	writeJSON(w, http.StatusOK, out)
}
func (h *Handler) categories(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	q, err := statsQuery(r, 168)
	if err != nil {
		writeValidationError(w, err.field, err.detail)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	out, e := h.cfg.Reader.Categories(ctx, q)
	if e != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	writeJSON(w, http.StatusOK, out)
}
func (h *Handler) sources(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	q, err := statsQuery(r, 168)
	if err != nil {
		writeValidationError(w, err.field, err.detail)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()
	out, e := h.cfg.Reader.Sources(ctx, q)
	if e != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	writeJSON(w, http.StatusOK, out)
}
func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	qv := r.URL.Query()
	raw, exists := qv["q"]
	if !exists || len(raw) == 0 {
		writeValidationError(w, "q", "field required")
		return
	}
	query := raw[0]
	if len([]rune(query)) < 2 {
		writeValidationError(w, "q", "ensure this value has at least 2 characters")
		return
	}
	query = strings.TrimSpace(query)
	if len([]rune(query)) < 2 {
		writeProblem(w, http.StatusBadRequest, "Search query must be at least 2 characters")
		return
	}
	limit, e := intQuery(qv.Get("limit"), 20, 1, 100)
	if e != nil {
		writeValidationError(w, "limit", "ensure this value is between 1 and 100")
		return
	}
	offset, e := intQuery(qv.Get("offset"), 0, 0, 1<<31-1)
	if e != nil {
		writeValidationError(w, "offset", "ensure this value is greater than or equal to 0")
		return
	}
	since, e := optionalIntQuery(qv.Get("since_hours"), 1, 8760)
	if e != nil {
		writeValidationError(w, "since_hours", "ensure this value is between 1 and 8760")
		return
	}
	sort := qv.Get("sort")
	if sort == "" {
		sort = "relevance"
	}
	if sort != "relevance" && sort != "date" && sort != "title" {
		writeValidationError(w, "sort", "string does not match regex \"^(relevance|date|title)$\"")
		return
	}
	hide, e := boolQuery(qv.Get("hide_ads"), true)
	if e != nil {
		writeValidationError(w, "hide_ads", "value could not be parsed to a boolean")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 7*time.Second)
	defer cancel()
	out, err := h.cfg.Reader.Search(ctx, repositories.SearchQuery{Query: query, Limit: limit, Offset: offset, Category: qv.Get("category"), SinceHours: since, Sort: sort, HideAds: hide})
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Internal server error")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

type validationError struct{ field, detail string }

func statsQuery(r *http.Request, max int) (repositories.StatsQuery, *validationError) {
	since, e := optionalIntQuery(r.URL.Query().Get("since_hours"), 1, max)
	if e != nil {
		return repositories.StatsQuery{}, &validationError{"since_hours", fmt.Sprintf("ensure this value is between 1 and %d", max)}
	}
	hide, e := boolQuery(r.URL.Query().Get("hide_ads"), true)
	if e != nil {
		return repositories.StatsQuery{}, &validationError{"hide_ads", "value could not be parsed to a boolean"}
	}
	return repositories.StatsQuery{SinceHours: since, HideAds: hide}, nil
}

func (h *Handler) apiRoot(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"message": "Evening News v2 API", "version": "2.0.0", "endpoints": map[string]any{"feed": "/api/v1/feed - Get news feed", "rss": "/api/v1/feed.rss - RSS feed", "categories": "/api/v1/categories - Get categories", "sources": "/api/v1/sources - Get sources", "summaries": "/api/v1/summaries/daily - Daily summaries", "generate_summaries": "/api/v1/summaries/generate - Generate daily summaries", "process": "/api/v1/process - Trigger processing", "telegram": "/api/v1/telegram/send-digest - Send Telegram digest", "backup": "/api/v1/backup - Backup management", "restore": "/api/v1/restore - Restore from backup", "system": map[string]string{"process_monitor": "/api/v1/system/process-monitor - Process monitor status", "cleanup": "/api/v1/system/process-monitor/cleanup - Manual process cleanup"}}, "documentation": "/docs", "timestamp": time.Now().UTC().Format(time.RFC3339Nano)})
}
func (h *Handler) apiFeed(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	q := r.URL.Query()
	limit, e := intQuery(q.Get("limit"), 20, 1, 100)
	if e != nil {
		writeValidationError(w, "limit", "ensure this value is between 1 and 100")
		return
	}
	offset, e := intQuery(q.Get("offset"), 0, 0, 1<<31-1)
	if e != nil {
		writeValidationError(w, "offset", "ensure this value is greater than or equal to 0")
		return
	}
	since, e := optionalIntQuery(q.Get("since_hours"), 1, 168)
	if e != nil {
		writeValidationError(w, "since_hours", "ensure this value is between 1 and 168")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	out, err := h.cfg.Reader.APIFeed(ctx, repositories.APIFeedQuery{Limit: limit, Offset: offset, SinceHours: since, Category: q.Get("category")})
	if err != nil {
		writeProblem(w, 500, "Internal server error")
		return
	}
	writeJSON(w, 200, out)
}

type rssDocument struct {
	XMLName xml.Name   `xml:"rss"`
	Version string     `xml:"version,attr"`
	Channel rssChannel `xml:"channel"`
}
type rssChannel struct {
	Title         string    `xml:"title"`
	Link          string    `xml:"link"`
	Description   string    `xml:"description"`
	Generator     string    `xml:"generator"`
	LastBuildDate string    `xml:"lastBuildDate"`
	Language      string    `xml:"language,omitempty"`
	Items         []rssItem `xml:"item"`
}
type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	Description string `xml:"description"`
	GUID        string `xml:"guid"`
	PubDate     string `xml:"pubDate,omitempty"`
}

func (h *Handler) rss(w http.ResponseWriter, r *http.Request) {
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	limit, e := intQuery(r.URL.Query().Get("limit"), 50, 1, 100)
	if e != nil {
		writeValidationError(w, "limit", "ensure this value is between 1 and 100")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	articles, err := h.cfg.Reader.RSS(ctx, limit)
	if err != nil {
		writeProblem(w, 500, "Internal server error")
		return
	}
	doc := rssDocument{Version: "2.0", Channel: rssChannel{Title: "Evening News v2", Link: "http://localhost:8000", Description: "Aggregated news feed", Generator: "Evening News v2.0", LastBuildDate: time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")}}
	if locale, _ := requestLocale(r); locale == "sr-Latn" {
		doc.Channel.Title = "Evening News — vesti na srpskom"
		doc.Channel.Description = "Objedinjene vesti na srpskom jeziku"
		link, err := serbianRSSChannelLink(h.cfg.SiteBaseURL)
		if err != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
			return
		}
		doc.Channel.Link = link
		doc.Channel.Language = "sr-Latn"
	}
	for _, a := range articles {
		title := ""
		if a.Title != nil {
			title = cleanPublicText(*a.Title)
		}
		description := title
		if a.Summary != nil {
			description = cleanPublicText(*a.Summary)
		}
		item := rssItem{Title: title, Link: a.URL, Description: description, GUID: strconv.FormatInt(a.ID, 10)}
		if a.PublishedAt != nil {
			item.PubDate = a.PublishedAt.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
		}
		doc.Channel.Items = append(doc.Channel.Items, item)
	}
	body, err := xml.MarshalIndent(doc, "", "  ")
	if err != nil {
		writeProblem(w, 500, "Internal server error")
		return
	}
	w.Header().Set("Content-Type", "application/rss+xml; charset=utf-8")
	w.WriteHeader(200)
	_, _ = w.Write(append([]byte(xml.Header), body...))
}

func serbianRSSChannelLink(base string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") {
		return "", errors.New("invalid public site origin")
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/", RawQuery: "locale=sr-Latn"}).String(), nil
}

func cleanPublicText(s string) string {
	s = strings.ReplaceAll(s, "&amp;", "&")
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	return strings.TrimSpace(s)
}

func (h *Handler) root(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	h.feedPage(w, r, "Лента новостей", "public/list.html")
}
func (h *Handler) listPage(w http.ResponseWriter, r *http.Request) {
	h.feedPage(w, r, "Лента новостей", "public/list.html")
}
func (h *Handler) searchPage(w http.ResponseWriter, r *http.Request) {
	locale, _ := requestLocale(r)
	fragments, err := h.pageFragments(locale)
	if err != nil {
		http.Error(w, "page unavailable", http.StatusInternalServerError)
		return
	}
	title := "Поиск новостей"
	if locale == "sr-Latn" {
		title = "Pretraga vesti"
	}
	h.renderPage(w, r, "public/search.html", map[string]any{"Title": title, "Page": "search", "FeedFragments": fragments})
}
func (h *Handler) feedPage(w http.ResponseWriter, r *http.Request, title, name string) {
	locale, _ := requestLocale(r)
	if locale == "sr-Latn" {
		title = "Vesti"
	}
	content, err := h.pageFragments(locale)
	if err != nil {
		http.Error(w, "page unavailable", 500)
		return
	}
	h.renderPage(w, r, name, map[string]any{"Title": title, "Page": "feed", "FeedFragments": content})
}
func (h *Handler) digestPage(w http.ResponseWriter, r *http.Request) {
	date, err := time.Parse("2006-01-02", r.PathValue("date"))
	if err != nil {
		writePublicPageProblem(w, r, 404, "Digest not found")
		return
	}
	query := r.URL.Query()
	locale, valid := requestLocale(r)
	if !valid {
		writePublicPageProblem(w, r, http.StatusNotFound, "Digest not found")
		return
	}
	w.Header().Add("Vary", "Cookie")
	editionValues, editionSupplied := query["edition"]
	editionID := ""
	if editionSupplied {
		if len(editionValues) != 1 || !validUUID(editionValues[0]) {
			writePublicPageProblem(w, r, http.StatusNotFound, "Digest not found")
			return
		}
		editionID = editionValues[0]
	}
	if h.cfg.Reader == nil {
		writePublicPageProblem(w, r, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	editorial, supportsEditions := h.cfg.Reader.(repositories.EditorialDigestReader)
	if (editionID != "" || locale != "ru") && !supportsEditions {
		writePublicPageProblem(w, r, http.StatusNotFound, "Digest not found")
		return
	}
	if supportsEditions {
		edition, readErr := editorial.EditorialDigest(ctx, date, editionID, locale)
		if readErr != nil {
			writePublicPageProblem(w, r, http.StatusServiceUnavailable, "Public data unavailable")
			return
		}
		if editionID != "" && edition == nil {
			writePublicPageProblem(w, r, http.StatusNotFound, "Digest not found")
			return
		}
		if edition != nil {
			if edition.Date != date.Format("2006-01-02") || edition.Locale != locale || edition.Edition.Locale != locale || (editionID != "" && edition.ID != editionID) {
				writePublicPageProblem(w, r, http.StatusNotFound, "Digest not found")
				return
			}
			view := buildEditorialDigestView(date, edition)
			switches := map[string]string{locale: edition.ID}
			target := "sr-Latn"
			if locale == "sr-Latn" {
				target = "ru"
			}
			if paired, ok := h.cfg.Reader.(repositories.EditorialDigestCounterpartReader); ok {
				id, pairErr := paired.EditorialDigestCounterpart(ctx, date, edition.ID, locale, target)
				if pairErr != nil {
					writePublicPageProblem(w, r, http.StatusServiceUnavailable, "Public data unavailable")
					return
				}
				if validUUID(id) {
					switches[target] = id
				}
			}
			switchURL := func(language string) string {
				id := switches[language]
				if id == "" {
					return ""
				}
				q := r.URL.Query()
				q.Set("locale", language)
				q.Set("edition", id)
				return r.URL.Path + "?" + q.Encode()
			}
			title := "Главное за " + date.Format("02.01.2006")
			if locale == "sr-Latn" {
				title = "Najvažnije za " + date.Format("02.01.2006")
			}
			data := map[string]any{"Title": title, "Page": "digest", "Locale": locale, "Editorial": view, "DigestSwitch": true, "SwitchRU": switchURL("ru"), "SwitchSR": switchURL("sr-Latn")}
			if switches["ru"] != "" && switches["sr-Latn"] != "" {
				data["AlternateRU"] = switchURL("ru")
				data["AlternateSR"] = switchURL("sr-Latn")
			}
			h.renderPage(w, r, "public/digest.html", data)
			return
		}
	}
	if locale != "ru" {
		writePublicPageProblem(w, r, http.StatusNotFound, "Digest not found")
		return
	}
	page, err := h.cfg.Reader.Digest(ctx, date)
	if err != nil {
		writePublicPageProblem(w, r, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	if len(page.Summaries) == 0 && len(page.Categories) == 0 {
		writePublicPageProblem(w, r, 404, "Digest not found")
		return
	}
	page.Date = date
	h.renderPage(w, r, "public/digest.html", map[string]any{"Title": "Главное за " + date.Format("02.01.2006"), "Page": "digest", "Legacy": buildLegacyDigestView(page)})
}

func (h *Handler) articlePage(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("articleID"), 10, 64)
	if err != nil || id < 1 {
		writePublicPageProblem(w, r, http.StatusNotFound, "Article not found")
		return
	}
	if h.cfg.Reader == nil {
		writePublicPageProblem(w, r, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	article, err := h.cfg.Reader.Article(ctx, id, false)
	if err != nil {
		writePublicPageProblem(w, r, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	locale, _ := requestLocale(r)
	if article == nil && locale == "sr-Latn" {
		original, readErr := h.cfg.Reader.Article(repositories.WithPublicLocale(ctx, "ru"), id, false)
		if readErr != nil {
			writePublicPageProblem(w, r, http.StatusServiceUnavailable, "Public data unavailable")
			return
		}
		if original != nil && original["is_advertisement"] != true {
			buffer := httptest.NewRecorder()
			h.renderPage(buffer, r, "public/translation_missing.html", map[string]any{"Title": "Prevod nije dostupan", "Page": "article", "Locale": "sr-Latn", "ArticleID": id})
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write(buffer.Body.Bytes())
			return
		}
	}
	if article == nil || article["is_advertisement"] == true {
		writePublicPageProblem(w, r, http.StatusNotFound, "Article not found")
		return
	}
	view := buildArticleView(id, article)
	if view.Title == "" {
		view.Title = "Статья"
	}
	h.renderPage(w, r, "public/article.html", map[string]any{"Title": view.Title, "Page": "article", "Locale": locale, "Article": view})
}

func writePublicPageProblem(w http.ResponseWriter, r *http.Request, status int, detail string) {
	locale, _ := requestLocale(r)
	if locale != "sr-Latn" {
		writeProblem(w, status, detail)
		return
	}
	message := "Podaci trenutno nisu dostupni."
	if status == http.StatusNotFound {
		if detail == "Digest not found" {
			message = "Izdanje nije pronađeno."
		} else {
			message = "Članak nije pronađen."
		}
	}
	http.Error(w, message, status)
}

// RenderTemplate is shared with the admin page component. Dynamic values pass
// through html/template escaping; templates do not receive raw request strings
// as trusted HTML.
func (h *Handler) RenderTemplate(w http.ResponseWriter, r *http.Request, name string, data any) error {
	return h.renderPageErr(w, r, name, data)
}
func (h *Handler) renderPage(w http.ResponseWriter, r *http.Request, name string, data any) {
	if err := h.renderPageErr(w, r, name, data); err != nil {
		http.Error(w, "page unavailable", 500)
	}
}
func (h *Handler) renderPageErr(w http.ResponseWriter, r *http.Request, name string, data any) error {
	if strings.Contains(name, "..") || filepath.IsAbs(name) {
		return errors.New("invalid template name")
	}
	adminPage := strings.HasPrefix(name, "admin/") && name != "admin/admin_base.html"
	locale, valid := requestLocale(r)
	if !valid {
		return errors.New("invalid locale")
	}
	if values, ok := data.(map[string]any); ok {
		copy := make(map[string]any, len(values)+1)
		for k, v := range values {
			copy[k] = v
		}
		copy["Locale"] = locale
		query := r.URL.Query()
		query.Del("locale")
		canonicalQuery := query.Encode()
		if locale == "sr-Latn" {
			query.Set("locale", "sr-Latn")
			canonicalQuery = query.Encode()
		}
		canonical := r.URL.EscapedPath()
		if canonicalQuery != "" {
			canonical += "?" + canonicalQuery
		}
		copy["Canonical"] = canonical
		if r.URL.Path == "/" || r.URL.Path == "/list" || r.URL.Path == "/search" {
			query.Del("locale")
			ru := r.URL.EscapedPath()
			if q := query.Encode(); q != "" {
				ru += "?" + q
			}
			copy["AlternateRU"] = ru
			query.Set("locale", "sr-Latn")
			copy["AlternateSR"] = r.URL.EscapedPath() + "?" + query.Encode()
		}
		data = copy
	}
	t, err := h.loadTemplate(name, adminPage, locale)
	if err != nil {
		return err
	}
	var rendered bytes.Buffer
	if adminPage {
		err = t.ExecuteTemplate(&rendered, "admin-base", data)
	} else if strings.HasPrefix(name, "admin/") {
		err = t.ExecuteTemplate(&rendered, filepath.Base(name), data)
	} else {
		err = t.ExecuteTemplate(&rendered, "base", data)
	}
	if err != nil {
		return fmt.Errorf("render template: %w", err)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if !adminPage && !strings.HasPrefix(name, "admin/") {
		sum := sha256.Sum256(rendered.Bytes())
		tag := fmt.Sprintf("\"%x\"", sum)
		w.Header().Set("ETag", tag)
		w.Header().Set("Cache-Control", "private, no-cache")
		w.Header().Add("Vary", "Cookie")
		if r.Header.Get("If-None-Match") == tag {
			w.WriteHeader(http.StatusNotModified)
			return nil
		}
	}
	_, err = w.Write(rendered.Bytes())
	return err
}

func (h *Handler) loadTemplate(name string, adminPage bool, locale string) (*template.Template, error) {
	key := name + "|" + locale
	h.templateMu.RLock()
	t := h.templates[key]
	h.templateMu.RUnlock()
	if t != nil {
		return t, nil
	}
	h.templateMu.Lock()
	defer h.templateMu.Unlock()
	if t = h.templates[key]; t != nil {
		return t, nil
	}
	path := filepath.Join(h.cfg.TemplatesDir, name)
	base := filepath.Join(h.cfg.TemplatesDir, "base.html")
	paths := []string{base, path}
	if name == "public/article.html" {
		paths = append(paths, filepath.Join(h.cfg.TemplatesDir, "public/fragments/article_reader.html"))
	}
	if adminPage {
		paths = []string{filepath.Join(h.cfg.TemplatesDir, "admin", "admin_base.html"), path}
	}
	funcs := template.FuncMap{"addOne": func(i int) int { return i + 1 }, "paragraphs": func(s string) []string {
		parts := strings.Split(s, "\n")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		return out
	}, "digestTotal": func(categories []repositories.DigestCategory) int {
		n := 0
		for _, category := range categories {
			n += len(category.Articles)
		}
		return n
	}}
	parsed := template.New(filepath.Base(name)).Funcs(funcs)
	for _, file := range paths {
		source, readErr := os.ReadFile(file)
		if readErr != nil {
			return nil, readErr
		}
		if _, parseErr := parsed.Parse(translateUISource(string(source), locale)); parseErr != nil {
			return nil, fmt.Errorf("parse template: %w", parseErr)
		}
	}
	h.templates[key] = parsed
	return parsed, nil
}

type PageFragments struct{ Sidebar, Filters, Core, Modal, ListStyles, SearchStyles template.HTML }

func (h *Handler) pageFragments(locale string) (PageFragments, error) {
	files := []string{"public/fragments/unified_sidebar.html", "public/fragments/filters_toolbar.html", "public/fragments/feed_core.html", "public/fragments/article_modal.html", "public/fragments/list_styles.html", "public/fragments/search_styles.html"}
	out := PageFragments{}
	targets := []*template.HTML{&out.Sidebar, &out.Filters, &out.Core, &out.Modal, &out.ListStyles, &out.SearchStyles}
	for i, path := range files {
		b, err := os.ReadFile(filepath.Join(h.cfg.TemplatesDir, path))
		if err != nil {
			return PageFragments{}, err
		}
		source := translateUISource(string(b), locale)
		if path == "public/fragments/article_modal.html" {
			reader, err := os.ReadFile(filepath.Join(h.cfg.TemplatesDir, "public/fragments/article_reader.html"))
			if err != nil {
				return PageFragments{}, err
			}
			t, err := template.New("reader").Funcs(template.FuncMap{"paragraphs": func(s string) []string { return strings.Split(s, "\n") }, "addOne": func(i int) int { return i + 1 }}).Parse(translateUISource(string(reader), locale))
			if err != nil {
				return PageFragments{}, err
			}
			var rendered bytes.Buffer
			if err = t.ExecuteTemplate(&rendered, "article-reader", map[string]any{"Locale": locale, "Article": buildArticleView(0, map[string]any{})}); err != nil {
				return PageFragments{}, err
			}
			source = strings.Replace(source, "{{READER_CONTENT}}", rendered.String(), 1)
		}
		*targets[i] = template.HTML(source)
	}
	return out, nil
}

func (h *Handler) static() http.Handler {
	root, err := filepath.Abs(h.cfg.StaticDir)
	if err != nil {
		return http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/static/")
		clean := filepath.Clean(filepath.FromSlash(rel))
		if clean == "." || filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			http.NotFound(w, r)
			return
		}
		path := filepath.Join(root, clean)
		real, err := filepath.EvalSymlinks(path)
		if err != nil || !strings.HasPrefix(real, root+string(filepath.Separator)) {
			http.NotFound(w, r)
			return
		}
		info, err := os.Stat(real)
		if err != nil || info.IsDir() {
			http.NotFound(w, r)
			return
		}
		if clean == "js/main.js" || clean == "js/onboarding.js" {
			locale, valid := requestLocale(r)
			if !valid {
				http.NotFound(w, r)
				return
			}
			w.Header().Add("Vary", "Cookie")
			if locale == "sr-Latn" {
				b, readErr := os.ReadFile(real)
				if readErr != nil {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
				http.ServeContent(w, r, info.Name(), info.ModTime(), strings.NewReader(translateUISource(string(b), locale)))
				return
			}
		}
		http.ServeFile(w, r, real)
	})
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "healthy"})
}
func (h *Handler) healthApp(w http.ResponseWriter, r *http.Request) {
	elapsed := time.Since(h.cfg.StartedAt).Seconds()
	writeJSON(w, 200, map[string]any{"version": h.cfg.Version, "started_at": h.cfg.StartedAt.UTC().Format(time.RFC3339Nano), "uptime_seconds": elapsed})
}

func intQuery(raw string, fallback, min, max int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, errors.New("invalid integer query")
	}
	return n, nil
}
func optionalIntQuery(raw string, min, max int) (*int, error) {
	if raw == "" {
		return nil, nil
	}
	n, err := intQuery(raw, 0, min, max)
	if err != nil {
		return nil, err
	}
	return &n, nil
}
func boolQuery(raw string, fallback bool) (bool, error) {
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseBool(raw)
	return v, err
}
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
func writeProblem(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}
func writeValidationError(w http.ResponseWriter, field, detail string) {
	writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"detail": []map[string]string{{"loc": "query." + field, "msg": detail, "type": "value_error"}}})
}
