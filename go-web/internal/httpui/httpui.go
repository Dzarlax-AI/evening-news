package httpui

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html/template"
	"net/http"
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
	mux.HandleFunc("GET /", h.root)
	mux.HandleFunc("GET /list", h.listPage)
	mux.HandleFunc("GET /search", h.searchPage)
	mux.HandleFunc("GET /digest/{date}", h.digestPage)
	mux.HandleFunc("GET /article/{articleID}", h.articlePage)
	for _, path := range []string{"/news", "/feed", "/cards"} {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/", http.StatusMovedPermanently) })
	}
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("GET /health/app", h.healthApp)
	mux.HandleFunc("GET /api/public/feed", h.publicFeed)
	mux.HandleFunc("GET /api/public/article/{articleID}", h.publicArticle)
	mux.HandleFunc("GET /api/public/categories/config", h.categoriesConfig)
	mux.HandleFunc("GET /api/public/categories", h.categories)
	mux.HandleFunc("GET /api/public/sources", h.sources)
	mux.HandleFunc("GET /api/public/search", h.search)
	mux.HandleFunc("GET /api/v1/", h.apiRoot)
	mux.HandleFunc("GET /api/v1/feed", h.apiFeed)
	mux.HandleFunc("GET /api/v1/feed.rss", h.rss)
	mux.Handle("GET /static/", h.static())
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
	fragments, err := h.pageFragments()
	if err != nil {
		http.Error(w, "page unavailable", http.StatusInternalServerError)
		return
	}
	h.renderPage(w, r, "public/search.html", map[string]any{"Title": "Поиск новостей", "Page": "search", "FeedFragments": fragments})
}
func (h *Handler) feedPage(w http.ResponseWriter, r *http.Request, title, name string) {
	content, err := h.pageFragments()
	if err != nil {
		http.Error(w, "page unavailable", 500)
		return
	}
	h.renderPage(w, r, name, map[string]any{"Title": title, "Page": "feed", "FeedFragments": content})
}
func (h *Handler) digestPage(w http.ResponseWriter, r *http.Request) {
	date, err := time.Parse("2006-01-02", r.PathValue("date"))
	if err != nil {
		writeProblem(w, 404, "Digest not found")
		return
	}
	query := r.URL.Query()
	localeValues, localeSupplied := query["locale"]
	locale := "ru"
	if localeSupplied {
		if len(localeValues) != 1 || localeValues[0] != "ru" {
			writeProblem(w, http.StatusNotFound, "Digest not found")
			return
		}
		locale = localeValues[0]
	}
	if locale != "ru" {
		writeProblem(w, http.StatusNotFound, "Digest not found")
		return
	}
	editionValues, editionSupplied := query["edition"]
	editionID := ""
	if editionSupplied {
		if len(editionValues) != 1 || !validUUID(editionValues[0]) {
			writeProblem(w, http.StatusNotFound, "Digest not found")
			return
		}
		editionID = editionValues[0]
	}
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	editorial, supportsEditions := h.cfg.Reader.(repositories.EditorialDigestReader)
	if editionID != "" && !supportsEditions {
		writeProblem(w, http.StatusNotFound, "Digest not found")
		return
	}
	if supportsEditions {
		edition, readErr := editorial.EditorialDigest(ctx, date, editionID, locale)
		if readErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
			return
		}
		if editionID != "" && edition == nil {
			writeProblem(w, http.StatusNotFound, "Digest not found")
			return
		}
		if edition != nil {
			if edition.Date != date.Format("2006-01-02") || edition.Locale != locale || edition.Edition.Locale != locale {
				writeProblem(w, http.StatusNotFound, "Digest not found")
				return
			}
			view := buildEditorialDigestView(date, edition)
			h.renderPage(w, r, "public/digest.html", map[string]any{"Title": "Главное за " + date.Format("02.01.2006"), "Page": "digest", "Editorial": view})
			return
		}
	}
	page, err := h.cfg.Reader.Digest(ctx, date)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	if len(page.Summaries) == 0 && len(page.Categories) == 0 {
		writeProblem(w, 404, "Digest not found")
		return
	}
	page.Date = date
	h.renderPage(w, r, "public/digest.html", map[string]any{"Title": "Главное за " + date.Format("02.01.2006"), "Page": "digest", "Legacy": buildLegacyDigestView(page)})
}

func (h *Handler) articlePage(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("articleID"), 10, 64)
	if err != nil || id < 1 {
		writeProblem(w, http.StatusNotFound, "Article not found")
		return
	}
	if h.cfg.Reader == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	article, err := h.cfg.Reader.Article(ctx, id, false)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Public data unavailable")
		return
	}
	if article == nil || article["is_advertisement"] == true {
		writeProblem(w, http.StatusNotFound, "Article not found")
		return
	}
	view := buildArticleView(id, article)
	if view.Title == "" {
		view.Title = "Статья"
	}
	h.renderPage(w, r, "public/article.html", map[string]any{"Title": view.Title, "Page": "article", "Article": view})
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
	t, err := h.loadTemplate(name, adminPage)
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
	_, err = w.Write(rendered.Bytes())
	return err
}

func (h *Handler) loadTemplate(name string, adminPage bool) (*template.Template, error) {
	key := name
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
	if adminPage {
		paths = []string{filepath.Join(h.cfg.TemplatesDir, "admin", "admin_base.html"), path}
	}
	funcs := template.FuncMap{"paragraphs": func(s string) []string {
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
	parsed, err := template.New(filepath.Base(name)).Funcs(funcs).ParseFiles(paths...)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	h.templates[key] = parsed
	return parsed, nil
}

type PageFragments struct{ Sidebar, Filters, Core, Modal, ListStyles, SearchStyles template.HTML }

func (h *Handler) pageFragments() (PageFragments, error) {
	files := []string{"public/fragments/unified_sidebar.html", "public/fragments/filters_toolbar.html", "public/fragments/feed_core.html", "public/fragments/article_modal.html", "public/fragments/list_styles.html", "public/fragments/search_styles.html"}
	out := PageFragments{}
	targets := []*template.HTML{&out.Sidebar, &out.Filters, &out.Core, &out.Modal, &out.ListStyles, &out.SearchStyles}
	for i, path := range files {
		b, err := os.ReadFile(filepath.Join(h.cfg.TemplatesDir, path))
		if err != nil {
			return PageFragments{}, err
		}
		*targets[i] = template.HTML(b)
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
