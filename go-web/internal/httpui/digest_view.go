package httpui

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
)

type EditorialDigestView struct {
	DateLabel    string
	IssueURL     string
	EditionID    string
	Main         []DigestStoryView
	BriefGroups  []DigestBriefGroup
	MainCount    int
	BriefCount   int
	ArticleCount int
	MainLabel    string
	BriefLabel   string
	ArticleLabel string
}

type DigestStoryView struct {
	ID         string
	Title      string
	Summary    string
	ArticleURL string
	Sources    []DigestSourceView
	Category   string
}

type DigestSourceView struct {
	Name string
	URL  string
}

type DigestBriefGroup struct {
	ID      string
	Name    string
	Stories []DigestStoryView
}

type LegacyDigestView struct {
	DateLabel    string
	Categories   []LegacyDigestCategory
	ArticleCount int
	SummaryCount int
}

type LegacyDigestCategory struct {
	ID       string
	Name     string
	Summary  string
	Articles []LegacyDigestArticle
}

type LegacyDigestArticle struct {
	ID         string
	Title      string
	Summary    string
	ArticleURL string
	Source     DigestSourceView
}

type ArticleView struct {
	ID           string
	Title        string
	Summary      string
	Content      string
	SourceName   string
	SourceURL    string
	Category     string
	PublishedAt  string
	PublishedISO string
}

func buildEditorialDigestView(date time.Time, edition *repositories.EditorialDigestEdition) EditorialDigestView {
	view := EditorialDigestView{DateLabel: russianDateLabel(date), EditionID: edition.ID}
	if edition.Locale == "sr-Latn" {
		view.DateLabel = date.Format("02.01.2006.")
	}
	view.IssueURL = fmt.Sprintf("/digest/%s?locale=%s&edition=%s", date.Format("2006-01-02"), url.QueryEscape(edition.Locale), url.QueryEscape(edition.ID))
	articles := make(map[string]repositories.EditorialDigestArticle, len(edition.Snapshot.Articles))
	for _, article := range edition.Snapshot.Articles {
		if validArticleID(article.ID) {
			articles[article.ID] = article
		}
	}

	mainArticleIDs := make(map[string]bool)
	articleIDsByStory := make(map[string][]string)
	mainStories := make([]DigestStoryView, 0)
	briefStories := make([]DigestStoryView, 0)
	seenMainStories := make(map[string]bool)
	for _, story := range edition.Edition.Stories {
		viewStory, ids, ok := editorialStoryView(story, articles)
		if !ok {
			continue
		}
		switch story.Kind {
		case "main":
			if seenMainStories[story.ID] {
				continue
			}
			seenMainStories[story.ID] = true
			mainStories = append(mainStories, viewStory)
			for _, id := range ids {
				mainArticleIDs[id] = true
			}
		case "brief":
			briefStories = append(briefStories, viewStory)
			articleIDsByStory[viewStory.ID] = ids
		}
	}
	view.Main = mainStories
	view.MainCount = len(mainStories)
	uniqueArticleIDs := make(map[string]bool)
	for id := range mainArticleIDs {
		uniqueArticleIDs[id] = true
	}
	groupsByName := make(map[string]int)
	for _, story := range briefStories {
		storyIDs := articleIDsByStory[story.ID]
		duplicate := false
		for _, id := range storyIDs {
			if mainArticleIDs[id] || uniqueArticleIDs[id] {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		name := story.Category
		if name == "" {
			name = "Другие темы"
			if edition.Locale == "sr-Latn" {
				name = "Ostale teme"
			}
		}
		groupIndex, exists := groupsByName[name]
		if !exists {
			groupIndex = len(view.BriefGroups)
			groupsByName[name] = groupIndex
			view.BriefGroups = append(view.BriefGroups, DigestBriefGroup{ID: fmt.Sprintf("briefs-%d", groupIndex), Name: name})
		}
		view.BriefGroups[groupIndex].Stories = append(view.BriefGroups[groupIndex].Stories, story)
		for _, id := range storyIDs {
			uniqueArticleIDs[id] = true
		}
	}
	for _, group := range view.BriefGroups {
		view.BriefCount += len(group.Stories)
	}
	view.ArticleCount = len(uniqueArticleIDs)
	view.MainLabel = russianCount(view.MainCount, "главный сюжет", "главных сюжета", "главных сюжетов")
	view.BriefLabel = russianCount(view.BriefCount, "короткая новость", "короткие новости", "коротких новостей")
	view.ArticleLabel = russianCount(view.ArticleCount, "материал", "материала", "материалов")
	if edition.Locale == "sr-Latn" {
		view.MainLabel = fmt.Sprintf("Glavne priče: %d", view.MainCount)
		view.BriefLabel = fmt.Sprintf("Kratke vesti: %d", view.BriefCount)
		view.ArticleLabel = fmt.Sprintf("Članci: %d", view.ArticleCount)
		for i := range view.Main {
			view.Main[i].ArticleURL += "?locale=sr-Latn"
		}
		for i := range view.BriefGroups {
			for j := range view.BriefGroups[i].Stories {
				view.BriefGroups[i].Stories[j].ArticleURL += "?locale=sr-Latn"
			}
		}
	}
	return view
}

func editorialStoryView(story repositories.EditorialDigestStory, articles map[string]repositories.EditorialDigestArticle) (DigestStoryView, []string, bool) {
	if story.Kind != "main" && story.Kind != "brief" {
		return DigestStoryView{}, nil, false
	}
	if !validArticleID(story.PrimaryArticleID) {
		return DigestStoryView{}, nil, false
	}
	primary, exists := articles[story.PrimaryArticleID]
	if !exists || strings.TrimSpace(story.Title) == "" {
		return DigestStoryView{}, nil, false
	}
	ids := []string{story.PrimaryArticleID}
	seen := map[string]bool{story.PrimaryArticleID: true}
	for _, id := range story.ArticleIDs {
		if validArticleID(id) && !seen[id] {
			if _, ok := articles[id]; ok {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}
	view := DigestStoryView{ID: story.ID, Title: story.Title, Summary: story.Summary, ArticleURL: "/article/" + story.PrimaryArticleID, Category: primary.CategoryName}
	seenSources := make(map[string]bool)
	for _, id := range ids {
		article := articles[id]
		sourceURL := safeExternalURL(article.URL)
		name := strings.TrimSpace(article.SourceName)
		if name == "" {
			continue
		}
		key := name + "\x00" + sourceURL
		if sourceURL != "" && !seenSources[key] {
			seenSources[key] = true
			view.Sources = append(view.Sources, DigestSourceView{Name: name, URL: sourceURL})
		}
	}
	return view, ids, true
}

func validArticleID(id string) bool {
	n, err := strconv.ParseInt(id, 10, 64)
	return err == nil && n > 0
}

func safeExternalURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil {
		return ""
	}
	return u.String()
}

func buildLegacyDigestView(page repositories.DigestPage) LegacyDigestView {
	view := LegacyDigestView{DateLabel: russianDateLabel(page.Date), SummaryCount: len(page.Summaries)}
	summaries := make(map[string]string, len(page.Summaries))
	for _, summary := range page.Summaries {
		summaries[summary.Category] = summary.Text
	}
	seen := make(map[string]bool)
	for index, category := range page.Categories {
		id := fmt.Sprintf("category-%d", index)
		viewCategory := LegacyDigestCategory{ID: id, Name: category.Name, Summary: summaries[category.Name]}
		for _, article := range category.Articles {
			if article.ID < 1 || seen[strconv.FormatInt(article.ID, 10)] {
				continue
			}
			articleID := strconv.FormatInt(article.ID, 10)
			seen[articleID] = true
			item := LegacyDigestArticle{ID: articleID, Title: article.Title, ArticleURL: "/article/" + articleID}
			if article.Summary != nil {
				item.Summary = *article.Summary
			}
			if external := safeExternalURL(article.URL); external != "" && strings.TrimSpace(article.SourceName) != "" {
				item.Source = DigestSourceView{Name: article.SourceName, URL: external}
			}
			viewCategory.Articles = append(viewCategory.Articles, item)
			view.ArticleCount++
		}
		view.Categories = append(view.Categories, viewCategory)
	}
	categoryNames := make(map[string]bool, len(view.Categories))
	for _, category := range view.Categories {
		categoryNames[category.Name] = true
	}
	for _, summary := range page.Summaries {
		if !categoryNames[summary.Category] {
			view.Categories = append(view.Categories, LegacyDigestCategory{ID: fmt.Sprintf("category-%d", len(view.Categories)), Name: summary.Category, Summary: summary.Text})
			categoryNames[summary.Category] = true
		}
	}
	return view
}

func russianDateLabel(date time.Time) string {
	months := [...]string{"января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"}
	return fmt.Sprintf("%d %s %d", date.Day(), months[date.Month()-1], date.Year())
}

func russianCount(count int, one, few, many string) string {
	mod100 := count % 100
	form := many
	if mod100 < 11 || mod100 > 14 {
		switch count % 10 {
		case 1:
			form = one
		case 2, 3, 4:
			form = few
		}
	}
	return fmt.Sprintf("%d %s", count, form)
}

func buildArticleView(id int64, article map[string]any) ArticleView {
	view := ArticleView{ID: strconv.FormatInt(id, 10), Title: stringField(article, "title"), Summary: stringField(article, "summary"), Content: stringField(article, "content"), SourceName: stringField(article, "source_name"), SourceURL: safeExternalURL(stringField(article, "url")), Category: stringField(article, "category"), PublishedISO: stringField(article, "published_at")}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999", "2006-01-02T15:04:05"} {
		if published, err := time.Parse(layout, view.PublishedISO); err == nil {
			view.PublishedISO = published.UTC().Format(time.RFC3339)
			view.PublishedAt = published.UTC().Format("02.01.2006, 15:04 UTC")
			break
		}
	}
	return view
}

func stringField(values map[string]any, key string) string {
	value, ok := values[key].(string)
	if !ok {
		return ""
	}
	return value
}
