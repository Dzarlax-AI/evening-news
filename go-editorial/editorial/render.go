package editorial

import (
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf16"
)

var generatedHTMLTags = regexp.MustCompile(`</?(?:b|a)(?: href="[^"]*")?>`)

type briefGroup struct {
	categoryID   string
	categoryName string
	stories      []Story
}

type plainBlock struct {
	html         string
	categoryID   string
	categoryName string
}

func Render(s Snapshot, locale string, e Edition) (Rendered, error) {
	if err := ValidateEdition(s, locale, e); err != nil {
		return Rendered{}, err
	}
	articles := make(map[string]Article, len(s.Articles))
	for _, a := range s.Articles {
		articles[a.ID] = a
	}
	mains, briefGroups := orderStories(e.Stories, articles)
	dateLabel, err := russianDateLabel(e.Date)
	if err != nil {
		return Rendered{}, fmt.Errorf("invalid_edition_date")
	}
	metadata := fmt.Sprintf("%d исходных материалов · Русский", len(s.Articles))
	if len(e.Excluded) > 0 {
		metadata += fmt.Sprintf(" · Исключено: %d", len(e.Excluded))
	}
	editionHeading := "Evening News · " + dateLabel

	var rich strings.Builder
	var preview strings.Builder
	blocks := []plainBlock{{html: "<b>" + html.EscapeString(editionHeading) + "</b>\n" + html.EscapeString(metadata)}, {html: "<b>Главное</b>"}}
	rich.WriteString("<h2>" + html.EscapeString(editionHeading) + "</h2><p>" + html.EscapeString(metadata) + "</p><h2>Главное</h2>")
	preview.WriteString(editionHeading + "\n" + metadata + "\nГлавное\n")
	if len(mains) == 0 {
		rich.WriteString("<p>Главных сюжетов нет.</p>")
		blocks = append(blocks, plainBlock{html: "Главных сюжетов нет."})
		preview.WriteString("Главных сюжетов нет.\n")
	}
	for _, story := range mains {
		renderStoryRich(&rich, story, articles, e.SiteBaseURL, false)
		renderStoryPreview(&preview, story, articles, e.SiteBaseURL)
		blocks = append(blocks, plainBlock{html: storyPlainHTML(story, articles, e.SiteBaseURL)})
	}

	if len(briefGroups) > 0 {
		rich.WriteString("<h2>Ещё коротко</h2>")
		preview.WriteString("\nЕщё коротко\n")
		blocks = append(blocks, plainBlock{html: "<b>Ещё коротко</b>"})
		for _, group := range briefGroups {
			rich.WriteString("<h3>" + html.EscapeString(group.categoryName) + "</h3>")
			for _, story := range group.stories {
				renderStoryRich(&rich, story, articles, e.SiteBaseURL, true)
				renderStoryPreview(&preview, story, articles, e.SiteBaseURL)
				blocks = append(blocks, plainBlock{html: storyPlainHTML(story, articles, e.SiteBaseURL), categoryID: group.categoryID, categoryName: group.categoryName})
			}
		}
	}
	rich.WriteString("<hr><p><a href=\"" + html.EscapeString(e.EditionURL) + "\">Полный выпуск</a></p>")
	preview.WriteString("\nПолный выпуск: " + e.EditionURL)
	parts, err := packPlainParts(blocks, e.EditionURL)
	if err != nil {
		return Rendered{}, err
	}
	return Rendered{RichHTML: rich.String(), PlainParts: parts, PreviewText: preview.String()}, nil
}

func orderStories(stories []Story, articles map[string]Article) ([]Story, []briefGroup) {
	mains := make([]Story, 0, len(stories))
	groups := make([]briefGroup, 0)
	groupIndex := make(map[string]int)
	for _, story := range stories {
		if story.Kind == "main" {
			mains = append(mains, story)
			continue
		}
		primary := articles[story.PrimaryArticleID]
		idx, ok := groupIndex[primary.CategoryID]
		if !ok {
			idx = len(groups)
			groupIndex[primary.CategoryID] = idx
			groups = append(groups, briefGroup{categoryID: primary.CategoryID, categoryName: primary.CategoryName})
		}
		groups[idx].stories = append(groups[idx].stories, story)
	}
	return mains, groups
}

func renderStoryRich(out *strings.Builder, story Story, articles map[string]Article, siteBaseURL string, brief bool) {
	primary := articles[story.PrimaryArticleID]
	internal := articleURL(siteBaseURL, primary.ID)
	if brief {
		out.WriteString("<p><b><a href=\"" + html.EscapeString(internal) + "\">" + html.EscapeString(story.Title) + "</a></b></p>")
	} else {
		out.WriteString("<h3><a href=\"" + html.EscapeString(internal) + "\">" + html.EscapeString(story.Title) + "</a></h3>")
	}
	out.WriteString("<p>" + html.EscapeString(story.Summary) + "</p><ul>")
	for _, id := range story.ArticleIDs {
		a := articles[id]
		out.WriteString("<li>Источник: <a href=\"" + html.EscapeString(a.URL) + "\">" + html.EscapeString(a.SourceName) + "</a></li>")
	}
	out.WriteString("</ul>")
}

func storyPlainHTML(story Story, articles map[string]Article, siteBaseURL string) string {
	primary := articles[story.PrimaryArticleID]
	var out strings.Builder
	out.WriteString("<b><a href=\"" + html.EscapeString(articleURL(siteBaseURL, primary.ID)) + "\">" + html.EscapeString(story.Title) + "</a></b>\n")
	out.WriteString(html.EscapeString(story.Summary))
	for _, id := range story.ArticleIDs {
		a := articles[id]
		out.WriteString("\nИсточник: <a href=\"" + html.EscapeString(a.URL) + "\">" + html.EscapeString(a.SourceName) + "</a>")
	}
	return out.String()
}

func renderStoryPreview(out *strings.Builder, story Story, articles map[string]Article, siteBaseURL string) {
	primary := articles[story.PrimaryArticleID]
	out.WriteString("• " + story.Title + "\n" + articleURL(siteBaseURL, primary.ID) + "\n" + story.Summary + "\n")
	for _, id := range story.ArticleIDs {
		a := articles[id]
		out.WriteString("Источник: " + a.SourceName + " — " + a.URL + "\n")
	}
}

func packPlainParts(blocks []plainBlock, editionURL string) ([]string, error) {
	footer := `<a href="` + html.EscapeString(editionURL) + `">Полный выпуск</a>`
	parts := make([]string, 0, 1)
	current := ""
	currentCategory := ""
	for _, block := range blocks {
		prefix, nextCategory := categoryPrefix(block, currentCategory)
		piece := prefix + block.html
		candidate := piece
		if current != "" {
			candidate = current + "\n\n" + piece
		}
		if visibleCount(candidate+"\n\n"+footer) <= MaxPlainPart {
			current = candidate
			currentCategory = nextCategory
			continue
		}
		if current == "" {
			return nil, fmt.Errorf("render_story_too_large")
		}
		parts = append(parts, current+"\n\n"+footer)
		prefix, nextCategory = categoryPrefix(block, "")
		piece = prefix + block.html
		if visibleCount(piece+"\n\n"+footer) > MaxPlainPart {
			return nil, fmt.Errorf("render_story_too_large")
		}
		current = piece
		currentCategory = nextCategory
	}
	if current != "" {
		parts = append(parts, current+"\n\n"+footer)
	}
	return parts, nil
}

func categoryPrefix(block plainBlock, currentCategory string) (prefix, nextCategory string) {
	if block.categoryID == "" {
		return "", ""
	}
	if block.categoryID != currentCategory {
		return "<b>" + html.EscapeString(block.categoryName) + "</b>\n", block.categoryID
	}
	return "", block.categoryID
}

func articleURLForID(id string) string {
	return "/article/" + url.PathEscape(id)
}

func articleURL(base, id string) string {
	return strings.TrimRight(base, "/") + articleURLForID(id)
}

func russianDateLabel(value string) (string, error) {
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return "", err
	}
	months := [...]string{"января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"}
	return fmt.Sprintf("%d %s %d", parsed.Day(), months[parsed.Month()-1], parsed.Year()), nil
}

func visibleCount(s string) int {
	text := html.UnescapeString(generatedHTMLTags.ReplaceAllString(s, ""))
	return len(utf16.Encode([]rune(text)))
}
