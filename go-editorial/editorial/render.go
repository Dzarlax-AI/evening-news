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

var generatedHTMLTags = regexp.MustCompile(`</?(?:b|i|a)(?: href="[^"]*")?>`)

type briefGroup struct {
	categoryID   string
	categoryName string
	stories      []Story
}

type plainBlock struct {
	html          string
	categoryID    string
	categoryName  string
	dividerBefore bool
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
	labels := renderLabels(locale)
	dateLabel, err := localizedDateLabel(e.Date, locale)
	if err != nil {
		return Rendered{}, fmt.Errorf("invalid_edition_date")
	}
	metadata := fmt.Sprintf(labels.materials, len(s.Articles))
	if len(e.Excluded) > 0 {
		metadata += fmt.Sprintf(labels.excluded, len(e.Excluded))
	}
	editionHeading := "Evening News · " + dateLabel

	var rich strings.Builder
	var preview strings.Builder
	blocks := []plainBlock{{html: "<b>" + html.EscapeString(editionHeading) + "</b>\n" + html.EscapeString(metadata)}, {html: "<b>" + labels.main + "</b>"}}
	rich.WriteString("<h2>" + html.EscapeString(editionHeading) + "</h2><p>" + html.EscapeString(metadata) + "</p><h2>" + labels.main + "</h2>")
	preview.WriteString(editionHeading + "\n" + metadata + "\n" + labels.main + "\n")
	if len(mains) == 0 {
		rich.WriteString("<p>" + labels.noMain + "</p>")
		blocks = append(blocks, plainBlock{html: labels.noMain})
		preview.WriteString(labels.noMain + "\n")
	}
	for _, story := range mains {
		renderStoryRich(&rich, story, articles, e.SiteBaseURL, false, locale)
		renderStoryPreview(&preview, story, articles, e.SiteBaseURL, false, locale)
		blocks = append(blocks, plainBlock{html: storyPlainHTML(story, articles, e.SiteBaseURL, false, locale)})
	}

	if len(briefGroups) > 0 {
		rich.WriteString("<h2>" + labels.briefs + "</h2>")
		preview.WriteString("\n" + labels.briefs + "\n")
		blocks = append(blocks, plainBlock{html: "<b>" + labels.briefs + "</b>"})
		for _, group := range briefGroups {
			rich.WriteString("<h3>" + html.EscapeString(group.categoryName) + "</h3>")
			for i, story := range group.stories {
				if i > 0 {
					rich.WriteString("<hr>")
					preview.WriteString("\n────────\n")
				}
				renderStoryRich(&rich, story, articles, e.SiteBaseURL, true, locale)
				renderStoryPreview(&preview, story, articles, e.SiteBaseURL, true, locale)
				blocks = append(blocks, plainBlock{html: storyPlainHTML(story, articles, e.SiteBaseURL, true, locale), categoryID: group.categoryID, categoryName: group.categoryName, dividerBefore: i > 0})
			}
		}
	}
	rich.WriteString("<hr><p><a href=\"" + html.EscapeString(e.EditionURL) + "\">" + labels.full + "</a></p>")
	preview.WriteString("\n" + labels.full + ": " + e.EditionURL)
	parts, err := packPlainParts(blocks, e.EditionURL, locale)
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

func renderStoryRich(out *strings.Builder, story Story, articles map[string]Article, siteBaseURL string, brief bool, locales ...string) {
	primary := articles[story.PrimaryArticleID]
	internal := articleURL(siteBaseURL, primary.ID, locales...)
	if brief {
		out.WriteString("<p>• <a href=\"" + html.EscapeString(internal) + "\">" + html.EscapeString(story.Summary) + "</a>")
		if sources := storySourceNames(story, articles); len(sources) > 0 {
			out.WriteString("<br><em>" + escapeJoined(sources) + "</em>")
		}
		out.WriteString("</p>")
		return
	}
	out.WriteString("<h3><a href=\"" + html.EscapeString(internal) + "\">" + html.EscapeString(story.Title) + "</a></h3>")
	out.WriteString("<p>" + html.EscapeString(story.Summary) + "</p>")
	if sources := storySourceNames(story, articles); len(sources) > 0 {
		out.WriteString("<p>" + sourceLabel(len(sources), locales...) + ": " + escapeJoined(sources) + "</p>")
	}
}

func storyPlainHTML(story Story, articles map[string]Article, siteBaseURL string, brief bool, locales ...string) string {
	primary := articles[story.PrimaryArticleID]
	var out strings.Builder
	if brief {
		out.WriteString("• <a href=\"" + html.EscapeString(articleURL(siteBaseURL, primary.ID, locales...)) + "\">" + html.EscapeString(story.Summary) + "</a>")
		if sources := storySourceNames(story, articles); len(sources) > 0 {
			out.WriteString("\n<i>" + escapeJoined(sources) + "</i>")
		}
		return out.String()
	}
	out.WriteString("<b><a href=\"" + html.EscapeString(articleURL(siteBaseURL, primary.ID, locales...)) + "\">" + html.EscapeString(story.Title) + "</a></b>\n")
	out.WriteString(html.EscapeString(story.Summary))
	if sources := storySourceNames(story, articles); len(sources) > 0 {
		out.WriteString("\n" + sourceLabel(len(sources), locales...) + ": " + escapeJoined(sources))
	}
	return out.String()
}

func renderStoryPreview(out *strings.Builder, story Story, articles map[string]Article, siteBaseURL string, brief bool, locales ...string) {
	primary := articles[story.PrimaryArticleID]
	if brief {
		out.WriteString("• " + story.Summary + " — " + articleURL(siteBaseURL, primary.ID, locales...) + "\n")
		if sources := storySourceNames(story, articles); len(sources) > 0 {
			out.WriteString(strings.Join(sources, ", ") + "\n")
		} else {
			out.WriteByte('\n')
		}
		return
	}
	out.WriteString("• " + story.Title + "\n" + articleURL(siteBaseURL, primary.ID, locales...) + "\n" + story.Summary + "\n")
	if sources := storySourceNames(story, articles); len(sources) > 0 {
		out.WriteString(sourceLabel(len(sources), locales...) + ": " + strings.Join(sources, ", ") + "\n")
	}
}

func storySourceNames(story Story, articles map[string]Article) []string {
	sources := make([]string, 0, len(story.ArticleIDs))
	seen := make(map[string]struct{}, len(story.ArticleIDs))
	for _, id := range story.ArticleIDs {
		name := strings.TrimSpace(articles[id].SourceName)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		sources = append(sources, name)
	}
	return sources
}

func sourceLabel(count int, locales ...string) string {
	if firstLocale(locales) == SerbianLocale {
		if count == 1 {
			return "Izvor"
		}
		return "Izvori"
	}
	if count == 1 {
		return "Источник"
	}
	return "Источники"
}

func escapeJoined(sources []string) string {
	escaped := make([]string, len(sources))
	for i, source := range sources {
		escaped[i] = html.EscapeString(source)
	}
	return strings.Join(escaped, ", ")
}

func packPlainParts(blocks []plainBlock, editionURL string, locales ...string) ([]string, error) {
	footer := `<a href="` + html.EscapeString(editionURL) + `">` + renderLabels(firstLocale(locales)).full + `</a>`
	parts := make([]string, 0, 1)
	current := ""
	currentCategory := ""
	for _, block := range blocks {
		piece, nextCategory := plainPiece(block, currentCategory)
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
		piece, nextCategory = plainPiece(block, "")
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

func plainPiece(block plainBlock, currentCategory string) (piece, nextCategory string) {
	category, nextCategory := categoryPrefix(block, currentCategory)
	divider := ""
	if block.dividerBefore && block.categoryID != "" && block.categoryID == currentCategory {
		divider = "────────\n"
	}
	return category + divider + block.html, nextCategory
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

func articleURL(base, id string, locales ...string) string {
	result := strings.TrimRight(base, "/") + articleURLForID(id)
	if firstLocale(locales) == SerbianLocale {
		result += "?locale=sr-Latn"
	}
	return result
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

type localizedRenderLabels struct{ main, briefs, full, noMain, materials, excluded string }

func firstLocale(locales []string) string {
	if len(locales) > 0 {
		return locales[0]
	}
	return RussianLocale
}
func renderLabels(locale string) localizedRenderLabels {
	if locale == SerbianLocale {
		return localizedRenderLabels{"Glavno", "Još ukratko", "Ceo pregled", "Nema glavnih vesti.", "%d izvornih članaka · Srpski", " · Izostavljeno: %d"}
	}
	return localizedRenderLabels{"Главное", "Ещё коротко", "Полный выпуск", "Главных сюжетов нет.", "%d исходных материалов · Русский", " · Исключено: %d"}
}
func localizedDateLabel(value, locale string) (string, error) {
	if locale != SerbianLocale {
		return russianDateLabel(value)
	}
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return "", err
	}
	months := [...]string{"januar", "februar", "mart", "april", "maj", "jun", "jul", "avgust", "septembar", "oktobar", "novembar", "decembar"}
	return fmt.Sprintf("%d. %s %d.", parsed.Day(), months[parsed.Month()-1], parsed.Year()), nil
}
