package analysis

import (
	"strings"
)

func buildPrompt(r Request) string {
	categories := make([]string, 0, len(r.Categories))
	names := make([]string, 0, len(r.Categories))
	for _, c := range r.Categories {
		name := c.Name + " (" + c.DisplayName + ")"
		if len(c.Examples) > 0 {
			name += " — e.g.: " + strings.Join(c.Examples, ", ")
		}
		categories = append(categories, name)
		names = append(names, c.Name)
	}
	lc := r.LocalCategory
	if lc == "" {
		lc = r.ConfiguredLocalCategory
	}
	localBlock := ""
	if lc != "" && contains(names, lc) {
		localBlock = `
LOCAL CATEGORY RULE (CRITICAL):
- If the article is specifically about ` + lc + ` (local events, government, economy, cities, daily life,
  local companies, local people) — use ONLY "` + lc + `" as the category.
- Do NOT combine "` + lc + `" with other categories like Business or Tech.
  Local economic news = "` + lc + `", not "` + lc + ` + Business".
- This is important because some readers filter out "` + lc + `" — mixing it with other categories pollutes those feeds.
- Exception: only add a second category if the article has GLOBAL significance beyond ` + lc + `.
`
	}
	content := runePreview(r.Content, 3500)
	original := ""
	contentOriginalSeparator := ""
	if r.OriginalContext != nil && strings.TrimSpace(*r.OriginalContext) != "" {
		contentOriginalSeparator = "\n"
		original = `

ORIGINAL SOURCE TEXT (from RSS feed or Telegram post):
` + runePreview(*r.OriginalContext, 2000) + `

⚠️ IMPORTANT: The "Content" above was extracted from the URL. The "Original Source Text" is the text
from the RSS feed or Telegram channel that linked to this URL. If the extracted content appears to be
an error page, cookie consent page, privacy policy, login page, or is completely unrelated to the
original source text — IGNORE the extracted content and base your analysis entirely on the Original
Source Text instead. The Original Source Text is the reliable source of information about the actual news.
`
	}
	separator := "\n\n"
	if original != "" {
		separator = "\n"
	}
	localExample := "\n"
	if r.ConfiguredLocalCategory != "" {
		localExample = `
- Local news: "categories": ["` + r.ConfiguredLocalCategory + `"], "category_confidences": [0.95] — NOT ["` + r.ConfiguredLocalCategory + `", "Business"]`
	}
	return `Analyze this article and provide complete analysis in JSON format.

🇷🇺 ВАЖНО: ВСЕ результаты анализа должны быть на РУССКОМ языке!

ARTICLE INFORMATION:
Title: ` + r.Title + `
URL: ` + r.URL + `
Source: ` + sourceContext(r.URL) + `
Content: ` + content + contentOriginalSeparator + original + separator + `ANALYSIS TASKS:
1. TITLE OPTIMIZATION: Create clear, informative headline (max 120 characters for Telegram)
2. CATEGORIZATION: Choose from available categories below` + "  " + `
3. SUMMARIZATION: Create accurate summary in Russian - adapt length to content (see rules below)
4. ADVERTISEMENT DETECTION: Determine if content is promotional
5. DATE EXTRACTION: Find publication date if mentioned

AVAILABLE CATEGORIES: ` + strings.Join(categories, ", ") + `

CATEGORIZATION RULES:
- Pick 1-2 categories from AVAILABLE CATEGORIES above. Use EXACT names from the list.
- Do NOT invent new category names. Only use: ` + strings.Join(names, ", ") + `
- "Other" is a LAST RESORT — only use if no other category fits at all. NEVER use "Other" as a second category.
- Maximum 2 categories per article. If only 1 fits well, use 1.
` + "\n" + localBlock + `

ADVERTISEMENT vs BUSINESS:
- Promotional posts with discounts, prices, "buy now", special offers → Marketing (NOT Business)
- News about companies, economy, markets, finance → Business
- If it's from a Telegram channel and reads like an ad → Marketing

TITLE OPTIMIZATION RULES:
- ALWAYS provide optimized_title in RUSSIAN, regardless of source language
- Maximum 120 characters for Telegram readability
- Make title clear and informative
- Remove clickbait elements (BREAKING, TOP-5, etc.)
- Fix truncated titles (ending with "..." or incomplete)
- If original is already in Russian and good, you can use it as optimized_title

SUMMARIZATION:
SUMMARIZATION REQUIREMENTS:
🚫 ЗАПРЕЩЕНО: ВЫДУМЫВАТЬ факты, добавлять информацию которой НЕТ в тексте, лить воду

📏 АДАПТИВНАЯ ДЛИНА на основе исходного текста:
- Длинный текст (>800 символов): 5-6 подробных предложений (200+ символов)
- Средний текст (300-800 символов): 3-4 предложения, сохраняя ВСЕ ключевые факты
- Короткий текст (<300 символов): 2-3 предложения, простой пересказ БЕЗ додумывания

📝 ПРАВИЛА ПЕРЕСКАЗА:
- ТОЧНО передавай ТОЛЬКО информацию из текста
- Не добавляй контекст, объяснения, предположения
- Не расширяй факты своими знаниями
- Структура: ЧТО произошло → ДЕТАЛИ из текста → РЕЗУЛЬТАТ/СЛЕДСТВИЯ (если есть в тексте)
- Сохраняй ВСЕ ключевые факты, числа, имена, даты из оригинала
- Избегай повтора заголовка, добавляй информацию из содержания
- Начинай сразу с сути (без "статья рассказывает о...")

🎯 ДЛЯ КОРОТКИХ ТЕКСТОВ:
- Если исходный текст короткий — НЕ растягивай искусственно
- Лучше краткий точный пересказ, чем длинный с выдумками
- Качество фактов важнее количества предложений

ADVERTISEMENT DETECTION:
🚨 CRITICAL: Tech/product announcements from NEWS SOURCES are NOT advertisements!

NEWS vs ADVERTISEMENT distinction:
✅ NEWS ARTICLES (is_advertisement: false):
- Product launches/releases reported by tech news sites
- Company announcements covered by journalism
- Industry analysis and reviews` + "  " + `
- Financial results and business updates
- Research findings and innovations
- Government/regulatory announcements

❌ ADVERTISEMENTS (is_advertisement: true):
- Direct sales offers ("купить", "заказать", "скидка")
- Promotional content with prices and deals
- Marketing materials from companies themselves
- Sponsored content clearly promoting services
- Event/webinar promotional announcements

KEY INDICATORS:
- NEWS source context (tech blogs, news sites) → likely NOT advertisement
- Journalistic tone vs promotional tone
- Third-party reporting vs first-party marketing
- Facts/analysis vs sales pitch

Promotional keywords: "купить", "заказать", "скидка", "акция", "распродажа", "цена от", "успей купить"
Business reporting keywords: "выпустила", "анонсировала", "представила", "запустила" (these are NEWS, not ads!)

DATE EXTRACTION:
Look for publication dates in content, ignore article dates.

OUTPUT FORMAT (JSON):
{
    "optimized_title": "Краткий информативный заголовок новости",
    "categories": ["Business"],
    "category_confidences": [0.95],
    "summary": "Краткий пересказ 5-6 предложений...",
    "summary_confidence": 0.90,
    "is_advertisement": false,
    "ad_type": "news_article",
    "ad_confidence": 0.1,
    "ad_reasoning": "Content focuses on news reporting...",
    "publication_date": "2024-01-15",
    "confidence": 0.85
}

EXAMPLES:
- Single category: "categories": ["Science"], "category_confidences": [0.95]
- Two categories: "categories": ["Tech", "AI"], "category_confidences": [0.9, 0.85]` + localExample + `
- Promotional post: "categories": ["Marketing"], "category_confidences": [0.9], "is_advertisement": true

Answer ONLY with valid JSON, no additional text.`
}

func sourceContext(rawURL string) string {
	for _, domain := range []string{"balkaninsight.com", "biznis.rs", "rts.rs", "b92.net", "politika.rs", "blic.rs", "novosti.rs", "euronews.rs"} {
		if strings.Contains(strings.ToLower(rawURL), domain) {
			return "from a NEWS source"
		}
	}
	return "from an UNKNOWN source"
}

func runePreview(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

func contains(items []string, item string) bool {
	for _, s := range items {
		if s == item {
			return true
		}
	}
	return false
}
