package processing

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

const (
	filterMinContent = 10
	filterMaxContent = 50000
	filterMinTitle   = 10
)

var (
	pythonLower = cases.Lower(language.Und)

	metadataMinuteRead  = regexp.MustCompile(`\p{Nd}+[\p{Z}\t\n\r\f\v]+min[\p{Z}\t\n\r\f\v]+read`)
	metadataMonthDay    = regexp.MustCompile(`(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[\p{Z}\t\n\r\f\v]+\p{Nd}{2}`)
	metadataDateRead    = regexp.MustCompile(`^[\p{Z}\t\n\r\f\v]*[\p{L}\p{N}_]{3}[\p{Z}\t\n\r\f\v]+\p{Nd}{2}[\p{Z}\t\n\r\f\v]+.*\p{Nd}+[\p{Z}\t\n\r\f\v]+min[\p{Z}\t\n\r\f\v]+read[\p{Z}\t\n\r\f\v]*$`)
	metadataSubscribe   = regexp.MustCompile(`subscribe[\p{Z}\t\n\r\f\v]+(?:now|here|today)`)
	metadataShare       = regexp.MustCompile(`share[\p{Z}\t\n\r\f\v]+(?:this|on|now)`)
	metadataShareRu     = "poделиться"
	metadataSubscribeRu = "подписаться"
	metadataNavPrefix   = []*regexp.Regexp{
		regexp.MustCompile(`^[\p{Z}\t\n\r\f\v]*(?:home|главная|news|новости|about|о нас|contact|контакты)[\p{Z}\t\n\r\f\v]*$`),
		regexp.MustCompile(`^[\p{Z}\t\n\r\f\v]*(?:menu|меню|navigation|навигация)[\p{Z}\t\n\r\f\v]*`),
		regexp.MustCompile(`^[\p{Z}\t\n\r\f\v]*(?:cookie|куки)[\p{Z}\t\n\r\f\v]+(?:policy|политика)`),
		regexp.MustCompile(`^[\p{Z}\t\n\r\f\v]*(?:privacy|конфиденциальность)[\p{Z}\t\n\r\f\v]+(?:policy|политика)`),
		regexp.MustCompile(`^[\p{Z}\t\n\r\f\v]*(?:terms|условия)[\p{Z}\t\n\r\f\v]+(?:of service|использования)`),
		regexp.MustCompile(`^[\p{Z}\t\n\r\f\v]*(?:404|error|ошибка)[\p{Z}\t\n\r\f\v]*`),
	}
	metadataSpamPhrasePatterns = []string{
		"click here", "кликни здесь", "жми сюда", "buy now", "купи сейчас", "заказать сейчас",
		"free download", "бесплатно скачать", "limited time", "ограниченное время", "act now", "действуй сейчас",
	}
	personalServicePatterns = []*regexp.Regexp{
		regexp.MustCompile(`я[\p{Z}\t\n\r\f\v]+программист`),
		regexp.MustCompile(`мой[\p{Z}\t\n\r\f\v]+опыт`),
		regexp.MustCompile(`моя[\p{Z}\t\n\r\f\v]+компания`),
		regexp.MustCompile(`предлагаю[\p{Z}\t\n\r\f\v]+услуги`),
		regexp.MustCompile(`обращайтесь[\p{Z}\t\n\r\f\v]+по[\p{Z}\t\n\r\f\v]+телефону`),
		regexp.MustCompile(`консультаци[ия][\p{Z}\t\n\r\f\v]+по[\p{Z}\t\n\r\f\v]+телефону`),
		regexp.MustCompile(`специализируюсь`),
		regexp.MustCompile(`оказываю[\p{Z}\t\n\r\f\v]+услуги`),
		regexp.MustCompile(`свяжитесь[\p{Z}\t\n\r\f\v]+со[\p{Z}\t\n\r\f\v]+мной`),
	}
)

var extractionSkipDomains = []string{"t.me", "telegram.me", "twitter.com", "x.com", "instagram.com"}

// shouldProcess ports SmartFilter.should_process_with_ai with duplicate detection
// kept separate so the caller can consult the durable hash_content index.
func shouldProcess(title, content, rawURL, sourceType string, allowExtraction bool) (bool, string) {
	trimmedContent := strings.TrimSpace(content)
	contentLength := utf8.RuneCountInString(trimmedContent)
	if content == "" || contentLength < filterMinContent || contentLength > filterMaxContent {
		return false, "Content too short (" + itoa(utf8.RuneCountInString(content)) + " chars < 10)"
	}
	if utf8.RuneCountInString(content) > filterMaxContent {
		return false, "Content too long (" + itoa(utf8.RuneCountInString(content)) + " chars > 50000)"
	}
	trimmedTitle := strings.TrimSpace(title)
	if title == "" || utf8.RuneCountInString(trimmedTitle) < filterMinTitle {
		return false, "Title too short or low quality (" + itoa(utf8.RuneCountInString(title)) + " chars)"
	}
	text := lower(title + " " + content)
	if isNavigationContent(text) {
		return false, "Navigation/boilerplate content detected"
	}
	if !isTargetLanguage(content) {
		return false, "Content not in target language (Russian/English)"
	}
	if isSpamContent(text) {
		return false, "Spam patterns detected"
	}
	metadata := isMetadataContent(content)
	extractableForFilter := allowExtraction && extractionURLPrefix(rawURL)
	if metadata {
		if extractableForFilter {
			if !hasExtractionSkipDomain(rawURL) {
				// Let the remaining checks run before returning the extraction reason.
			} else {
				return false, "Metadata/low-quality content detected (non-extractable URL)"
			}
		} else {
			return false, "Metadata/low-quality content detected (no extraction possible)"
		}
	}
	if extractableForFilter && !hasExtractionSkipDomain(rawURL) {
		wordCount := len(strings.Fields(content))
		charCount := utf8.RuneCountInString(strings.TrimSpace(content))
		if charCount < 300 && wordCount < 50 {
			titleWords := len(strings.Fields(title))
			if titleWords >= 5 && charCount < 200 {
				return false, "Content suspiciously short (" + itoa(charCount) + " chars, " + itoa(wordCount) + " words) for detailed title (needs extraction)"
			} else if charCount < 150 {
				return false, "Content very short (" + itoa(charCount) + " chars, " + itoa(wordCount) + " words) - likely RSS summary (needs extraction)"
			}
		}
	}
	quality := scoreQuality(title, content, sourceType)
	if metadata && extractableForFilter && !hasExtractionSkipDomain(rawURL) {
		return false, "Metadata/low-quality content detected (needs extraction)"
	}
	threshold := 0.4
	if sourceType == "extraction" {
		threshold = 0.35
	}
	if quality < threshold {
		return false, "Content quality too low (score: " + formatTwoDecimals(quality) + ")"
	}
	return true, "Passed all filters (quality: " + formatTwoDecimals(quality) + ")"
}

func reasonNeedsExtraction(reason string) bool {
	reason = lower(reason)
	return strings.Contains(reason, "needs extraction") ||
		strings.Contains(reason, "content too short") ||
		strings.Contains(reason, "metadata/low-quality content detected")
}

func extractionURLPrefix(rawURL string) bool {
	return strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://")
}

func hasExtractionSkipDomain(rawURL string) bool {
	lowerURL := lower(rawURL)
	for _, domain := range extractionSkipDomains {
		if strings.Contains(lowerURL, domain) {
			return true
		}
	}
	return false
}

func isNavigationContent(text string) bool {
	for _, pattern := range metadataNavPrefix {
		if pattern.MatchString(text) {
			return true
		}
	}
	return false
}

func isTargetLanguage(content string) bool {
	cyrillic, latin := 0, 0
	for _, r := range content {
		if (r >= 'а' && r <= 'я') || (r >= 'А' && r <= 'Я') || r == 'ё' || r == 'Ё' {
			cyrillic++
		} else if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			latin++
		}
	}
	total := cyrillic + latin
	if total < 50 {
		return true
	}
	ratio := float64(cyrillic) / float64(total)
	return ratio >= 0.3 || ratio <= 0.1
}

func isSpamContent(text string) bool {
	for _, phrase := range metadataSpamPhrasePatterns {
		if containsUnicodeWordPhrase(text, phrase) {
			return true
		}
	}
	if strings.Contains(text, "$$$") || regexp.MustCompile(`!{2}!{3,}`).MatchString(text) {
		return true
	}
	return false
}

func isMetadataContent(content string) bool {
	if content == "" {
		return false
	}
	text := lower(strings.TrimSpace(content))
	if metadataMinuteRead.MatchString(text) || metadataMonthDay.MatchString(text) || metadataDateRead.MatchString(text) ||
		matchesPythonWordBoundaryPattern(text, metadataSubscribe) ||
		containsUnicodeWordPhrase(text, metadataSubscribeRu) ||
		matchesPythonWordBoundaryPattern(text, metadataShare) ||
		containsUnicodeWordPhrase(text, metadataShareRu) {
		return true
	}
	if utf8.RuneCountInString(strings.TrimSpace(content)) < 100 {
		words := strings.Fields(content)
		unique := make(map[string]struct{}, len(words))
		for _, word := range words {
			unique[lower(word)] = struct{}{}
		}
		if len(words) > 5 && float64(len(unique))/float64(len(words)) < 0.6 {
			return true
		}
	}
	return false
}

func scoreQuality(title, content, sourceType string) float64 {
	score := 0.5
	if title != "" && utf8.RuneCountInString(strings.TrimSpace(title)) > 20 {
		score += 0.1
	}
	if content != "" {
		sentenceCount := len(regexp.MustCompile(`[.!?]+`).FindAllString(content, -1))
		if sentenceCount >= 3 && sentenceCount <= 50 {
			score += 0.15
		} else if sentenceCount > 50 {
			score += 0.05
		}
		paragraphs := 0
		for _, paragraph := range strings.Split(content, "\n\n") {
			if strings.TrimSpace(paragraph) != "" {
				paragraphs++
			}
		}
		if paragraphs >= 2 {
			score += 0.1
		}
		wordCount := len(strings.Fields(content))
		if wordCount >= 50 && wordCount <= 2000 {
			score += 0.1
		} else if wordCount > 2000 {
			score += 0.05
		}
	}
	switch sourceType {
	case "rss":
		score += 0.05
	case "telegram":
		score += 0.02
	case "extraction":
		score += 0.1
	}
	if containsTenASCIIUpper(content) {
		score -= 0.1
	}
	if len(regexp.MustCompile(`!{2,}`).FindAllString(content, -1)) > 3 {
		score -= 0.1
	}
	text := lower(title + " " + content)
	for _, pattern := range personalServicePatterns {
		if pattern.MatchString(text) {
			score -= 0.3
		}
	}
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}

func containsTenASCIIUpper(s string) bool {
	run := 0
	for _, r := range s {
		if r >= 'A' && r <= 'Z' {
			run++
			if run >= 10 {
				return true
			}
		} else {
			run = 0
		}
	}
	return false
}

func containsUnicodeWordPhrase(text, phrase string) bool {
	start := 0
	for {
		i := strings.Index(text[start:], phrase)
		if i < 0 {
			return false
		}
		begin := start + i
		end := begin + len(phrase)
		leftOK := begin == 0 || !isPythonWordRune(lastRune(text[:begin]))
		rightOK := end == len(text) || !isPythonWordRune(firstRune(text[end:]))
		if leftOK && rightOK {
			return true
		}
		start = begin + 1
		if start >= len(text) {
			return false
		}
	}
}

func isPythonWordRune(r rune) bool { return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r) }
func firstRune(s string) rune      { r, _ := utf8.DecodeRuneInString(s); return r }
func lastRune(s string) rune       { r, _ := utf8.DecodeLastRuneInString(s); return r }

func lower(s string) string { return pythonLower.String(s) }

func formatTwoDecimals(value float64) string {
	return fmt.Sprintf("%.2f", value)
}

func itoa(value int) string { return fmt.Sprintf("%d", value) }

func matchesPythonWordBoundaryPattern(text string, pattern *regexp.Regexp) bool {
	for _, loc := range pattern.FindAllStringIndex(text, -1) {
		leftOK := loc[0] == 0 || !isPythonWordRune(lastRune(text[:loc[0]]))
		rightOK := loc[1] == len(text) || !isPythonWordRune(firstRune(text[loc[1]:]))
		if leftOK && rightOK {
			return true
		}
	}
	return false
}

// DuplicateCache mirrors SmartFilter's 24-hour process-local MD5 hash cache.
// Durable hash_content lookups remain the caller's responsibility.
type DuplicateCache struct {
	mu     sync.Mutex
	recent map[string]duplicateCacheEntry
	window time.Duration
}

type duplicateCacheEntry struct {
	seenAt time.Time
	owner  *int64
}

func NewDuplicateCache() *DuplicateCache {
	return &DuplicateCache{recent: make(map[string]duplicateCacheEntry), window: 24 * time.Hour}
}

// Lookup reports a recent duplicate or returns the content hash for a durable
// lookup. cacheHit distinguishes owner replays from cache misses.
func (c *DuplicateCache) Lookup(content string, articleID int64, now time.Time) (hash string, duplicate, cacheHit bool) {
	if strings.TrimSpace(content) == "" {
		return "", false, false
	}
	if c == nil {
		return contentHash(content), false, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.window <= 0 {
		c.window = 24 * time.Hour
	}
	if c.recent == nil {
		c.recent = make(map[string]duplicateCacheEntry)
	}
	cutoff := now.Add(-c.window)
	for key, entry := range c.recent {
		if !entry.seenAt.After(cutoff) {
			delete(c.recent, key)
		}
	}
	hash = contentHash(content)
	entry, ok := c.recent[hash]
	if !ok {
		return hash, false, false
	}
	if articleID > 0 && entry.owner != nil && *entry.owner == articleID {
		return hash, false, true
	}
	return hash, true, true
}

func (c *DuplicateCache) Remember(hash string, owner *int64, now time.Time) {
	if c == nil || hash == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.recent == nil {
		c.recent = make(map[string]duplicateCacheEntry)
	}
	if c.window <= 0 {
		c.window = 24 * time.Hour
	}
	var copyOwner *int64
	if owner != nil {
		id := *owner
		copyOwner = &id
	}
	c.recent[hash] = duplicateCacheEntry{seenAt: now, owner: copyOwner}
}

func contentHash(content string) string {
	clean := lower(strings.TrimSpace(content))
	sum := md5.Sum([]byte(clean))
	return hex.EncodeToString(sum[:])
}
