package topicshadow

import (
	"sort"
	"strings"
	"time"
	"unicode"
)

const candidateContextAge = 30 * 24 * time.Hour

// CandidateTopic is the retrieval-time view of a topic. LastMaterialAt is the
// availability time of its latest material, not the article's event date.
type CandidateTopic struct {
	Card           TopicCard
	Status         LifecycleStatus
	LastMaterialAt time.Time
}

// RankCandidates returns relevant cards in a deterministic order. Results for
// limits 4, 6, and 8 are prefixes of the same ordering. Up to two dormant
// topics are reserved among the first four results when relevant dormant
// topics exist.
func RankCandidates(article ArticleInput, candidates []CandidateTopic, asOf time.Time, limit int) []TopicCard {
	if limit <= 0 || asOf.IsZero() {
		return nil
	}
	terms, articleText := articleTerms(article)
	type ranked struct {
		topic CandidateTopic
		score int
	}
	items := make([]ranked, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	cutoff := asOf.Add(-candidateContextAge)
	for _, topic := range candidates {
		id := strings.TrimSpace(topic.Card.ID)
		if id == "" {
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		if !topic.LastMaterialAt.IsZero() && (topic.LastMaterialAt.Before(cutoff) || topic.LastMaterialAt.After(asOf)) {
			continue
		}
		score := candidateScore(topic.Card, terms, articleText)
		if score == 0 {
			continue
		}
		seen[id] = struct{}{}
		items = append(items, ranked{topic: topic, score: score})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		return items[i].topic.Card.ID < items[j].topic.Card.ID
	})

	// Construct the first-four reservation once, then append all remaining
	// ranked topics. This keeps every requested K as a prefix of one ordering.
	first := make([]ranked, 0, 4)
	used := make(map[string]struct{}, len(items))
	appendGroup := func(dormant bool, max int) {
		added := 0
		for _, item := range items {
			isDormant := item.topic.Status == LifecycleDormant
			if isDormant != dormant || added == max {
				continue
			}
			first = append(first, item)
			used[item.topic.Card.ID] = struct{}{}
			added++
		}
	}
	appendGroup(false, 2)
	appendGroup(true, 2)
	if len(first) < 4 {
		for _, item := range items {
			if len(first) == 4 {
				break
			}
			if _, ok := used[item.topic.Card.ID]; ok {
				continue
			}
			if item.topic.Status == LifecycleDormant {
				continue
			}
			first = append(first, item)
			used[item.topic.Card.ID] = struct{}{}
		}
	}
	ordered := append([]ranked(nil), first...)
	for _, item := range items {
		if _, ok := used[item.topic.Card.ID]; !ok {
			ordered = append(ordered, item)
		}
	}
	if limit > len(ordered) {
		limit = len(ordered)
	}
	result := make([]TopicCard, 0, limit)
	for _, item := range ordered[:limit] {
		card := item.topic.Card
		card.Aliases = append([]string(nil), card.Aliases...)
		result = append(result, card)
	}
	return result
}

func articleTerms(article ArticleInput) (map[string]struct{}, string) {
	text := strings.Join([]string{article.Title, article.Summary, article.SerbianTitle, article.SerbianSummary}, " ")
	terms := tokenize(text)
	return terms, " " + normalizedPhrase(text) + " "
}

func candidateScore(card TopicCard, articleTerms map[string]struct{}, articleText string) int {
	all := append([]string{card.Title}, card.Aliases...)
	cardTerms := make(map[string]struct{})
	phraseMatch := false
	for _, alias := range all {
		normalized := normalizedPhrase(alias)
		if normalized != "" && strings.Contains(articleText, " "+normalized+" ") {
			phraseMatch = true
		}
		for term := range tokenize(alias) {
			cardTerms[term] = struct{}{}
		}
	}
	overlap := 0
	for term := range cardTerms {
		if _, ok := articleTerms[term]; ok {
			overlap++
		}
	}
	if overlap == 0 {
		return 0
	}
	score := overlap
	if phraseMatch {
		score += 4
	}
	return score
}

func tokenize(text string) map[string]struct{} {
	result := make(map[string]struct{})
	for _, term := range strings.Fields(normalizedPhrase(text)) {
		if len([]rune(term)) < 3 || candidateStopWords[term] {
			continue
		}
		result[term] = struct{}{}
	}
	return result
}

func normalizedPhrase(text string) string {
	parts := strings.FieldsFunc(normalizedText(text), func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) })
	for i, part := range parts {
		parts[i] = conservativeStem(part)
	}
	return strings.Join(parts, " ")
}

func normalizedText(text string) string {
	text = strings.ToLower(text)
	var out strings.Builder
	for _, r := range text {
		if replacement, ok := candidateTransliteration[r]; ok {
			out.WriteString(replacement)
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

func conservativeStem(term string) string {
	// Strip only common inflection endings, and only when a useful lexical stem
	// of at least four runes remains. This is deliberately not a lemmatizer.
	for _, suffix := range candidateSuffixes {
		if !strings.HasSuffix(term, suffix) {
			continue
		}
		stem := strings.TrimSuffix(term, suffix)
		if len([]rune(stem)) >= 4 {
			return stem
		}
	}
	return term
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

var candidateTransliteration = map[rune]string{
	'а': "a", 'б': "b", 'в': "v", 'г': "g", 'д': "d", 'ђ': "dj", 'е': "e", 'ё': "yo",
	'ж': "zh", 'з': "z", 'и': "i", 'й': "j", 'ј': "j", 'к': "k", 'л': "l", 'љ': "lj", 'м': "m",
	'н': "n", 'њ': "nj", 'о': "o", 'п': "p", 'р': "r", 'с': "s", 'т': "t", 'ћ': "c",
	'у': "u", 'ф': "f", 'х': "h", 'ц': "c", 'ч': "c", 'џ': "dz", 'ш': "s", 'щ': "shh",
	'ы': "y", 'э': "e", 'ю': "yu", 'я': "ya", 'ъ': "", 'ь': "",
	'č': "c", 'ć': "c", 'đ': "dj", 'š': "s", 'ž': "zh",
}

var candidateStopWords = map[string]bool{
	"and": true, "the": true, "for": true, "with": true, "from": true,
	"za": true, "na": true, "pri": true, "iz": true, "dlya": true, "od": true, "i": true, "u": true, "do": true, "po": true,
}

var candidateSuffixes = []string{
	"ovima", "evima", "ijama", "ami", "yami", "ima", "ama", "ove", "ovi", "evi", "ova", "eva", "om", "em",
	"ostyu", "eniya", "enie", "ogo", "emu", "ymi", "imi", "aya", "yaya", "oye", "eye", "ov", "ev",
	"ami", "y", "a", "e", "i", "u",
}
