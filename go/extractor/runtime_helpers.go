package extractor

// NormalizeArticleURL exposes the offline URL admission used by the network
// orchestrator before any transport work. It does not grant network permission.
func NormalizeArticleURL(raw string) (string, bool) {
	cleaned := cleanURL(raw)
	return cleaned, validURL(cleaned)
}

// HasIncompleteTable identifies a static widget worth one bounded browser attempt.
func HasIncompleteTable(markup string) bool {
	tables := parse(markup).Find("article table,main table")
	for i := range tables.Nodes {
		if tables.Eq(i).Find("td,th").Length() == 0 {
			return true
		}
	}
	return false
}

// HasPopulatedTableCounterpart keeps the offline identity check authoritative.
func HasPopulatedTableCounterpart(static, rendered string) bool {
	return hasPopulatedTableCounterpart(parse(static), parse(rendered))
}
