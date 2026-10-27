package editorial

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	idPattern         = regexp.MustCompile(`^[1-9][0-9]{0,9}$`)
	categoryPattern   = regexp.MustCompile(`^[1-9][0-9]{0,9}$`)
	ErrInvalidInput   = errors.New("invalid_input")
	ErrInvalidEdition = errors.New("invalid_edition")
)

func ValidateSnapshot(s Snapshot, locale string) (string, error) {
	if locale != RussianLocale {
		return "", fmt.Errorf("%w: unsupported locale", ErrInvalidInput)
	}
	if _, err := time.Parse("2006-01-02", s.Date); err != nil || len(s.Date) != 10 {
		return "", fmt.Errorf("%w: date must be YYYY-MM-DD", ErrInvalidInput)
	}
	_, offset := s.CutoffUTC.Zone()
	if s.CutoffUTC.IsZero() || offset != 0 {
		return "", fmt.Errorf("%w: cutoff_utc must have a UTC offset", ErrInvalidInput)
	}
	if err := validateURL(s.SiteBaseURL, true); err != nil {
		return "", fmt.Errorf("%w: invalid site_base_url", ErrInvalidInput)
	}
	if len(s.SiteBaseURL) > 2048 || len(s.EditionURL) > 2048 || s.EditionURL == "" {
		return "", fmt.Errorf("%w: URL is too long or edition_url is missing", ErrInvalidInput)
	}
	if err := validateURL(s.EditionURL, false); err != nil {
		return "", fmt.Errorf("%w: invalid edition_url", ErrInvalidInput)
	}
	if len(s.Articles) == 0 || len(s.Articles) > 500 {
		return "", fmt.Errorf("%w: article count must be between 1 and 500", ErrInvalidInput)
	}
	seen := make(map[string]struct{}, len(s.Articles))
	for i, a := range s.Articles {
		_, idErr := strconv.ParseInt(a.ID, 10, 32)
		_, categoryErr := strconv.ParseInt(a.CategoryID, 10, 32)
		if !idPattern.MatchString(a.ID) || idErr != nil || !categoryPattern.MatchString(a.CategoryID) || categoryErr != nil || strings.TrimSpace(a.Title) == "" || len(a.Title) > 1000 || strings.TrimSpace(a.SourceName) == "" || len(a.SourceName) > 300 || strings.TrimSpace(a.CategoryName) == "" || len(a.CategoryName) > 200 {
			return "", fmt.Errorf("%w: invalid article at index %d", ErrInvalidInput, i)
		}
		if _, ok := seen[a.ID]; ok {
			return "", fmt.Errorf("%w: duplicate article id", ErrInvalidInput)
		}
		seen[a.ID] = struct{}{}
		if len(a.Summary) > 10000 || len(a.Content) > 30000 || len(a.URL) > 2048 || !categoryPattern.MatchString(a.CategoryID) {
			return "", fmt.Errorf("%w: invalid article metadata", ErrInvalidInput)
		}
		if err := validateURL(a.URL, false); err != nil {
			return "", fmt.Errorf("%w: unsafe article URL", ErrInvalidInput)
		}
	}
	b, err := json.Marshal(s)
	if err != nil {
		return "", fmt.Errorf("%w: snapshot encoding failed", ErrInvalidInput)
	}
	return hash(b), nil
}

func ValidateEdition(s Snapshot, locale string, e Edition) error {
	snapshotHash, err := ValidateSnapshot(s, locale)
	if err != nil {
		return err
	}
	if e.Version != EditionVersion || e.Locale != RussianLocale || e.Locale != locale || e.Date != s.Date || !e.CutoffUTC.Equal(s.CutoffUTC) || e.SiteBaseURL != strings.TrimRight(s.SiteBaseURL, "/") || e.EditionURL != s.EditionURL || e.SourceSnapshotHash != snapshotHash {
		return fmt.Errorf("%w: edition metadata does not match snapshot", ErrInvalidEdition)
	}
	if len(e.Stories) > len(s.Articles) {
		return fmt.Errorf("%w: invalid story count", ErrInvalidEdition)
	}
	articles := make(map[string]Article, len(s.Articles))
	for _, a := range s.Articles {
		articles[a.ID] = a
	}
	assigned := make(map[string]bool, len(articles))
	mainCount := 0
	for i, story := range e.Stories {
		if story.Kind == "main" {
			mainCount++
		} else if story.Kind != "brief" {
			return fmt.Errorf("%w: invalid story kind at index %d", ErrInvalidEdition, i)
		}
		if story.Kind == "main" && mainCount > MaxMainStories {
			return fmt.Errorf("%w: too many main stories", ErrInvalidEdition)
		}
		if len(story.ArticleIDs) == 0 || len(story.ArticleIDs) > 100 || strings.TrimSpace(story.Title) == "" || strings.TrimSpace(story.Summary) == "" || len(story.Title) > MaxStoryText || len(story.Summary) > MaxStoryText {
			return fmt.Errorf("%w: incomplete story", ErrInvalidEdition)
		}
		if story.ID != StoryID(story.Kind, story.ArticleIDs) {
			return fmt.Errorf("%w: unstable story id", ErrInvalidEdition)
		}
		containsPrimary := false
		for _, id := range story.ArticleIDs {
			if _, ok := articles[id]; !ok {
				return fmt.Errorf("%w: unknown article id", ErrInvalidEdition)
			}
			if assigned[id] {
				return fmt.Errorf("%w: article assigned more than once", ErrInvalidEdition)
			}
			assigned[id] = true
			if id == story.PrimaryArticleID {
				containsPrimary = true
			}
		}
		if !containsPrimary {
			return fmt.Errorf("%w: primary article must belong to story", ErrInvalidEdition)
		}
		evidenceByArticle := make(map[string]bool, len(story.Evidence))
		if len(story.Evidence) == 0 || len(story.Evidence) > 100 {
			return fmt.Errorf("%w: missing or excessive evidence", ErrInvalidEdition)
		}
		for _, ev := range story.Evidence {
			a, ok := articles[ev.ArticleID]
			if !ok || !contains(story.ArticleIDs, ev.ArticleID) || strings.TrimSpace(ev.Quote) == "" || utf8.RuneCountInString(ev.Quote) > MaxEvidence {
				return fmt.Errorf("%w: invalid evidence reference", ErrInvalidEdition)
			}
			if !strings.Contains(preferredEvidenceText(a), ev.Quote) {
				return fmt.Errorf("%w: evidence is not an exact source snippet", ErrInvalidEdition)
			}
			if evidenceByArticle[ev.ArticleID] {
				return fmt.Errorf("%w: duplicate evidence reference", ErrInvalidEdition)
			}
			evidenceByArticle[ev.ArticleID] = true
		}
		for _, id := range story.ArticleIDs {
			if !evidenceByArticle[id] {
				return fmt.Errorf("%w: supporting article lacks evidence", ErrInvalidEdition)
			}
		}
	}
	allowedReasons := map[string]bool{"advertisement": true, "not_news": true, "insufficient_content": true}
	for _, x := range e.Excluded {
		if !allowedReasons[x.Reason] || len(x.Reason) > 64 {
			return fmt.Errorf("%w: invalid exclusion reason", ErrInvalidEdition)
		}
		if _, ok := articles[x.ArticleID]; !ok || assigned[x.ArticleID] {
			return fmt.Errorf("%w: invalid exclusion article", ErrInvalidEdition)
		}
		assigned[x.ArticleID] = true
	}
	if len(assigned) != len(articles) {
		return fmt.Errorf("%w: article coverage is incomplete", ErrInvalidEdition)
	}
	for id := range articles {
		if !assigned[id] {
			return fmt.Errorf("%w: article coverage is incomplete", ErrInvalidEdition)
		}
	}
	contentHash, err := editionContentHash(e)
	if err != nil || contentHash != e.ContentHash {
		return fmt.Errorf("%w: content hash mismatch", ErrInvalidEdition)
	}
	return nil
}

func FinalizeEdition(s Snapshot, locale string, stories []Story, excluded []Exclusion) (Edition, error) {
	snapshotHash, err := ValidateSnapshot(s, locale)
	if err != nil {
		return Edition{}, err
	}
	for i := range stories {
		stories[i].ID = StoryID(stories[i].Kind, stories[i].ArticleIDs)
	}
	e := Edition{Version: EditionVersion, Locale: locale, Date: s.Date, CutoffUTC: s.CutoffUTC, SiteBaseURL: strings.TrimRight(s.SiteBaseURL, "/"), EditionURL: s.EditionURL, SourceSnapshotHash: snapshotHash, Stories: stories, Excluded: excluded}
	e.ContentHash, err = editionContentHash(e)
	if err != nil {
		return Edition{}, err
	}
	if err := ValidateEdition(s, locale, e); err != nil {
		return Edition{}, err
	}
	return e, nil
}

func StoryID(kind string, articleIDs []string) string {
	ids := append([]string(nil), articleIDs...)
	// Membership identifies a story regardless of the model's ordering of its sources.
	sortStrings(ids)
	b, _ := json.Marshal(struct {
		Version string   `json:"version"`
		Kind    string   `json:"kind"`
		IDs     []string `json:"article_ids"`
	}{EditionVersion, kind, ids})
	return kind + "-" + hash(b)[:16]
}

func preferredEvidenceText(a Article) string {
	if strings.TrimSpace(a.Content) != "" {
		return a.Content
	}
	if strings.TrimSpace(a.Summary) != "" {
		return a.Summary
	}
	return a.Title
}

func validateURL(raw string, base bool) error {
	u, err := url.Parse(raw)
	if err != nil || u == nil || u.User != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || strings.ContainsAny(raw, "\r\n\x00") {
		return ErrInvalidInput
	}
	if base && (u.RawQuery != "" || u.Fragment != "") {
		return ErrInvalidInput
	}
	return nil
}

func editionContentHash(e Edition) (string, error) {
	e.ContentHash = ""
	b, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return hash(b), nil
}

func hash(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }
func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
