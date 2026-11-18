package processing

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

const (
	DedupFingerprintVersion int16 = 1
	DedupTrialVersion             = 1
	DefaultDedupTrialWindow       = 48 * time.Hour
	dedupComparisonLimit          = 6
	dedupTrialTimeout             = time.Second
)

type DedupRelation string

const (
	DedupSameEvent      DedupRelation = "same_event"
	DedupMaterialUpdate DedupRelation = "material_update"
	DedupDifferentEvent DedupRelation = "different_event"
	DedupUnclear        DedupRelation = "unclear"
)

// DedupCompare receives one bounded batch of candidates. A future semantic
// comparator can evaluate the subject, action, object and material details for
// each pair. Missing, invalid or failed results are unclear.
type DedupCompare func(context.Context, store.DedupArticle, []store.DedupArticle) (map[int64]DedupRelation, error)

type DedupPair struct {
	ArticleID     int64
	TitleMatch    bool
	SharedTerms   int
	NumberChanged bool
	Relation      DedupRelation
}

type DedupTrialReport struct {
	ArticleID             int64
	ArticleAttempt        int
	FingerprintVersion    int16
	TrialVersion          int
	Recorded              bool
	ExactRepresentativeID int64
	CandidatesScanned     int
	CandidatePoolFull     bool
	MissingFetchedAt      bool
	ComparisonFailed      bool
	Pairs                 []DedupPair
}

// dedupObservation fingerprints the original or extracted article body.
// Whitespace is folded; case, numbers, punctuation and order are preserved.
// Sparse inputs can still participate in candidate comparisons by title.
func dedupObservation(body string) store.DedupObservation {
	obs := store.DedupObservation{Version: DedupFingerprintVersion}
	words := strings.Fields(body)
	if len(words) < 16 || !utf8.ValidString(body) {
		return obs
	}
	canonical := strings.Join(words, " ")
	if utf8.RuneCountInString(canonical) < 120 || IsChallengeText(canonical) {
		return obs
	}
	hash := sha256.Sum256([]byte(canonical))
	obs.ExactHash = hash[:]
	return obs
}

// runDedupTrial is called after a successful completed outcome commit. The
// caller supplies source text used for this attempt, excluding generated
// summaries and title fallbacks. The result is diagnostic only.
func runDedupTrial(ctx context.Context, db *store.Store, f store.Fence, a store.Article, attempt int, inputFingerprint, originalText string, window time.Duration, compare DedupCompare) (DedupTrialReport, error) {
	report := DedupTrialReport{
		ArticleID: a.ID, ArticleAttempt: attempt,
		FingerprintVersion: DedupFingerprintVersion, TrialVersion: DedupTrialVersion,
	}
	representative, recorded, err := db.RecordDedupObservation(ctx, f, a.ID, attempt, a.ClaimToken, inputFingerprint, dedupObservation(originalText))
	if err != nil || !recorded {
		return report, err
	}
	report.Recorded = true
	report.ExactRepresentativeID = representative
	current, candidates, truncated, err := db.LoadDedupTrial(ctx, a.ID, a.ClaimToken, DedupFingerprintVersion, window)
	if err != nil || current.ID == 0 {
		return report, err
	}
	if current.FetchedAt == nil {
		report.MissingFetchedAt = true
		return report, nil
	}
	report.CandidatesScanned = len(candidates)
	report.CandidatePoolFull = truncated
	report.Pairs, report.ComparisonFailed = compareDedupCandidates(ctx, current, candidates, compare)
	return report, nil
}

// finishAndObserveDedup keeps trial failures independent of the durable article outcome.
func (p *Processor) finishAndObserveDedup(ctx context.Context, f store.Fence, a store.Article, attempt int, inputFingerprint string, outcome Outcome, sourceText string) (Outcome, int, error) {
	saved, savedAttempt, err := p.finish(ctx, f, a, attempt, inputFingerprint, outcome)
	if !p.DedupTrial || err != nil || saved.State != StateCompleted {
		return saved, savedAttempt, err
	}
	if strings.TrimSpace(sourceText) == strings.TrimSpace(a.Title) || EnforceTextLimit(sourceText, p.textLimit()) != nil {
		sourceText = ""
	}
	started := time.Now()
	trialCtx, cancel := context.WithTimeout(ctx, dedupTrialTimeout)
	defer cancel()
	report, trialErr := runDedupTrial(trialCtx, p.DB, f, a, savedAttempt, inputFingerprint, sourceText, DefaultDedupTrialWindow, nil)
	if trialErr != nil {
		slog.WarnContext(ctx, "article_dedup_trial_failed", "article_id", a.ID, "attempt", savedAttempt, "trial_version", DedupTrialVersion, "recorded", report.Recorded)
	} else {
		slog.InfoContext(ctx, "article_dedup_trial", "article_id", report.ArticleID, "attempt", report.ArticleAttempt,
			"fingerprint_version", report.FingerprintVersion, "trial_version", report.TrialVersion, "recorded", report.Recorded,
			"exact_representative_id", report.ExactRepresentativeID, "candidates_scanned", report.CandidatesScanned,
			"pool_truncated", report.CandidatePoolFull, "missing_fetched_at", report.MissingFetchedAt, "pairs", report.Pairs,
			"elapsed_ms", time.Since(started).Milliseconds())
	}
	return saved, savedAttempt, nil
}

type dedupSignals struct {
	titles  []string
	terms   map[string]int
	numbers map[string]bool
}

func buildDedupSignals(a store.DedupArticle) dedupSignals {
	s := dedupSignals{terms: make(map[string]int), numbers: make(map[string]bool)}
	for _, title := range []string{a.SourceTitle, a.Title, a.Russian.Title, a.Serbian.Title} {
		words := dedupWords(title)
		if len(words) != 0 {
			s.titles = append(s.titles, strings.Join(words, " "))
		}
		for _, word := range words {
			if strings.IndexFunc(word, unicode.IsNumber) >= 0 {
				s.numbers[word] = true
			}
			if utf8.RuneCountInString(word) >= 4 || s.numbers[word] {
				s.terms[word] = 3
			}
		}
	}
	for _, summary := range []string{a.Summary, a.Russian.Summary, a.Serbian.Summary} {
		for _, word := range dedupWords(summary) {
			if utf8.RuneCountInString(word) >= 4 || strings.IndexFunc(word, unicode.IsNumber) >= 0 {
				s.terms[word] = max(s.terms[word], 1)
			}
		}
	}
	return s
}

func dedupWords(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && !unicode.IsMark(r)
	})
}

func compareDedupCandidates(ctx context.Context, current store.DedupArticle, candidates []store.DedupArticle, compare DedupCompare) ([]DedupPair, bool) {
	type rankedCandidate struct {
		article store.DedupArticle
		pair    DedupPair
		score   int
	}
	subject := buildDedupSignals(current)
	ranked := make([]rankedCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		signals := buildDedupSignals(candidate)
		pair := DedupPair{ArticleID: candidate.ID, Relation: DedupUnclear}
		for _, title := range subject.titles {
			if slices.Contains(signals.titles, title) {
				pair.TitleMatch = true
				break
			}
		}
		score := 0
		for term, weight := range subject.terms {
			if otherWeight := signals.terms[term]; otherWeight > 0 {
				score += min(weight, otherWeight)
				pair.SharedTerms++
			}
		}
		if !pair.TitleMatch && (pair.SharedTerms < 2 || score < 4) {
			continue
		}
		if len(subject.numbers) > 0 && len(signals.numbers) > 0 {
			pair.NumberChanged = len(subject.numbers) != len(signals.numbers)
			for number := range subject.numbers {
				pair.NumberChanged = pair.NumberChanged || !signals.numbers[number]
			}
		}
		ranked = append(ranked, rankedCandidate{article: candidate, pair: pair, score: score})
	}
	// Keep recently fetched candidates ahead on ties. Store order is stable.
	slices.SortStableFunc(ranked, func(a, b rankedCandidate) int {
		if a.pair.TitleMatch != b.pair.TitleMatch {
			if a.pair.TitleMatch {
				return -1
			}
			return 1
		}
		return b.score - a.score
	})
	ranked = ranked[:min(len(ranked), dedupComparisonLimit)]
	pairs := make([]DedupPair, 0, len(ranked))
	batch := make([]store.DedupArticle, 0, len(ranked))
	for _, row := range ranked {
		pairs = append(pairs, row.pair)
		batch = append(batch, row.article)
	}
	if compare == nil || len(batch) == 0 {
		return pairs, false
	}
	results, err := compare(ctx, current, batch)
	if err != nil {
		return pairs, true
	}
	for i := range pairs {
		switch relation := results[pairs[i].ArticleID]; relation {
		case DedupSameEvent, DedupMaterialUpdate, DedupDifferentEvent:
			pairs[i].Relation = relation
		}
	}
	return pairs, false
}
