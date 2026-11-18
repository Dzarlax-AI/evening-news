package processing

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

func TestDedupFingerprintPreservesMaterialDetails(t *testing.T) {
	const report = "The city closed the bridge on Monday at 8 a.m. and directed traffic through the northern tunnel until the structural inspection was complete."
	exact := dedupObservation(report)
	if exact.ExactHash == nil || exact.Version != DedupFingerprintVersion {
		t.Fatal("eligible source text must yield a versioned fingerprint")
	}
	whitespace := strings.ReplaceAll(report, " ", "\r\n\t ")
	if got := dedupObservation(whitespace); !bytes.Equal(got.ExactHash, exact.ExactHash) {
		t.Fatal("the same source text with different spacing and title must match")
	}
	for _, material := range []string{"Tuesday", "Sunday"} {
		updated := strings.Replace(report, "Monday", material, 1)
		if got := dedupObservation(updated); got.ExactHash == nil || bytes.Equal(got.ExactHash, exact.ExactHash) {
			t.Fatal("material date change must produce a different exact fingerprint")
		}
	}
	if got := dedupObservation("Short feed teaser"); got.ExactHash != nil {
		t.Fatal("short feed teaser must not become an exact fingerprint")
	}
}

func TestDedupTrialRanksRelatedStoriesAndPreservesUpdates(t *testing.T) {
	brief := store.DedupArticle{ID: 1,
		SourceTitle: "Nvidia says it does not intend to reinstate Jensen Huang as CEO",
		Title:       "Nvidia не намерена возвращать Дженсена Хуанга на пост гендиректора",
		Summary:     "Совет директоров Nvidia принял отставку Дженсена Хуанга и отказался восстанавливать его в должности.",
	}
	candidates := []store.DedupArticle{
		{ID: 2, SourceTitle: "Read everything that Nvidia’s board chair had to say about Jensen Huang", Title: "Nvidia не намерена возвращать Дженсена Хуанга на пост гендиректора", Summary: "Стенограмма звонка Nvidia, детали поиска гендиректора и комментарий Коллетт Кресс."},
		{ID: 3, SourceTitle: brief.SourceTitle, Title: brief.Title, Summary: "Новый гендиректор Nvidia назначен после завершения поиска."},
		{ID: 4, SourceTitle: "Quarterly report confirms a new Nvidia datacenter in Helsinki", Title: brief.Title, Summary: "Nvidia commissioned a datacenter with no change in the chief executive."},
		{ID: 5, SourceTitle: "Odbor kompanije odbio povratak bivšeg direktora", Russian: store.DedupLocalizedText{Title: brief.Title, Summary: brief.Summary}},
		{ID: 6, SourceTitle: "Railway repairs close a rural station", Summary: "Regional services are suspended overnight."},
	}
	calls := 0
	pairs, failed := compareDedupCandidates(context.Background(), brief, candidates, func(_ context.Context, subject store.DedupArticle, peers []store.DedupArticle) (map[int64]DedupRelation, error) {
		calls++
		if subject.ID != brief.ID {
			t.Fatal("comparison called for wrong subject")
		}
		got := make(map[int64]bool)
		for _, peer := range peers {
			got[peer.ID] = true
		}
		if !got[2] || !got[3] || !got[4] || !got[5] || got[6] {
			t.Fatal("same title, independent coverage and localized overlap must be ranked ahead of unrelated stories")
		}
		// Reviewed labels model a transcript worth keeping, a changed
		// outcome, and a second-language report of the original decision.
		return map[int64]DedupRelation{2: DedupUnclear, 3: DedupMaterialUpdate, 4: DedupDifferentEvent, 5: DedupSameEvent, 999: DedupSameEvent}, nil
	})
	if failed || calls != 1 {
		t.Fatalf("expected a single comparison batch, got %d", calls)
	}
	verdicts := make(map[int64]DedupRelation)
	for _, pair := range pairs {
		verdicts[pair.ArticleID] = pair.Relation
	}
	if verdicts[2] != DedupUnclear || verdicts[3] != DedupMaterialUpdate || verdicts[4] != DedupDifferentEvent || verdicts[5] != DedupSameEvent || verdicts[999] != "" {
		t.Fatalf("unexpected trial verdicts: %v", verdicts)
	}
}

func TestDedupTrialBatchesAndFallsBackToUnclear(t *testing.T) {
	now := store.DedupArticle{ID: 1, SourceTitle: "Nvidia reports 25 percent growth"}
	var candidates []store.DedupArticle
	for id := int64(2); id < 2+dedupComparisonLimit+2; id++ {
		candidates = append(candidates, store.DedupArticle{ID: id, SourceTitle: "NVIDIA reports 20 percent growth!"})
	}
	pairs, failed := compareDedupCandidates(context.Background(), now, candidates, func(_ context.Context, _ store.DedupArticle, peers []store.DedupArticle) (map[int64]DedupRelation, error) {
		if len(peers) != dedupComparisonLimit {
			t.Fatalf("comparison batch size = %d", len(peers))
		}
		return map[int64]DedupRelation{2: DedupSameEvent}, errors.New("trial comparator unavailable")
	})
	if !failed || len(pairs) != dedupComparisonLimit {
		t.Fatalf("unexpected failed comparison result: len=%d failed=%v", len(pairs), failed)
	}
	for i, pair := range pairs {
		if pair.Relation != DedupUnclear || !pair.NumberChanged || pair.ArticleID != int64(i+2) {
			t.Fatalf("failed pair should preserve ranking and number signal with unclear verdict: %+v", pair)
		}
	}
}
