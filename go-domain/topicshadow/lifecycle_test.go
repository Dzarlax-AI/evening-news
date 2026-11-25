package topicshadow

import (
	"testing"
	"time"
)

func TestExpireLifecycleBoundary(t *testing.T) {
	base := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	state := LifecycleState{CandidateID: "topic-1", Status: LifecycleActive, OpenedAt: base, LastMaterialAvailableAt: base}

	before := ExpireLifecycle(state, base.Add(167*time.Hour+59*time.Minute), 0)
	if before.State.Status != LifecycleActive || len(before.Journal) != 0 {
		t.Fatalf("before boundary: got status=%q journal=%d", before.State.Status, len(before.Journal))
	}
	boundary := ExpireLifecycle(state, base.Add(168*time.Hour), 0)
	if boundary.State.Status != LifecycleDormant || len(boundary.Journal) != 1 || boundary.Journal[0].Kind != "quieted" {
		t.Fatalf("at boundary: got status=%q journal=%+v", boundary.State.Status, boundary.Journal)
	}
	if !boundary.State.QuietedAt.Equal(base.Add(168 * time.Hour)) {
		t.Fatalf("quieted at %s, want threshold", boundary.State.QuietedAt)
	}
}

func TestNonMaterialRepeatContextAndReviewDoNotRefresh(t *testing.T) {
	available := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	state := LifecycleState{CandidateID: "topic-1", Status: LifecycleActive, LastMaterialAvailableAt: available}
	for _, decision := range []LifecycleDecision{
		{CandidateID: "topic-1", AvailableAt: available.Add(time.Hour), MaterialUpdate: false},
		{CandidateID: "topic-1", AvailableAt: available.Add(time.Hour), MaterialUpdate: true, Repeat: true},
		{CandidateID: "topic-1", AvailableAt: available.Add(time.Hour), MaterialUpdate: true, Context: true},
		{CandidateID: "topic-1", AvailableAt: available.Add(time.Hour), MaterialUpdate: true, Review: true},
	} {
		result := ApplyLifecycle(state, decision, 0)
		if !result.State.LastMaterialAvailableAt.Equal(available) || len(result.Journal) != 0 {
			t.Fatalf("decision %+v refreshed lifecycle: %+v", decision, result)
		}
	}
}

func TestOpenReturnOnceAndKeepEventTimeSeparate(t *testing.T) {
	available := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	event := available.Add(-72 * time.Hour)
	d := LifecycleDecision{CandidateID: "topic-1", AvailableAt: available, EventAt: event, FactID: "fact-1", SourceIDs: []string{"source-1"}, MaterialUpdate: true, ConcreteStatusChange: true}
	opened := ApplyLifecycle(LifecycleState{}, d, 0)
	if opened.State.Status != LifecycleActive || len(opened.Journal) != 1 || opened.Journal[0].Kind != "opened" {
		t.Fatalf("expected one opening transition, got %+v", opened)
	}
	if !opened.Journal[0].AvailableAt.Equal(available) || !opened.Journal[0].EventAt.Equal(event) {
		t.Fatalf("event and availability timestamps were conflated: %+v", opened.Journal[0])
	}
	// A report cutoff sees the fact only when it became available, even though
	// the event itself is earlier and may be displayed as historical context.
	cutoff := available.Add(-time.Minute)
	if !opened.Journal[0].AvailableAt.After(cutoff) || !opened.Journal[0].EventAt.Before(cutoff) {
		t.Fatalf("late-fact cutoff distinction not represented: %+v", opened.Journal[0])
	}

	dormant := ExpireLifecycle(opened.State, available.Add(DefaultQuietAfter), 0)
	returned := ApplyLifecycle(dormant.State, LifecycleDecision{CandidateID: "topic-1", AvailableAt: available.Add(DefaultQuietAfter + time.Hour), EventAt: event, FactID: "fact-2", MaterialUpdate: true}, 0)
	if returned.State.Status != LifecycleActive || len(returned.Journal) != 1 || returned.Journal[0].Kind != "returned" {
		t.Fatalf("expected one return transition: %+v", returned)
	}
	again := ApplyLifecycle(returned.State, LifecycleDecision{CandidateID: "topic-1", AvailableAt: available.Add(DefaultQuietAfter + 2*time.Hour), MaterialUpdate: true, Repeat: true}, 0)
	if len(again.Journal) != 0 || again.State.Status != LifecycleActive {
		t.Fatalf("repeat must not emit a second return: %+v", again)
	}
}

func TestCandidateNeedsConcreteChangeOrSecondIndependentMaterial(t *testing.T) {
	available := time.Now().UTC()
	one := ApplyLifecycle(LifecycleState{}, LifecycleDecision{CandidateID: "topic-1", AvailableAt: available, MaterialUpdate: true}, 0)
	if one.State.Status != LifecycleCandidate || len(one.Journal) != 0 {
		t.Fatalf("one routine material should remain a candidate: %+v", one)
	}
	two := ApplyLifecycle(one.State, LifecycleDecision{CandidateID: "topic-1", AvailableAt: available.Add(time.Minute), MaterialUpdate: true, IndependentMaterialCount: 2}, 0)
	if two.State.Status != LifecycleActive || len(two.Journal) != 1 || two.Journal[0].Reason != "multiple_materials" {
		t.Fatalf("second independent material should open: %+v", two)
	}
}
