package processing

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

func TestOperationalSnapshotReadFailureStopsBeforeProcessingDatabaseAccess(t *testing.T) {
	wantErr := errors.New("pinned snapshot missing")
	calls := 0
	p := &Processor{LoadOperationalSnapshot: func(context.Context, store.Fence) (OperationalSnapshot, error) {
		calls++
		return OperationalSnapshot{}, wantErr
	}}
	_, err := p.Run(context.Background(), store.Fence{})
	if err == nil || !strings.Contains(err.Error(), "operational_configuration") || !errors.Is(err, wantErr) {
		t.Fatalf("Run error=%v, want operational configuration failure wrapping snapshot error", err)
	}
	if calls != 1 {
		t.Fatalf("snapshot reads=%d, want exactly one", calls)
	}
	// DB is intentionally nil: any CanonicalJob, recovery, or selection access
	// before returning the snapshot error would panic this test.
}

func TestOperationalBoundsRemainAnchoredToPinnedAt(t *testing.T) {
	pinnedAt := time.Date(2026, 10, 29, 15, 45, 0, 0, time.UTC)
	snapshot := OperationalSnapshot{
		PinnedAt:      pinnedAt,
		LimitsEnabled: true,
		MaxArticles:   120,
		PerSource:     8,
		Days:          3,
		NewestDate:    "2026-10-30",
	}
	for attempt := 0; attempt < 2; attempt++ {
		cycle, source, from, to := operationalBounds(snapshot)
		if cycle != 120 || source != 8 || from == nil || to == nil {
			t.Fatalf("attempt %d bounds cycle=%d source=%d from=%v to=%v", attempt, cycle, source, from, to)
		}
		wantFrom := time.Date(2026, 10, 26, 15, 45, 0, 0, time.UTC)
		wantTo := time.Date(2026, 10, 30, 23, 59, 59, 999999999, time.UTC)
		if !from.Equal(wantFrom) || !to.Equal(wantTo) {
			t.Fatalf("attempt %d bounds from=%s to=%s, want %s through %s", attempt, from, to, wantFrom, wantTo)
		}
	}
	cycle, source, from, to := operationalBounds(OperationalSnapshot{LimitsEnabled: false})
	if cycle != 0 || source != 0 || from != nil || to != nil {
		t.Fatalf("disabled limits retained bounds: cycle=%d source=%d from=%v to=%v", cycle, source, from, to)
	}
}
