package topicshadow

import "time"

// LifecycleStatus is the persisted lifecycle state of a topic candidate.
type LifecycleStatus string

const (
	LifecycleCandidate LifecycleStatus = "candidate"
	LifecycleActive    LifecycleStatus = "active"
	LifecycleDormant   LifecycleStatus = "dormant"
	DefaultQuietAfter                  = 168 * time.Hour
)

// LifecycleState contains only the fields needed to make lifecycle decisions.
// Callers may persist it alongside the topic and journal in one transaction.
type LifecycleState struct {
	CandidateID             string
	Status                  LifecycleStatus
	FirstAvailableAt        time.Time
	OpenedAt                time.Time
	LastMaterialAvailableAt time.Time
	QuietedAt               time.Time
	MaterialCount           int
}

// LifecycleDecision is an explicit, already-reviewed decision for one item.
// AvailableAt is when the item/decision became usable; EventAt is the reported
// event time and may be earlier. Repeat, Context, and Review never refresh a
// topic. Stale/background admission is represented by MaterialUpdate=false.
type LifecycleDecision struct {
	CandidateID              string
	AvailableAt              time.Time
	EventAt                  time.Time
	FactID                   string
	SourceIDs                []string
	MaterialUpdate           bool
	Repeat                   bool
	Context                  bool
	Review                   bool
	ConcreteStatusChange     bool
	IndependentMaterialCount int
}

// LifecycleJournalEntry records one actual transition. EventAt is retained
// independently from AvailableAt so late facts do not move historical timing.
type LifecycleJournalEntry struct {
	Kind          string
	CandidateID   string
	AvailableAt   time.Time
	EventAt       time.Time
	FactID        string
	SourceIDs     []string
	Reason        string
	MaterialCount int `json:",omitempty"`
}

type LifecycleResult struct {
	State   LifecycleState
	Journal []LifecycleJournalEntry
}

// ApplyLifecycle applies one accepted decision. quietAfter<=0 selects the
// 168-hour default. Caller must supply deterministic UTC (or consistently
// normalized) AvailableAt values and serialize/persist the returned state and
// journal atomically. Candidate counts are caller-provided independent facts.
func ApplyLifecycle(state LifecycleState, d LifecycleDecision, quietAfter time.Duration) LifecycleResult {
	if quietAfter <= 0 {
		quietAfter = DefaultQuietAfter
	}
	if state.CandidateID == "" {
		state.CandidateID = d.CandidateID
	}
	if state.Status == "" {
		state.Status = LifecycleCandidate
	}
	result := LifecycleResult{State: state}
	if d.AvailableAt.IsZero() || d.CandidateID == "" || state.CandidateID != d.CandidateID || d.Review || d.Repeat || d.Context || !d.MaterialUpdate {
		return result
	}

	state = result.State
	if state.FirstAvailableAt.IsZero() {
		state.FirstAvailableAt = d.AvailableAt
	}
	state.MaterialCount++
	if state.Status == LifecycleCandidate {
		if !d.ConcreteStatusChange && d.IndependentMaterialCount < 2 {
			result.State = state
			return result
		}
		state.Status = LifecycleActive
		state.OpenedAt = d.AvailableAt
		state.LastMaterialAvailableAt = d.AvailableAt
		reason := "one_material"
		if !d.ConcreteStatusChange {
			reason = "multiple_materials"
		}
		result.State = state
		opened := journalEntry("opened", d, reason)
		opened.MaterialCount = state.MaterialCount
		result.Journal = []LifecycleJournalEntry{opened}
		return result
	}

	if state.Status == LifecycleDormant {
		state.Status = LifecycleActive
		state.LastMaterialAvailableAt = d.AvailableAt
		state.QuietedAt = time.Time{}
		result.State = state
		result.Journal = []LifecycleJournalEntry{journalEntry("returned", d, "new_material_update")}
		return result
	}
	if state.Status == LifecycleActive {
		state.LastMaterialAvailableAt = d.AvailableAt
		result.State = state
		result.Journal = []LifecycleJournalEntry{journalEntry("updated", d, "material_update")}
		return result
	}
	return result
}

// ExpireLifecycle performs the no-article expiry pass. At exactly quietAfter
// since the last material became available, an active topic becomes dormant.
func ExpireLifecycle(state LifecycleState, availableAt time.Time, quietAfter time.Duration) LifecycleResult {
	if quietAfter <= 0 {
		quietAfter = DefaultQuietAfter
	}
	result := LifecycleResult{State: state}
	if state.Status != LifecycleActive || state.LastMaterialAvailableAt.IsZero() || availableAt.Before(state.LastMaterialAvailableAt.Add(quietAfter)) {
		return result
	}
	state.Status = LifecycleDormant
	state.QuietedAt = state.LastMaterialAvailableAt.Add(quietAfter)
	result.State = state
	result.Journal = []LifecycleJournalEntry{{Kind: "quieted", CandidateID: state.CandidateID, AvailableAt: state.QuietedAt, EventAt: state.QuietedAt, Reason: "quiet_window_elapsed"}}
	return result
}

func journalEntry(kind string, d LifecycleDecision, reason string) LifecycleJournalEntry {
	return LifecycleJournalEntry{Kind: kind, CandidateID: d.CandidateID, AvailableAt: d.AvailableAt, EventAt: d.EventAt, FactID: d.FactID, SourceIDs: append([]string(nil), d.SourceIDs...), Reason: reason}
}
