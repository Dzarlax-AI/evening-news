package pipeline

import (
	"context"
	"time"
)

const (
	SourceManagerMaxWorkers = 5
	SourceManagerMaxQueue   = 8
)

// SourceJob is one already-validated source operation. Browser jobs share one
// lane; ordinary HTTP jobs may run alongside it.
type SourceJob struct {
	SourceID int64
	Browser  bool
	Budget   time.Duration
	Run      func(context.Context) SourceJobResult
	// RetainResult runs in the worker immediately before a completion enters the
	// manager's retained results. Callers may use it to reserve shared memory.
	RetainResult func(SourceJobResult) SourceJobResult
}

// SourceJobResult keeps the original source identity and reports whether the
// operation ran, failed, or was left queued when the batch was canceled.
type SourceJobResult struct {
	SourceID  int64
	Outcome   string
	Error     string
	Retryable bool
	Value     any
}

type SourceManager struct {
	MaxWorkers int
}

type sourceJobCompletion struct {
	index  int
	result SourceJobResult
}

// Collect dispatches at most five jobs at a time. A job's budget starts at
// dispatch, so time spent waiting for a worker or browser lane is not charged
// against its source operation deadline.
func (m SourceManager) Collect(ctx context.Context, jobs []SourceJob) []SourceJobResult {
	results := make([]SourceJobResult, len(jobs))
	if len(jobs) == 0 {
		return results
	}
	workers := m.MaxWorkers
	if workers <= 0 || workers > SourceManagerMaxWorkers {
		workers = SourceManagerMaxWorkers
	}
	completed := make(chan sourceJobCompletion, len(jobs))
	queued := make([]int, len(jobs))
	for i := range jobs {
		queued[i] = i
	}
	active := 0
	browserActive := false
	finished := 0

	for finished < len(jobs) {
		// Dispatch runnable work, scanning past a waiting browser job so it
		// cannot block RSS/HTTP work behind it.
		for ctx.Err() == nil && active < workers && len(queued) > 0 {
			pick := -1
			for pos, index := range queued {
				if !jobs[index].Browser || !browserActive {
					pick = pos
					break
				}
			}
			if pick < 0 {
				break
			}
			index := queued[pick]
			queued = append(queued[:pick], queued[pick+1:]...)
			job := jobs[index]
			active++
			if job.Browser {
				browserActive = true
			}
			go func(index int, job SourceJob) {
				result := SourceJobResult{SourceID: job.SourceID, Outcome: "failed"}
				finish := func() {
					if job.RetainResult != nil {
						result = job.RetainResult(result)
					}
					completed <- sourceJobCompletion{index: index, result: result}
				}
				budget := job.Budget
				if budget <= 0 {
					budget = 120 * time.Second
				}
				jobCtx, cancel := context.WithTimeout(ctx, budget)
				defer cancel()
				if job.Run == nil {
					result.Error = "unavailable"
					finish()
					return
				}
				func() {
					defer func() {
						if recover() != nil {
							result = SourceJobResult{SourceID: job.SourceID, Outcome: "failed", Error: "source_panic", Retryable: false}
						}
					}()
					result = job.Run(jobCtx)
					result.SourceID = job.SourceID
				}()
				if jobCtx.Err() != nil && result.Outcome != "success" && result.Outcome != "partial" {
					result.Outcome = "failed"
					if jobCtx.Err() == context.DeadlineExceeded {
						result.Error = "deadline_exceeded"
						result.Retryable = true
					} else {
						result.Error = "cancelled"
						result.Retryable = false
					}
				} else if result.Outcome == "" {
					result.Outcome = "success"
				}
				finish()
			}(index, job)
		}

		if len(queued) > 0 && ctx.Err() != nil {
			for _, index := range queued {
				results[index] = SourceJobResult{SourceID: jobs[index].SourceID, Outcome: "deferred", Error: "batch_deadline_exceeded", Retryable: true}
				finished++
			}
			queued = nil
		}
		if active == 0 {
			if len(queued) == 0 {
				break
			}
			continue
		}
		var done sourceJobCompletion
		if ctx.Err() != nil {
			// Once canceled, block on child completion instead of selecting the
			// already-ready Done channel and spinning while owned work drains.
			done = <-completed
		} else {
			select {
			case done = <-completed:
			case <-ctx.Done():
				continue
			}
		}
		results[done.index] = done.result
		active--
		finished++
		if jobs[done.index].Browser {
			browserActive = false
		}
	}
	return results
}
