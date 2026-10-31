package digestworkflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGenerationContextBudgetAndEarlierJobDeadline(t *testing.T) {
	started := time.Now()
	ctx, cancel := generationContext(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok || deadline.Sub(started) < 179*time.Second || deadline.Sub(started) > 181*time.Second {
		t.Fatal("digest generation must have a bounded three-minute budget")
	}
	jobDeadline := time.Now().Add(20 * time.Millisecond)
	job, stop := context.WithDeadline(context.Background(), jobDeadline)
	defer stop()
	short, finish := generationContext(job)
	defer finish()
	if got, ok := short.Deadline(); !ok || !got.Equal(jobDeadline) {
		t.Fatal("generation expanded the job deadline")
	}
	<-short.Done()
	if !errors.Is(short.Err(), context.DeadlineExceeded) {
		t.Fatal("job deadline did not cancel generation")
	}
	canceled, abort := context.WithCancel(context.Background())
	abort()
	child, cleanup := generationContext(canceled)
	defer cleanup()
	if !errors.Is(child.Err(), context.Canceled) {
		t.Fatal("generation lost parent cancellation")
	}
}
