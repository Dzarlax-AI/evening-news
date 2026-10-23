package pipeline

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSourceManagerPreservesOrderAndSerializesBrowserLane(t *testing.T) {
	manager := SourceManager{MaxWorkers: 5}
	var browserActive atomic.Int32
	var maxBrowser atomic.Int32
	var rssActive atomic.Int32
	var maxRSS atomic.Int32
	var startedCount atomic.Int32
	var ready sync.WaitGroup
	ready.Add(3)
	release := make(chan struct{})
	job := func(id int64, browser bool) SourceJob {
		return SourceJob{SourceID: id, Browser: browser, Budget: time.Second, Run: func(ctx context.Context) SourceJobResult {
			if browser {
				active := browserActive.Add(1)
				for old := maxBrowser.Load(); active > old && !maxBrowser.CompareAndSwap(old, active); old = maxBrowser.Load() {
				}
				defer browserActive.Add(-1)
			} else {
				active := rssActive.Add(1)
				for old := maxRSS.Load(); active > old && !maxRSS.CompareAndSwap(old, active); old = maxRSS.Load() {
				}
				defer rssActive.Add(-1)
			}
			if startedCount.Add(1) <= 3 {
				ready.Done()
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return SourceJobResult{SourceID: id, Outcome: "success"}
		}}
	}
	jobs := []SourceJob{job(1, true), job(2, true), job(3, false), job(4, true), job(5, false)}
	done := make(chan []SourceJobResult, 1)
	go func() { done <- manager.Collect(context.Background(), jobs) }()
	readyDone := make(chan struct{})
	go func() { ready.Wait(); close(readyDone) }()
	select {
	case <-readyDone:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("manager did not run browser and RSS jobs concurrently")
	}
	if maxBrowser.Load() != 1 {
		close(release)
		t.Fatalf("browser concurrency = %d, want 1", maxBrowser.Load())
	}
	if maxRSS.Load() < 2 {
		close(release)
		t.Fatalf("RSS concurrency = %d, want at least 2", maxRSS.Load())
	}
	close(release)
	results := <-done
	for i, result := range results {
		if result.SourceID != jobs[i].SourceID || result.Outcome != "success" {
			t.Fatalf("result[%d] = %+v", i, result)
		}
	}
}

func TestSourceManagerStartsBudgetAfterQueueWait(t *testing.T) {
	manager := SourceManager{MaxWorkers: 1}
	started := time.Now()
	jobs := []SourceJob{
		{SourceID: 1, Budget: time.Second, Run: func(ctx context.Context) SourceJobResult {
			time.Sleep(40 * time.Millisecond)
			return SourceJobResult{Outcome: "success"}
		}},
		{SourceID: 2, Budget: 10 * time.Millisecond, Run: func(ctx context.Context) SourceJobResult {
			select {
			case <-ctx.Done():
				return SourceJobResult{Outcome: "failed", Error: "deadline_exceeded"}
			default:
				return SourceJobResult{Outcome: "success"}
			}
		}},
	}
	results := manager.Collect(context.Background(), jobs)
	if elapsed := time.Since(started); elapsed < 40*time.Millisecond {
		t.Fatalf("test did not exercise a queue wait: %s", elapsed)
	}
	if results[1].Outcome != "success" {
		t.Fatalf("queued operation inherited queue wait: %+v", results[1])
	}
}

func TestSourceManagerDefersQueuedWorkAndDrainsActiveWorkOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	jobs := []SourceJob{
		{SourceID: 1, Budget: time.Second, Run: func(ctx context.Context) SourceJobResult {
			close(started)
			<-ctx.Done()
			time.Sleep(25 * time.Millisecond)
			return SourceJobResult{Outcome: "failed", Error: "cancelled"}
		}},
		{SourceID: 2, Budget: time.Second, Run: func(context.Context) SourceJobResult {
			t.Error("queued job started after cancellation")
			return SourceJobResult{Outcome: "success"}
		}},
	}
	done := make(chan []SourceJobResult, 1)
	go func() { done <- (SourceManager{MaxWorkers: 1}).Collect(ctx, jobs) }()
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("manager returned before active child drained")
	case <-time.After(5 * time.Millisecond):
	}
	results := <-done
	if results[0].Outcome != "failed" || results[1].Outcome != "deferred" {
		t.Fatalf("results = %+v", results)
	}
}

func TestSourceManagerCanceledQueuedBrowserDoesNotBlockRetainedRSS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rssRetained := make(chan struct{})
	jobs := []SourceJob{
		{SourceID: 1, Browser: true, Budget: time.Second, Run: func(ctx context.Context) SourceJobResult {
			<-ctx.Done()
			return SourceJobResult{Outcome: "failed", Error: "cancelled"}
		}},
		{SourceID: 2, Browser: true, Budget: time.Second, Run: func(context.Context) SourceJobResult {
			t.Error("queued browser job started")
			return SourceJobResult{Outcome: "success"}
		}, RetainResult: func(result SourceJobResult) SourceJobResult {
			t.Error("queued browser job reserved a result")
			return result
		}},
		{SourceID: 3, Budget: time.Second, Run: func(context.Context) SourceJobResult {
			return SourceJobResult{Outcome: "success", Value: "rss-result"}
		}, RetainResult: func(result SourceJobResult) SourceJobResult {
			close(rssRetained)
			return result
		}},
	}
	done := make(chan []SourceJobResult, 1)
	go func() { done <- (SourceManager{MaxWorkers: 2}).Collect(ctx, jobs) }()
	select {
	case <-rssRetained:
	case <-time.After(time.Second):
		t.Fatal("RSS result did not reach reservation while browser lane was occupied")
	}
	cancel()
	select {
	case results := <-done:
		if results[1].Outcome != "deferred" || results[2].Outcome != "success" {
			t.Fatalf("results = %+v", results)
		}
	case <-time.After(time.Second):
		t.Fatal("queued browser work blocked result collection after cancellation")
	}
}

func TestSourceManagerEmptyBatchAndNilRunner(t *testing.T) {
	if got := (SourceManager{}).Collect(context.Background(), nil); len(got) != 0 {
		t.Fatalf("empty batch returned %d results", len(got))
	}
	got := (SourceManager{}).Collect(context.Background(), []SourceJob{{SourceID: 3}})
	if len(got) != 1 || got[0].Outcome != "failed" || got[0].Error != "unavailable" {
		t.Fatalf("nil runner result = %+v", got)
	}
}
