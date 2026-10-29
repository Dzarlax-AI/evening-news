package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-worker/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/executor"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/store"
)

var ErrLeaseLost = errors.New("worker lease lost")
var ErrExecutorGone = errors.New("accepted executor instance exited before completing")
var ErrExecutorAuth = errors.New("executor authentication failed; worker stopped fail-closed")

const (
	leaseAcquireWait     = 20 * time.Second
	leaseAcquireInterval = time.Second
)

type Worker struct {
	config   config.Config
	store    QueueStore
	executor *executor.Client
	policy   QueuePolicy
	logger   *slog.Logger
	onReady  func()
}

type QueuePolicy interface {
	KnownKind(string) bool
	MayDeliver(kind string, payload json.RawMessage) bool
}

type QueueStore interface {
	CheckSchema(context.Context) error
	Acquire(context.Context) (store.LeaseInfo, error)
	Release(context.Context) error
	Heartbeat(context.Context) error
	Prune(context.Context, time.Duration, int) (int64, error)
	FailExhaustedInterrupted(context.Context, int64, int) (int64, error)
	Claim(context.Context, int64) (store.Job, error)
	ActiveJobs(context.Context) ([]store.Job, error)
	TerminalJobsNeedingEvents(context.Context, int) ([]store.Job, error)
	DatabaseNow(context.Context) (time.Time, error)
	Info() store.LeaseInfo
	Finish(context.Context, store.Job, string, map[string]any, *string) error
	FinishRecovered(context.Context, store.Job, string, map[string]any, *string) error
}

func New(c config.Config, db QueueStore, policy QueuePolicy, logger *slog.Logger) (*Worker, error) {
	return NewWithReady(c, db, policy, logger, nil)
}

func NewWithReady(c config.Config, db QueueStore, policy QueuePolicy, logger *slog.Logger, onReady func()) (*Worker, error) {
	client, err := executor.New(c.ExecutorURL, c.ExecutorToken, c.RequestTimeout)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, errors.New("queue policy is required")
	}
	return &Worker{config: c, store: db, executor: client, policy: policy, logger: logger, onReady: onReady}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.store.CheckSchema(ctx); err != nil {
		return err
	}
	info, err := w.acquireLease(ctx)
	if err != nil {
		return err
	}
	w.logger.Info("worker lease acquired", "generation", info.Generation, "backend_pid", info.BackendPID)
	defer w.store.Release(context.Background())

	heartbeatCtx, cancelHeartbeat := context.WithCancel(context.Background())
	defer cancelHeartbeat()
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	heartbeatLost := make(chan struct{})
	go w.heartbeatLoop(heartbeatCtx, cancelRun, heartbeatLost)
	claimCtx, cancelClaims := context.WithCancel(runCtx)
	defer cancelClaims()
	stopShutdown := context.AfterFunc(ctx, cancelClaims)
	defer stopShutdown()

	if err := w.recoverBeforeClaim(claimCtx); err != nil {
		return err
	}
	if err := w.replayFinalizedEvents(claimCtx); err != nil {
		return err
	}
	if err := w.executor.Recovered(claimCtx, info.OwnerID, info.Generation); err != nil {
		return fmt.Errorf("Python recovery adapter unavailable: %w", err)
	}
	if w.onReady != nil {
		w.onReady()
	}

	pruneTimer := time.NewTicker(time.Hour)
	defer pruneTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-heartbeatLost:
			return ErrLeaseLost
		case <-pruneTimer.C:
			if _, err := w.store.Prune(claimCtx, w.config.JobRetention, 100); err != nil && !errors.Is(err, store.ErrLeaseLost) {
				w.logger.Warn("job retention pass failed", "error", err)
			}
		default:
			if _, err := w.store.FailExhaustedInterrupted(claimCtx, int64(w.config.MaxJobAttempts), 100); err != nil {
				if errors.Is(err, store.ErrLeaseLost) {
					return ErrLeaseLost
				}
				w.logger.Warn("interrupted job recovery pass failed", "error", err)
			}
			if err := w.replayFinalizedEvents(claimCtx); err != nil {
				w.logger.Warn("legacy completion replay scan failed", "error", err)
			}
			job, err := w.store.Claim(claimCtx, int64(w.config.MaxJobAttempts))
			if errors.Is(err, store.ErrNoJob) {
				if err := sleep(claimCtx, w.config.PollInterval); err != nil {
					if runCtx.Err() != nil {
						return ErrLeaseLost
					}
					return nil
				}
				continue
			}
			if err != nil {
				if errors.Is(err, store.ErrLeaseLost) {
					return ErrLeaseLost
				}
				w.logger.Warn("job claim failed", "error", err)
				if err := sleep(claimCtx, w.config.PollInterval); err != nil {
					if runCtx.Err() != nil {
						return ErrLeaseLost
					}
					return nil
				}
				continue
			}
			if err := w.runJobWithDrain(ctx, runCtx, job); err != nil {
				if errors.Is(err, ErrExecutorAuth) {
					return err
				}
				if errors.Is(err, store.ErrLeaseLost) || errors.Is(err, ErrLeaseLost) {
					return ErrLeaseLost
				}
				w.logger.Warn("job attempt did not reach a confirmed outcome", "job_id", job.ID, "error", err)
				// A running row remains the durable gate. Never claim another job
				// while the Python receipt or its cancellation is unresolved.
				for ctx.Err() == nil && runCtx.Err() == nil {
					if err := sleep(claimCtx, w.config.PollInterval); err != nil {
						break
					}
					if err := w.recoverJob(ctx, job); err == nil {
						break
					} else if errors.Is(err, store.ErrLeaseLost) {
						return ErrLeaseLost
					} else {
						w.logger.Warn("job outcome is still unresolved", "job_id", job.ID, "error", err)
					}
				}
			}
			if runCtx.Err() != nil {
				return ErrLeaseLost
			}
		}
	}
}

func (w *Worker) acquireLease(ctx context.Context) (store.LeaseInfo, error) {
	return acquireLeaseWithRetry(ctx, w.store, w.logger, leaseAcquireWait, leaseAcquireInterval)
}

func acquireLeaseWithRetry(ctx context.Context, db interface {
	Acquire(context.Context) (store.LeaseInfo, error)
}, logger *slog.Logger, maxWait, retryInterval time.Duration) (store.LeaseInfo, error) {
	if maxWait <= 0 || retryInterval <= 0 {
		return store.LeaseInfo{}, errors.New("worker lease wait configuration is invalid")
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, maxWait)
	defer cancel()
	deadline := time.NewTimer(maxWait)
	defer deadline.Stop()
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	announced := false
	for {
		info, err := db.Acquire(deadlineCtx)
		if err == nil {
			return info, nil
		}
		if !errors.Is(err, store.ErrLeaseHeld) {
			if ctx.Err() != nil {
				return store.LeaseInfo{}, ctx.Err()
			}
			return store.LeaseInfo{}, err
		}
		if !announced && logger != nil {
			logger.Info("worker lease held; waiting for current owner", "max_wait_seconds", int(maxWait.Seconds()))
			announced = true
		}
		select {
		case <-ctx.Done():
			return store.LeaseInfo{}, ctx.Err()
		case <-deadline.C:
			return store.LeaseInfo{}, store.ErrLeaseHeld
		case <-ticker.C:
			if errors.Is(deadlineCtx.Err(), context.DeadlineExceeded) {
				return store.LeaseInfo{}, store.ErrLeaseHeld
			}
		}
	}
}

func (w *Worker) runJobWithDrain(shutdownCtx, leaseCtx context.Context, job store.Job) error {
	attemptCtx, cancelAttempt := context.WithCancel(context.Background())
	jobDone := make(chan struct{})
	go func() {
		select {
		case <-shutdownCtx.Done():
			timer := time.NewTimer(w.config.ShutdownDrain)
			defer timer.Stop()
			select {
			case <-jobDone:
			case <-leaseCtx.Done():
				cancelAttempt()
			case <-timer.C:
				cancelAttempt()
			}
		case <-leaseCtx.Done():
			cancelAttempt()
		case <-jobDone:
		}
	}()
	err := w.runJob(attemptCtx, job)
	close(jobDone)
	cancelAttempt()
	return err
}

func (w *Worker) heartbeatLoop(ctx context.Context, cancelRun context.CancelFunc, lost chan<- struct{}) {
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatCtx, cancel := context.WithTimeout(context.Background(), w.config.RequestTimeout)
			var once sync.Once
			var failureAt time.Time
			notifyLoss := func() { once.Do(func() { failureAt = time.Now().UTC(); cancelRun(); close(lost) }) }
			var err error
			if notifying, ok := w.store.(interface {
				HeartbeatWithFailure(context.Context, func()) error
			}); ok {
				err = notifying.HeartbeatWithFailure(heartbeatCtx, notifyLoss)
			} else {
				err = w.store.Heartbeat(heartbeatCtx)
			}
			cancel()
			if err != nil {
				notifyLoss()
				var heartbeatErr *store.HeartbeatError
				if errors.As(err, &heartbeatErr) {
					w.logger.Error("worker heartbeat failed; lease released",
						"event", "worker_heartbeat_failure",
						"event_utc", failureAt.Format(time.RFC3339Nano),
						"stage", heartbeatErr.Stage,
						"reason", heartbeatErr.Reason,
						"sqlstate", heartbeatErr.SQLState,
						"backend_pid", heartbeatErr.BackendPID,
						"generation", heartbeatErr.Generation,
						"mutex_wait_ms", heartbeatErr.MutexWait.Milliseconds(),
						"begin_ms", heartbeatErr.BeginDuration.Milliseconds(),
						"update_ms", heartbeatErr.UpdateDuration.Milliseconds(),
						"commit_ms", heartbeatErr.CommitDuration.Milliseconds())
				} else {
					w.logger.Error("worker heartbeat failed; lease released", "event", "worker_heartbeat_failure", "event_utc", failureAt.Format(time.RFC3339Nano), "stage", "unknown", "reason", "other")
				}
				return
			}
		}
	}
}

func (w *Worker) runJob(ctx context.Context, job store.Job) error {
	if !w.policy.KnownKind(job.Kind) {
		code := "invalid_job"
		return w.store.Finish(ctx, job, "failed", map[string]any{"status": "incomplete"}, &code)
	}
	var policy executor.Policy
	for {
		var err error
		timeoutPolicy, err := w.executor.Policy(ctx, job.ID)
		if err == nil {
			policy = timeoutPolicy
			break
		}
		if staleExecutorError(err) {
			return ErrLeaseLost
		}
		if executorAuthError(err) {
			return fmt.Errorf("%w: %v", ErrExecutorAuth, err)
		}
		if permanentExecutorError(err) {
			if !definitiveJobValidationError(err) {
				return err
			}
			code := "invalid_job"
			writeCtx, cancel := activeWriteContext(ctx, w.config.RequestTimeout)
			defer cancel()
			return w.store.Finish(writeCtx, job, "failed", map[string]any{"status": "incomplete"}, &code)
		}
		if err := sleep(ctx, w.config.PollInterval); err != nil {
			code := "worker_interrupted"
			writeCtx, cancel := activeWriteContext(ctx, w.config.RequestTimeout)
			defer cancel()
			return w.store.Finish(writeCtx, job, "interrupted", map[string]any{"status": "warning"}, &code)
		}
	}
	startedAt, err := w.store.DatabaseNow(ctx)
	if err != nil {
		return err
	}
	deadline := startedAt.Add(time.Duration(policy.TimeoutSeconds) * time.Second)
	info := w.store.Info()
	envelope := executor.Envelope{
		Version: 1, JobID: job.ID, OwnerID: info.OwnerID,
		Generation: info.Generation, AttemptGeneration: job.AttemptGeneration,
		DeadlineAt: deadline,
	}
	receipt, err := w.acceptAndWait(ctx, job, envelope, deadline, w.store.DatabaseNow)
	if err != nil {
		if errors.Is(err, ErrExecutorGone) {
			state, result, code := w.recoveredOutcome(job)
			writeCtx, cancel := activeWriteContext(ctx, w.config.RequestTimeout)
			defer cancel()
			return w.store.Finish(writeCtx, job, state, result, &code)
		}
		if permanentExecutorError(err) {
			if executorAuthError(err) {
				return fmt.Errorf("%w: %v", ErrExecutorAuth, err)
			}
			if !definitiveJobValidationError(err) {
				return err
			}
			code := "invalid_job"
			writeCtx, cancel := activeWriteContext(ctx, w.config.RequestTimeout)
			defer cancel()
			return w.store.Finish(writeCtx, job, "failed", map[string]any{"status": "incomplete"}, &code)
		}
		return err
	}
	persistCtx, cancel := activeWriteContext(ctx, w.config.RequestTimeout)
	defer cancel()
	return w.persistReceipt(persistCtx, job, receipt, false)
}

func activeWriteContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithTimeout(context.Background(), timeout)
}

func (w *Worker) acceptAndWait(ctx context.Context, job store.Job, envelope executor.Envelope, deadline time.Time,
	databaseNow func(context.Context) (time.Time, error)) (out executor.Receipt, outErr error) {
	defer func() {
		// All cancellation exits converge here so no new attempt can be
		// claimed until the accepted Python task is stopped or the bounded
		// cleanup window ends unresolved.
		if ctx.Err() != nil && outErr != nil {
			resolved, err := w.cancelAfterContextExit(job, envelope, deadline)
			if err == nil {
				out, outErr = resolved, nil
			} else {
				outErr = err
			}
		}
	}()
	var postErr error
	var receipt executor.Receipt
	postAttempted := false
	postMayHaveAccepted := false
	postRejectedAfterAmbiguous := false
	for {
		if ctx.Err() != nil {
			return executor.Receipt{}, ctx.Err()
		}
		readCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
		current, err := w.executor.Receipt(readCtx, job.ID, job.AttemptGeneration)
		cancel()
		if err == nil {
			if !sameEnvelope(current, envelope) {
				return executor.Receipt{}, errors.New("existing receipt does not match this attempt envelope")
			}
			receipt = current
		} else if errors.Is(err, executor.ErrNotFound) {
			if !postRejectedAfterAmbiguous {
				postCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
				postAttempted = true
				postErr = w.executor.Accept(postCtx, envelope)
				cancel()
				if postErr == nil {
					postMayHaveAccepted = true
				} else if permanentExecutorError(postErr) {
					if definitiveJobValidationError(postErr) && postMayHaveAccepted {
						// A previous POST may still commit. This later rejection
						// cannot resolve that earlier ambiguous acceptance.
						postRejectedAfterAmbiguous = true
					} else {
						return executor.Receipt{}, fmt.Errorf("executor rejected attempt: %w", postErr)
					}
				} else {
					// A transport failure or server error may have happened after
					// the durable receipt commit.
					postMayHaveAccepted = true
				}
			}
		} else if permanentExecutorError(err) {
			if !postAttempted && receipt.State == "" {
				return executor.Receipt{}, err
			}
			// Once a POST was attempted, a later auth/schema failure cannot
			// prove that the durable accept did not commit. Keep the running job
			// as the gate until receipt polling or confirmed cancellation resolves it.
		}
		if receipt.State != "" {
			if terminal(receipt.State) {
				return receipt, nil
			}
			// The Python singleton guard permits only one executor process to
			// own this database. A newly ready instance therefore proves that
			// the receipt's old executor and its tasks have exited.
			statusCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
			status, statusErr := w.executor.Status(statusCtx)
			cancel()
			if statusErr == nil && status.InstanceID != receipt.ExecutorInstance {
				return executor.Receipt{}, ErrExecutorGone
			}
			now, clockErr := databaseNow(ctx)
			if clockErr != nil {
				if ctx.Err() != nil {
					return executor.Receipt{}, ctx.Err()
				}
				return executor.Receipt{}, clockErr
			}
			if !now.Before(deadline) {
				return w.cancelAfterContextExit(job, envelope, deadline)
			}
		}
		now, clockErr := databaseNow(ctx)
		if clockErr != nil {
			if ctx.Err() != nil {
				return executor.Receipt{}, ctx.Err()
			}
			return executor.Receipt{}, clockErr
		}
		if !now.Before(deadline) {
			return w.cancelAfterContextExit(job, envelope, deadline)
		}
		if err := sleep(ctx, minDuration(w.config.PollInterval, deadline.Sub(now))); err != nil {
			if ctx.Err() != nil {
				return executor.Receipt{}, err
			}
			return executor.Receipt{}, err
		}
	}
}

func (w *Worker) cancelAfterContextExit(job store.Job, envelope executor.Envelope, deadline time.Time) (executor.Receipt, error) {
	cleanupTimeout := 2 * w.config.RequestTimeout
	cleanupCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	return w.cancelUntilResolved(cleanupCtx, job, envelope, deadline)
}

func (w *Worker) cancelUntilResolved(ctx context.Context, job store.Job, envelope executor.Envelope, deadline time.Time) (executor.Receipt, error) {
	for {
		callCtx, cancel := context.WithTimeout(context.Background(), w.config.RequestTimeout)
		status, statusErr := w.executor.Status(callCtx)
		cancel()
		if statusErr == nil {
			callCtx, cancel = context.WithTimeout(context.Background(), w.config.RequestTimeout)
			current, readErr := w.executor.Receipt(callCtx, job.ID, job.AttemptGeneration)
			cancel()
			if readErr == nil && terminal(current.State) {
				return current, nil
			}
			if readErr == nil && current.ExecutorInstance != status.InstanceID {
				return executor.Receipt{}, ErrExecutorGone
			}
			if readErr == nil && sameEnvelope(current, envelope) {
				callCtx, cancel = context.WithTimeout(context.Background(), w.config.RequestTimeout)
				stopped, cancelErr := w.executor.Cancel(callCtx, job.ID, job.AttemptGeneration)
				cancel()
				if cancelErr == nil && stopped.ExecutionStopped && stopped.CurrentInstance == status.InstanceID {
					if terminal(stopped.State) {
						return stopped, nil
					}
					return executor.Receipt{}, ErrExecutorGone
				}
			}
		}
		// An RPC timeout never proves that Python stopped. Keep the running row
		// and its lease until cancellation or instance replacement is observed.
		if err := sleep(ctx, w.config.PollInterval); err != nil {
			return executor.Receipt{}, err
		}
	}
}

func (w *Worker) recoverBeforeClaim(ctx context.Context) error {
	jobs, err := w.store.ActiveJobs(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := w.recoverJob(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) recoverJob(ctx context.Context, job store.Job) error {
	readCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
	receipt, receiptErr := w.executor.Receipt(readCtx, job.ID, job.AttemptGeneration)
	cancel()
	if receiptErr == nil && terminal(receipt.State) {
		return w.persistReceipt(ctx, job, receipt, true)
	}
	if receiptErr != nil && !errors.Is(receiptErr, executor.ErrNotFound) {
		return fmt.Errorf("cannot resolve previous Python receipt: %w", receiptErr)
	}
	if receiptErr == nil && (receipt.State == "accepted" || receipt.State == "running") {
		callCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
		status, statusErr := w.executor.Status(callCtx)
		cancel()
		if statusErr != nil {
			return fmt.Errorf("cannot identify previous executor instance: %w", statusErr)
		}
		if receipt.ExecutorInstance == status.InstanceID {
			callCtx, cancel = context.WithTimeout(ctx, w.config.RequestTimeout)
			stopped, cancelErr := w.executor.Cancel(callCtx, job.ID, job.AttemptGeneration)
			cancel()
			if cancelErr != nil || !stopped.ExecutionStopped || stopped.CurrentInstance != status.InstanceID {
				return errors.New("previous Python execution is not confirmed stopped")
			}
			if terminal(stopped.State) {
				return w.persistReceipt(ctx, job, stopped, true)
			}
			state, result, code := w.recoveredOutcome(job)
			return w.store.FinishRecovered(ctx, job, state, result, &code)
		}
		// A different authenticated instance ID proves the accepted executor
		// process exited; its task and child tasks cannot still be running.
	}
	state, result, code := w.recoveredOutcome(job)
	return w.store.FinishRecovered(ctx, job, state, result, &code)
}

func (w *Worker) recoveredOutcome(job store.Job) (string, map[string]any, string) {
	if w.policy.MayDeliver(job.Kind, job.Payload) {
		return "delivery_unknown", map[string]any{"status": "warning"}, "external_outcome_unknown"
	}
	if job.AttemptGeneration >= int64(w.config.MaxJobAttempts) {
		return "failed", map[string]any{"status": "incomplete"}, "worker_recovery_exhausted"
	}
	return "interrupted", map[string]any{"status": "warning"}, "worker_interrupted"
}

func (w *Worker) persistReceipt(ctx context.Context, job store.Job, receipt executor.Receipt, recovered bool) error {
	state := receipt.State
	if state == "accepted" || state == "running" {
		return errors.New("Python execution receipt is still active")
	}
	if state == "interrupted" {
		state = "interrupted"
	}
	var result map[string]any
	if len(receipt.Result) > 0 {
		if err := json.Unmarshal(receipt.Result, &result); err != nil {
			return errors.New("Python receipt result is malformed")
		}
	}
	if result == nil {
		result = map[string]any{"status": "warning"}
	}
	var code *string
	if receipt.ErrorCode != nil {
		code = receipt.ErrorCode
	} else if state != "succeeded" {
		fallback := "worker_error"
		code = &fallback
	}
	var err error
	if recovered {
		err = w.store.FinishRecovered(ctx, job, state, result, code)
	} else {
		err = w.store.Finish(ctx, job, state, result, code)
	}
	if err != nil {
		return err
	}
	if state != "interrupted" {
		callCtx, cancel := context.WithTimeout(context.Background(), w.config.RequestTimeout)
		defer cancel()
		if err := w.executor.Finalized(callCtx, job.ID, job.AttemptGeneration); err != nil {
			w.logger.Warn("Python legacy completion event will replay", "job_id", job.ID, "error", err)
		}
	}
	return nil
}

func (w *Worker) replayFinalizedEvents(ctx context.Context) error {
	jobs, err := w.store.TerminalJobsNeedingEvents(ctx, 100)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		callCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
		err := w.executor.Finalized(callCtx, job.ID, job.AttemptGeneration)
		cancel()
		if err != nil {
			w.logger.Warn("legacy completion event replay failed", "job_id", job.ID, "error", err)
		}
	}
	return nil
}

func sameEnvelope(receipt executor.Receipt, envelope executor.Envelope) bool {
	if receipt.JobID != envelope.JobID || receipt.OwnerID != envelope.OwnerID ||
		receipt.Generation != envelope.Generation || receipt.AttemptGeneration != envelope.AttemptGeneration {
		return false
	}
	deadline, err := time.Parse(time.RFC3339Nano, receipt.DeadlineAt)
	return err == nil && deadline.Equal(envelope.DeadlineAt)
}

func terminal(state string) bool {
	switch state {
	case "succeeded", "failed", "interrupted", "delivery_unknown":
		return true
	default:
		return false
	}
}

func permanentExecutorError(err error) bool {
	var httpErr *executor.HTTPError
	if !errors.As(err, &httpErr) {
		return false
	}
	switch httpErr.Status {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound,
		http.StatusRequestEntityTooLarge, http.StatusUnprocessableEntity:
		return true
	default:
		return false
	}
}

func executorAuthError(err error) bool {
	var httpErr *executor.HTTPError
	return errors.As(err, &httpErr) && (httpErr.Status == http.StatusUnauthorized || httpErr.Status == http.StatusForbidden)
}

func definitiveJobValidationError(err error) bool {
	var httpErr *executor.HTTPError
	return errors.As(err, &httpErr) && httpErr.Status == http.StatusUnprocessableEntity
}

func staleExecutorError(err error) bool {
	var httpErr *executor.HTTPError
	return errors.As(err, &httpErr) && httpErr.Status == http.StatusConflict
}

func sleep(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if b <= 0 || a < b {
		return a
	}
	return b
}
