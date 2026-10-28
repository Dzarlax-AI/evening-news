package airuntime

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
)

// DigestInput is the authenticated domain operation and trusted execution
// context required by the existing digest workflow.
type DigestInput struct {
	Operation, OperationID, DomainInstanceID, Fingerprint string
	Input                                                 json.RawMessage
	Envelope                                              authority.Envelope
	Fence                                                 authority.Fence
}

func (s *Server) ExecuteDigest(ctx context.Context, in DigestInput) (map[string]any, error) {
	if s.draining.Load() {
		return nil, errors.New("runtime_draining")
	}
	e := in.Envelope
	v := digestInput{Operation: in.Operation, OperationID: in.OperationID, DomainInstanceID: in.DomainInstanceID, Fingerprint: in.Fingerprint, Input: in.Input, Envelope: digestEnvelope{Version: e.Version, JobID: e.JobID, Attempt: e.AttemptGeneration, OwnerID: e.OwnerID, Generation: e.Generation, BackendPID: e.BackendPID, ExecutorID: e.ExecutorInstanceID, Deadline: e.DeadlineAt}, Kind: in.Fence.Kind, Payload: in.Fence.Payload, ExecutorBackendPID: in.Fence.ExecutorBackendPID}
	deadline, ok := validDigest(v)
	if !ok {
		return nil, errors.New("invalid_input")
	}
	if in.Fence.Pool == nil || in.Fence.Pool != s.db || in.Fence.JobID != e.JobID || in.Fence.OwnerID != e.OwnerID || in.Fence.ExecutorInstanceID != e.ExecutorInstanceID || in.Fence.AttemptGeneration != e.AttemptGeneration || in.Fence.Generation != e.Generation || in.Fence.WorkerBackendPID != e.BackendPID || !in.Fence.Deadline.Equal(deadline) {
		return nil, authority.ErrStale
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	stop := make(chan struct{})
	go func() {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		case <-stop:
		}
	}()
	defer func() { close(stop); cancel() }()
	return s.executeDigest(ctx, v, in.Fence)
}
func DigestErrorCode(err error) string { return digestErrorCode(err) }
