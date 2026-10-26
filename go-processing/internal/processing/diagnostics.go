package processing

import "errors"

// phaseError carries only a finite phase name across the asynchronous HTTP
// boundary. Its underlying error is never written to service logs.
type phaseError struct {
	phase string
	err   error
}

func (e *phaseError) Error() string { return e.phase + " failed" }
func (e *phaseError) Unwrap() error { return e.err }

func withPhase(phase string, err error) error {
	if err == nil {
		return nil
	}
	return &phaseError{phase: phase, err: err}
}

func diagnosticPhase(err error) string {
	var phase *phaseError
	if errors.As(err, &phase) {
		return phase.phase
	}
	return "processing"
}
