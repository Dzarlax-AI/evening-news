package transport

import (
	"context"
	"sync"
	"time"
)

type pageMonitorDiagnosticKey struct{}

// PageMonitorDiagnostic carries request-local, bounded diagnostic metadata.
// It is internal to the process and never changes the extractor wire contract.
type PageMonitorDiagnostic struct {
	Token    string
	SourceID int64
	Started  time.Time

	mu     sync.Mutex
	stage  string
	reason string
}

func NewPageMonitorDiagnostic(token string, sourceID int64) *PageMonitorDiagnostic {
	return &PageMonitorDiagnostic{Token: token, SourceID: sourceID, Started: time.Now()}
}

func WithPageMonitorDiagnostic(ctx context.Context, diagnostic *PageMonitorDiagnostic) context.Context {
	return context.WithValue(ctx, pageMonitorDiagnosticKey{}, diagnostic)
}

// RecordPageMonitorDiagnostic keeps the first terminal native failure only.
func RecordPageMonitorDiagnostic(ctx context.Context, stage, reason string) {
	diagnostic, _ := ctx.Value(pageMonitorDiagnosticKey{}).(*PageMonitorDiagnostic)
	if diagnostic == nil {
		return
	}
	diagnostic.mu.Lock()
	defer diagnostic.mu.Unlock()
	if diagnostic.stage == "" {
		diagnostic.stage, diagnostic.reason = stage, reason
	}
}

func (d *PageMonitorDiagnostic) Details() (stage, reason string) {
	if d == nil {
		return "", ""
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stage, d.reason
}
