package transport

import (
	"context"
	"errors"
)

type ownedBrowserCaller interface {
	callCleanup(context.Context, string, string, any, any) error
}

// disposeOwnedBrowser closes a known disposable context atomically. Explicitly
// closing its target before disposing the context races on pinned Cloak builds.
// The fallback is only for IDs already recorded as owned without a context ID.
func disposeOwnedBrowser(ctx context.Context, client ownedBrowserCaller, contextID, targetID, sessionID string) error {
	if contextID != "" {
		return client.callCleanup(ctx, "", "Target.disposeBrowserContext", map[string]any{"browserContextId": contextID}, nil)
	}
	var cleanupErr error
	if sessionID != "" {
		cleanupErr = client.callCleanup(ctx, "", "Target.detachFromTarget", map[string]any{"sessionId": sessionID}, nil)
	}
	if targetID != "" {
		cleanupErr = errors.Join(cleanupErr, client.callCleanup(ctx, "", "Target.closeTarget", map[string]any{"targetId": targetID}, nil))
	}
	return cleanupErr
}
