package transport

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type cleanupRecorder struct {
	methods  []string
	failures map[string]error
	params   []map[string]any
}

func (r *cleanupRecorder) callCleanup(ctx context.Context, session, method string, params, result any) error {
	r.methods = append(r.methods, method)
	r.params = append(r.params, params.(map[string]any))
	return r.failures[method]
}

func TestSharedLegacyCleanupAtomicallyDisposesKnownContext(t *testing.T) {
	for _, fail := range []error{nil, errors.New("dispose refused")} {
		r := &cleanupRecorder{failures: map[string]error{"Target.disposeBrowserContext": fail}}
		err := disposeOwnedBrowser(context.Background(), r, "owned-context", "owned-target", "owned-session")
		if !errors.Is(err, fail) || !reflect.DeepEqual(r.methods, []string{"Target.disposeBrowserContext"}) || r.params[0]["browserContextId"] != "owned-context" {
			t.Fatalf("known context was closed piecemeal or failure lost: methods=%v err=%v", r.methods, err)
		}
	}
}

func TestSharedLegacyCleanupFallbackUsesOnlyOwnedIDsAndJoinsErrors(t *testing.T) {
	detach, closeTarget := errors.New("detach failed"), errors.New("close failed")
	r := &cleanupRecorder{failures: map[string]error{"Target.detachFromTarget": detach, "Target.closeTarget": closeTarget}}
	err := disposeOwnedBrowser(context.Background(), r, "", "owned-target", "owned-session")
	if !errors.Is(err, detach) || !errors.Is(err, closeTarget) || !reflect.DeepEqual(r.methods, []string{"Target.detachFromTarget", "Target.closeTarget"}) {
		t.Fatalf("fallback failed: %v %v", r.methods, err)
	}
	if r.params[0]["sessionId"] != "owned-session" || r.params[1]["targetId"] != "owned-target" {
		t.Fatal("cleanup addressed unrelated resources")
	}
	if err := disposeOwnedBrowser(context.Background(), &cleanupRecorder{}, "", "", ""); err != nil {
		t.Fatal(err)
	}
}
