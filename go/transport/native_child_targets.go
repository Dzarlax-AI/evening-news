package transport

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/chromedp/cdproto"
)

const nativePausedTargetLimit = 32

// nativeEventFailure adds a finite reason without retaining publisher metadata.
type nativeEventFailure struct {
	reason string
	cause  error
}

func (e *nativeEventFailure) Error() string { return e.cause.Error() }
func (e *nativeEventFailure) Unwrap() error { return e.cause }

// nativeEventReason recovers a finite event classification from a wrapped native failure.
func nativeEventReason(err error) string {
	var event *nativeEventFailure
	if errors.As(err, &event) {
		return event.reason
	}
	return ""
}

// nativePausedTargets is confined to one render's event consumer. Service
// workers remain paused; owned frames receive guards before any resume.
type nativePausedTargets struct {
	context, target, session string
	targets                  map[string]*nativeChildTarget
	sessions                 map[string]*nativeChildTarget
}

type nativeChildTarget struct {
	id, session, parent, kind string
	frame                     atomic.Value
	active, detaching, gone   bool
}

// frameID reads the frame identity published after guarded child-session setup.
func (t *nativeChildTarget) frameID() string {
	if value := t.frame.Load(); value != nil {
		return value.(string)
	}
	return ""
}

// configureFrame installs recursive request guards and verifies frame ownership before resuming
// the paused frame.
func (t *nativeChildTarget) configureFrame(ctx context.Context, client *browserCDP, mainFrame string) error {
	failure := func(err error) error {
		return &nativeEventFailure{"child_frame_guard_failed", errors.Join(ErrNativePolicy, ErrNativeOwnershipLost, err)}
	}
	for _, command := range []struct {
		method string
		params any
	}{
		{"Page.enable", map[string]any{}},
		{"Target.setAutoAttach", map[string]any{"autoAttach": true, "waitForDebuggerOnStart": true, "flatten": true}},
		{"Network.enable", map[string]any{"maxPostDataSize": 1}},
		{"Network.setBlockedURLs", map[string]any{"urls": []string{"http://*", "ws://*", "wss://*", "file://*", "ftp://*"}}},
		{"Fetch.enable", map[string]any{"handleAuthRequests": true, "patterns": nativeFetchPatterns()}},
	} {
		if err := client.call(ctx, t.session, command.method, command.params, nil); err != nil {
			return failure(err)
		}
	}
	var tree struct {
		Tree struct {
			Frame struct {
				ID string `json:"id"`
			} `json:"frame"`
		} `json:"frameTree"`
	}
	if err := client.call(ctx, t.session, "Page.getFrameTree", map[string]any{}, &tree); err != nil {
		return failure(err)
	}
	if tree.Tree.Frame.ID == "" || len(tree.Tree.Frame.ID) > 256 || tree.Tree.Frame.ID == mainFrame {
		return failure(nil)
	}
	t.frame.Store(tree.Tree.Frame.ID)
	if err := client.call(ctx, t.session, "Runtime.runIfWaitingForDebugger", map[string]any{}, nil); err != nil {
		return failure(err)
	}
	return nil
}

// confirmGone requires target absence before a detached child can release its pending request
// accounting.
func (t *nativeChildTarget) confirmGone(ctx context.Context, client *browserCDP) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	failure := func(err error) error {
		return &nativeEventFailure{"child_detach_unconfirmed", errors.Join(ErrNativePolicy, ErrNativeOwnershipLost, err)}
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		var targets struct {
			Infos []struct {
				ID string `json:"targetId"`
			} `json:"targetInfos"`
		}
		if err := client.call(ctx, "", "Target.getTargets", map[string]any{}, &targets); err != nil {
			return failure(err)
		}
		if targets.Infos == nil {
			return failure(nil)
		}
		found := false
		for _, target := range targets.Infos {
			if target.ID == t.id {
				found = true
				break
			}
		}
		if !found {
			return nil
		}
		select {
		case <-ctx.Done():
			return failure(ctx.Err())
		case <-tick.C:
		}
	}
}

func newNativePausedTargets(context, target, session string) *nativePausedTargets {
	return &nativePausedTargets{context: context, target: target, session: session, targets: make(map[string]*nativeChildTarget), sessions: make(map[string]*nativeChildTarget)}
}

// ownsFrame recognizes only frames belonging to admitted child sessions of this render.
func (p *nativePausedTargets) ownsFrame(id string) bool {
	if id == "" {
		return false
	}
	for _, child := range p.targets {
		if child.kind == "iframe" && child.frameID() == id {
			return true
		}
	}
	return false
}

// admit validates paused child ownership and bounded target counts, refusing unsupported target
// activity.
func (p *nativePausedTargets) admit(message *cdproto.Message) (*nativeChildTarget, error) {
	var event struct {
		Session string `json:"sessionId"`
		Paused  bool   `json:"waitingForDebugger"`
		Target  struct {
			ID      string `json:"targetId"`
			Type    string `json:"type"`
			Context string `json:"browserContextId"`
		} `json:"targetInfo"`
	}
	invalid := func() (*nativeChildTarget, error) {
		return nil, &nativeEventFailure{"child_target_invalid", errors.Join(ErrNativePolicy, ErrNativeOwnershipLost)}
	}
	if message == nil {
		return invalid()
	}
	parent := string(message.SessionID)
	if parent != p.session {
		owner := p.sessions[parent]
		if owner == nil || owner.kind != "iframe" || owner.gone || owner.detaching {
			return invalid()
		}
	}
	if json.Unmarshal(message.Params, &event) != nil {
		return invalid()
	}
	if event.Session == "" || len(event.Session) > 256 || event.Session == p.session || event.Target.ID == "" || len(event.Target.ID) > 256 || event.Target.ID == p.target || event.Target.Context != p.context {
		return invalid()
	}
	if p.sessions[event.Session] != nil || p.targets[event.Target.ID] != nil {
		return invalid()
	}
	if !event.Paused {
		return nil, &nativeEventFailure{"child_target_unpaused", errors.Join(ErrNativePolicy, ErrNativeOwnershipLost)}
	}
	switch event.Target.Type {
	case "service_worker", "iframe":
	case "page":
		return nil, &nativeEventFailure{"child_page_denied", ErrNativeUnsupported}
	default:
		return nil, &nativeEventFailure{"child_target_unsupported", ErrNativeUnsupported}
	}
	if len(p.targets) >= nativePausedTargetLimit {
		return nil, nativeResourceLimit("child_target_count", extractor.ErrWireLimit)
	}
	child := &nativeChildTarget{id: event.Target.ID, session: event.Session, parent: parent, kind: event.Target.Type}
	p.targets[child.id] = child
	p.sessions[child.session] = child
	return child, nil
}
