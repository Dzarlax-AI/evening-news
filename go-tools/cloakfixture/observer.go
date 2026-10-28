package cloakfixture

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

var commandName = regexp.MustCompile(`^[0-9a-f]{32}\.json$`)
var observerErrors = map[string]bool{"fixture_evidence_invalid": true, "fixture_arrival_timeout": true, "origin_state_unavailable": true, "worker_unavailable": true, "worker_response_invalid": true, "client_timeout": true, "one_slot_overlap_detected": true, "two_slot_barrier_or_third_waiter_invalid": true, "synthetic_article_request_failed": true, "cancellation_isolation_failed": true, "upstream_error_classification_invalid": true, "peer_target_or_context_lost": true, "peer_session_probe_failed": true, "news_browser_resource_leak": true, "cdp_connection_closed": true, "cdp_command_failed": true, "fixture_release_failed": true}

func privateJSON(path string, value any) error {
	b, e := json.Marshal(value)
	if e != nil || len(b) > 4096 {
		return errors.New("receipt_invalid")
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return errors.New("receipt_write_failed")
	}
	_, e = f.Write(append(b, '\n'))
	if e == nil {
		e = f.Sync()
	}
	if x := f.Close(); e == nil {
		e = x
	}
	if e != nil {
		_ = os.Remove(path)
		return errors.New("receipt_write_failed")
	}
	return nil
}
func atomicReceipt(dir, name string, value any) error {
	temp, e := os.CreateTemp(dir, ".receipt-")
	if e != nil {
		return errors.New("receipt_write_failed")
	}
	path := temp.Name()
	temp.Close()
	os.Remove(path)
	defer os.Remove(path)
	if e = privateJSON(path, value); e != nil {
		return e
	}
	if e = os.Rename(path, filepath.Join(dir, name)); e != nil {
		return errors.New("receipt_write_failed")
	}
	return nil
}
func RunObserver(ctx context.Context, p *Profiles, peer *CDPPeer, commandDir, resultDir string) error {
	if e := os.MkdirAll(resultDir, 0700); e != nil {
		return errors.New("observer_error")
	}
	if e := atomicReceipt(resultDir, "peer-open.json", map[string]string{"target": "held", "context": "held"}); e != nil {
		return e
	}
	defer peer.Close(context.Background())
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		entries, e := os.ReadDir(commandDir)
		if e != nil {
			return errors.New("observer_error")
		}
		for _, entry := range entries {
			if !commandName.MatchString(entry.Name()) || !entry.Type().IsRegular() {
				continue
			}
			path := filepath.Join(commandDir, entry.Name())
			info, e := entry.Info()
			if e != nil || info.Size() > 4096 || info.Mode().Perm()&0077 != 0 {
				return errors.New("client_command_invalid")
			}
			b, e := os.ReadFile(path)
			var command struct {
				Action string `json:"action"`
			}
			if e != nil || json.Unmarshal(b, &command) != nil {
				return errors.New("client_command_invalid")
			}
			if e = os.Remove(path); e != nil {
				return errors.New("client_command_invalid")
			}
			id := entry.Name()[:32]
			var result any
			switch command.Action {
			case "profile_one":
				result, e = p.Profile(ctx, 1)
			case "profile_two":
				result, e = p.Profile(ctx, 2)
			case "cancellation":
				result, e = p.Cancellation(ctx)
			case "upstream_error":
				result, e = p.UpstreamError(ctx)
			case "probe":
				e = peer.AssertClean(ctx)
				result = map[string]bool{"peer_survived": true}
			case "close":
				e = peer.Close(ctx)
				result = map[string]bool{"peer_closed": true}
			default:
				e = errors.New("client_command_invalid")
			}
			receipt := map[string]any{"command_id": id, "ok": e == nil}
			if e != nil {
				code := e.Error()
				if !observerErrors[code] {
					code = "client_gate_failed"
				}
				receipt["error"] = code
			} else {
				receipt["result"] = result
			}
			if err := atomicReceipt(resultDir, entry.Name(), receipt); err != nil {
				return err
			}
			if e != nil {
				return errors.New("client_gate_failed")
			}
			if command.Action == "close" {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return errors.New("observer_cancelled")
		case <-ticker.C:
		}
	}
}
