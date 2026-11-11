package cloakfixture

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

var browserEventKinds = []string{"server-policy", "service-worker", "iframe", "nested-iframe", "worker", "shared-worker", "service-worker-large", "wire-gzip-fixed", "wire-gzip-chunked", "iframe-private", "iframe-loopback", "iframe-metadata", "iframe-http", "iframe-ws", "iframe-popup", "iframe-download", "iframe-wire", "encoded", "redirect", "inherited-blank", "inherited-blob"}

func pausedWorkerReceipts(logs string) map[string]int { return workerStateReceipts(logs, "paused") }
func closedWorkerReceipts(logs string) map[string]int { return workerStateReceipts(logs, "closed") }

// workerStateReceipts counts only finite worker-kind and lifecycle-state witnesses in controller
// logs.
func workerStateReceipts(logs, state string) map[string]int {
	logs += "\n"
	out := make(map[string]int)
	for _, kind := range []string{"worker", "shared_worker", "service_worker"} {
		reason := kind + "_" + state
		out[reason] = strings.Count(logs, "native_browser_diagnostic stage=events reason="+reason+"\n")
	}
	return out
}

// nativeFailureReceipts extracts allowlisted failure reasons without retaining publisher
// metadata.
func nativeFailureReceipts(logs string) []string {
	logs += "\n"
	var out []string
	for _, reason := range []string{"child_target_close_unsupported", "child_target_close_missing", "child_target_close_rejected", "child_target_close_unconfirmed", "child_target_close_failed", "child_target_invalid", "child_target_unpaused", "child_target_unsupported",
		"response_policy_invalid", "response_policy_failed", "worker_policy_header_seen", "worker_policy_applied", "child_frame_guard_failed", "child_detach_unconfirmed", "iframe_guarded", "child_frame_unsupported", "child_page_denied", "child_target_activity", "context_timeout",
		"protocol_error", "operation_failed", "unsupported", "ownership_loss",
		"paused_worker_dom_ready", "paused_worker_dom_unready", "paused_worker_requests_pending"} {
		if strings.Contains(logs, " reason="+reason+"\n") {
			out = append(out, reason)
		}
	}
	for _, match := range regexp.MustCompile(`reason=(protocol_error_-?[0-9]{1,6})(?:\n|$)`).FindAllStringSubmatch(logs, -1) {
		out = append(out, match[1])
	}
	return out
}

// browserEventKind restricts synthetic origin routes to the declared browser-policy scenarios.
func browserEventKind(kind string) bool {
	for _, k := range browserEventKinds {
		if kind == k {
			return true
		}
	}
	return false
}

// browserEventRoute serves isolated worker, frame, compression and forbidden-network probes with
// separate execution counters.
func (o *Origin) browserEventRoute(w http.ResponseWriter, r *http.Request) bool {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if len(parts) != 2 || !browserEventKind(parts[1]) {
		return false
	}
	route, kind := parts[0], parts[1]
	switch route {
	case "browser-events":
		if kind == "redirect" {
			http.Redirect(w, r, "/browser-events/worker", http.StatusFound)
			return true
		}
		script := ""
		switch kind {
		case "inherited-blank", "inherited-blob":
			body := "<script>fetch('https://native-fixture.test/frame-executed/iframe');try{new SharedWorker('https://native-fixture.test/event-script/shared-worker')}catch(e){};try{new Worker('https://native-fixture.test/event-script/worker')}catch(e){};<" + "/script>"
			encoded, _ := json.Marshal(body)
			script = "const f=document.createElement('iframe');"
			if kind == "inherited-blank" {
				script += "document.body.appendChild(f);f.contentDocument.open();f.contentDocument.write(" + string(encoded) + ");f.contentDocument.close();"
			} else {
				script += "f.src=URL.createObjectURL(new Blob([" + string(encoded) + "],{type:'text/html'}));document.body.appendChild(f);"
			}
		case "server-policy":
			w.Header().Set("Content-Security-Policy", "worker-src 'none'")
			script = "try{new Worker('/event-script/worker')}catch(e){};try{new SharedWorker('/event-script/shared-worker')}catch(e){};navigator.serviceWorker.register('/event-script/service-worker').catch(()=>{});"
		case "worker":
			script = "new Worker('/event-script/worker');"
		case "shared-worker":
			script = "new SharedWorker('/event-script/shared-worker');"
		case "iframe", "nested-iframe", "iframe-private", "iframe-loopback", "iframe-metadata", "iframe-http", "iframe-ws", "iframe-popup", "iframe-download", "iframe-wire":
			script = "const f=document.createElement('iframe');f.src='https://native-frame.test/event-frame/" + kind + "';document.body.appendChild(f);"
		case "service-worker", "service-worker-large":
			script = "navigator.serviceWorker.register('/event-script/" + kind + "', {scope:'/'}).catch(()=>{});"
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		body := "<html><body><main><article><a href=\"/story\"><h2>Synthetic PageMonitor story</h2></a><time datetime=\"2026-10-29T12:00:00Z\">29 October 2026</time><p>" + fixtureBody + "</p></article></main><script>" + script + "</script></body></html>"

		if kind == "wire-gzip-fixed" || kind == "wire-gzip-chunked" {
			encoded := paddedFixtureGzip([]byte(body), 6*1024*1024)
			w.Header().Set("Content-Encoding", "gzip")
			if kind == "wire-gzip-fixed" {
				w.Header().Set("Content-Length", strconv.Itoa(len(encoded)))
			} else {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
			}
			n, err := w.Write(encoded)
			o.mu.Lock()
			o.state.ProbeEncodedBytes += n
			o.state.ProbeDecodedBytes += len(body)
			persistErr := o.persist()
			o.mu.Unlock()
			_ = err
			_ = persistErr
		} else if kind == "encoded" {
			w.Header().Set("Content-Encoding", "gzip")
			w.Header().Set("Content-Security-Policy", "connect-src 'self'")
			z := gzip.NewWriter(w)
			fmt.Fprint(z, body)
			z.Close()
		} else {
			fmt.Fprint(w, body)
		}

	case "event-frame":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		nested := ""
		if kind == "nested-iframe" {
			nested = "const f=document.createElement('iframe');f.src='https://native-nested.test/event-frame/iframe';document.body.appendChild(f);"
		}

		action := ""
		switch kind {
		case "iframe-private":
			action = "fetch('https://10.0.0.1/forbidden').catch(()=>{});"
		case "iframe-loopback":
			action = "fetch('https://127.0.0.1/forbidden').catch(()=>{});"
		case "iframe-metadata":
			action = "fetch('https://169.254.169.254/forbidden').catch(()=>{});"
		case "iframe-http":
			action = "fetch('http://native-frame.test/forbidden').catch(()=>{});"
		case "iframe-ws":
			action = "document.addEventListener('securitypolicyviolation',function witness(e){if(e.effectiveDirective==='connect-src'&&e.blockedURI.startsWith('wss:')){document.removeEventListener('securitypolicyviolation',witness);fetch('/frame-blocked/iframe-ws')}});try{new WebSocket('wss://native-frame.test/forbidden?probe=iframe-ws')}catch(e){};"
		case "iframe-popup":
			action = "window.open('https://native-frame.test/forbidden?probe=iframe-popup');"
		case "iframe-download":
			action = "const a=document.createElement('a');a.href='/event-download/iframe';a.download='synthetic.txt';document.body.appendChild(a);a.click();"
		case "iframe-wire":
			action = "fetch('/event-large/iframe').catch(()=>{});"
		}
		fmt.Fprint(w, "<html><body>Guarded synthetic child<script>fetch('/frame-executed/"+kind+"');try{new Worker('/event-script/worker')}catch(e){};try{new SharedWorker('/event-script/shared-worker')}catch(e){};navigator.serviceWorker.register('/event-script/service-worker').catch(()=>{});"+nested+action+"</script></body></html>")
	case "event-download":
		w.Header().Set("Content-Disposition", "attachment; filename=synthetic.txt")
		w.Header().Set("Content-Type", "application/octet-stream")
		fmt.Fprint(w, "Synthetic denied download")
	case "event-large":
		w.Header().Set("Content-Type", "application/octet-stream")
		fmt.Fprint(w, strings.Repeat("x", 6*1024*1024))

	case "frame-blocked":
		o.mu.Lock()
		defer o.mu.Unlock()
		if o.state.BlockedActions == nil {
			o.state.BlockedActions = map[string]int{}
		}
		o.state.BlockedActions[kind]++
		if err := o.persist(); err != nil {
			http.Error(w, "fixture state unavailable", 503)
			return true
		}
		w.WriteHeader(204)
	case "frame-executed":
		o.mu.Lock()
		defer o.mu.Unlock()
		o.state.FrameExecutions++
		if err := o.persist(); err != nil {
			http.Error(w, "fixture state unavailable", http.StatusServiceUnavailable)
			return true
		}
		w.WriteHeader(http.StatusNoContent)
	case "event-script", "event-executed":
		o.mu.Lock()
		defer o.mu.Unlock()
		if route == "event-script" {
			o.state.EventScripts[kind]++
		} else {
			o.state.EventExecutions++
			o.state.WorkerExecutions[kind]++
		}
		if err := o.persist(); err != nil {
			http.Error(w, "fixture state unavailable", http.StatusServiceUnavailable)
			return true
		}
		if route == "event-script" {
			w.Header().Set("Content-Type", "application/javascript")
			w.Header().Set("Service-Worker-Allowed", "/")
			if kind == "service-worker-large" {
				fmt.Fprint(w, "/*"+strings.Repeat("x", 6*1024*1024)+"*/")
			}
			fmt.Fprintf(w, "fetch('/event-executed/%s');", kind)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	default:
		return false
	}
	return true
}

type BrowserEventsResult struct {
	BlockedActions              map[string]int             `json:"blocked_actions,omitempty"`
	ForbiddenProbes             map[string]int             `json:"forbidden_probes,omitempty"`
	ProbeEncodedBytes           int                        `json:"probe_encoded_bytes"`
	ProbeDecodedBytes           int                        `json:"probe_decoded_bytes"`
	ServerPolicyPreventsWorkers bool                       `json:"server_policy_prevents_workers"`
	ForbiddenRequests           int                        `json:"forbidden_requests"`
	PublisherAuthHeaders        int                        `json:"publisher_auth_headers"`
	WorkerExecutions            map[string]int             `json:"executions_by_worker_kind"`
	CountersObserved            bool                       `json:"counters_observed"`
	FailedKind                  string                     `json:"failed_kind,omitempty"`
	Requests                    []PageMonitorRequestResult `json:"requests"`
	Scripts                     map[string]int             `json:"initial_script_requests"`
	Executions                  int                        `json:"worker_execution_requests"`
	FrameExecutions             int                        `json:"frame_execution_requests"`
}

// BrowserEvents runs browser-policy scenarios and rejects worker execution, forbidden requests
// or credential leakage.
func (p *Profiles) BrowserEvents(ctx context.Context) (BrowserEventsResult, error) {
	var out BrowserEventsResult
	for _, kind := range browserEventKinds {
		var result PageMonitorRequestResult
		var err error
		if strings.HasPrefix(kind, "wire-gzip-") {
			result, err = p.Worker.pageMonitorRejected(ctx, "browser-events/"+kind)
			if result.Error != "resource_limit" {
				err = errors.New("replay_original_wire_unaccounted")
			}
		} else if strings.HasPrefix(kind, "iframe-") {
			result, err = p.Worker.pageMonitorRejected(ctx, "browser-events/"+kind)
			if result.Outcome == "success" && result.FetchMethod == "native_browser" && result.ItemCount == 1 && (kind == "iframe-http" || kind == "iframe-ws") {
				err = nil
			}
			if kind == "iframe-wire" && result.Error != "resource_limit" {
				err = errors.New("frame_wire_unaccounted")
			}
			if result.Error == "deadline_exceeded" {
				err = errors.New("frame_policy_timeout")
			}
		} else {
			result, err = p.Worker.pageMonitorRequest(ctx, "browser-events/"+kind)
		}
		if err != nil {
			out.FailedKind = kind
			result.Name = kind
			out.Requests = append(out.Requests, result)
			if state, stateErr := p.state(); stateErr == nil {
				out.Scripts, out.Executions, out.CountersObserved = state.EventScripts, state.EventExecutions, true
				out.FrameExecutions = state.FrameExecutions
				out.WorkerExecutions = state.WorkerExecutions
				out.ForbiddenRequests = state.ForbiddenRequests
				out.ForbiddenProbes = state.ForbiddenProbes
				out.BlockedActions = state.BlockedActions
				out.PublisherAuthHeaders = state.PublisherAuthHeaders
				out.ProbeEncodedBytes = state.ProbeEncodedBytes
				out.ProbeDecodedBytes = state.ProbeDecodedBytes
			}
			return out, err
		}
		if kind == "server-policy" {
			state, stateErr := p.state()
			if stateErr != nil {
				return out, stateErr
			}
			out.ServerPolicyPreventsWorkers = len(state.EventScripts) == 0 && state.EventExecutions == 0
			if !out.ServerPolicyPreventsWorkers {
				return out, errors.New("server_policy_control_failed")
			}
		}
		result.Name = kind
		out.Requests = append(out.Requests, result)
		if err := p.Peer.AssertClean(ctx); err != nil {
			return out, err
		}
	}
	state, err := p.state()
	if err != nil {
		return out, err
	}
	out.Scripts, out.Executions, out.CountersObserved = state.EventScripts, state.EventExecutions, true
	out.FrameExecutions = state.FrameExecutions
	out.WorkerExecutions = state.WorkerExecutions
	out.ForbiddenRequests = state.ForbiddenRequests
	out.ForbiddenProbes = state.ForbiddenProbes
	out.BlockedActions = state.BlockedActions
	out.PublisherAuthHeaders = state.PublisherAuthHeaders
	out.ProbeEncodedBytes = state.ProbeEncodedBytes
	out.ProbeDecodedBytes = state.ProbeDecodedBytes
	if out.BlockedActions["iframe-ws"] < 1 || out.ForbiddenRequests != 0 || out.PublisherAuthHeaders != 0 || out.Executions != 0 || out.FrameExecutions < 3 {
		return out, errors.New("browser_events_failed")
	}
	for _, count := range out.Scripts {
		if count != 0 {
			return out, errors.New("browser_events_failed")
		}
	}
	return out, nil
}

// Empty stored deflate blocks increase encoded bytes without expanding HTML.
func paddedFixtureGzip(body []byte, padding int) []byte {
	var out bytes.Buffer
	out.Write([]byte{0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 3})
	for i := 0; i < padding/5; i++ {
		out.Write([]byte{0, 0, 0, 0xff, 0xff})
	}
	stream, _ := flate.NewWriter(&out, flate.NoCompression)
	stream.Write(body)
	stream.Close()
	binary.Write(&out, binary.LittleEndian, crc32.ChecksumIEEE(body))
	binary.Write(&out, binary.LittleEndian, uint32(len(body)))
	return out.Bytes()
}

// Only bounded numeric/boolean synthetic observations may leave the test stand.
func replayProbeReceipts(logs string) map[string]any {
	out := map[string]any{}
	streams, closes, closeFailures := 0, 0, 0
	var decoded, encoded, dataEncoded float64
	for _, line := range strings.Split(logs, "\n") {
		if !strings.HasPrefix(line, "native_replay_probe ") {
			continue
		}
		var value map[string]any
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "native_replay_probe ")), &value) != nil {
			continue
		}
		event, ok := value["event"].(string)
		if !ok || (event != "stream" && event != "loading" && event != "close") {
			continue
		}
		safe := map[string]any{"event": event}
		for key, item := range value {
			switch key {
			case "decoded", "encoded", "data_encoded", "length":
				if n, ok := item.(float64); ok && n >= 0 && n <= 128*1024*1024 {
					safe[key] = n
				}
			case "closed", "has_length", "compressed":
				if b, ok := item.(bool); ok {
					safe[key] = b
				}
			}
		}
		switch event {
		case "stream":
			streams++
			out["last_stream"] = safe
			if n, ok := safe["decoded"].(float64); ok {
				decoded += n
			}
		case "loading":
			out["last_loading"] = safe
			if n, ok := safe["encoded"].(float64); ok {
				encoded += n
			}
			if n, ok := safe["data_encoded"].(float64); ok {
				dataEncoded += n
			}
		case "close":
			closes++
			if ok, _ := safe["closed"].(bool); !ok {
				closeFailures++
			}
		}
	}
	if streams > 0 || closes > 0 {
		out["streams"] = streams
		out["decoded_total"] = decoded
		out["loading_encoded_total"] = encoded
		out["data_encoded_total"] = dataEncoded
		out["io_closes"] = closes
		out["io_close_failures"] = closeFailures
	}
	return out
}

// renderBudgetProbeReceipts summarizes validated prototype counters and distinguishes absent
// instrumentation from zero traffic.
func renderBudgetProbeReceipts(logs string) map[string]any {
	count, exhausted := 0, 0
	var maxBytes, maxLimit, maxActive float64
	for _, line := range strings.Split(logs, "\n") {
		if !strings.HasPrefix(line, "native_budget_probe ") {
			continue
		}
		var value struct {
			Bytes     float64 `json:"bytes"`
			Limit     float64 `json:"limit"`
			Active    float64 `json:"active"`
			Exhausted bool    `json:"exhausted"`
		}
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "native_budget_probe ")), &value) != nil || value.Bytes < 0 || value.Limit < 1 || value.Limit > 5*1024*1024 || value.Bytes > value.Limit || value.Active < 0 || value.Active > 128 {
			continue
		}
		count++
		if value.Exhausted {
			exhausted++
		}
		maxBytes = max(maxBytes, value.Bytes)
		maxLimit = max(maxLimit, value.Limit)
		maxActive = max(maxActive, value.Active)
	}
	if count == 0 {
		// Product binaries validate receipts internally without prototype logs.
		return map[string]any{"counters_observed": false, "receipt_validation": "native_controller"}
	}
	return map[string]any{"counters_observed": true, "closed_budgets": count, "exhausted_budgets": exhausted, "max_wire_bytes": maxBytes, "max_limit_bytes": maxLimit, "max_active_tunnels_after_revoke": maxActive}
}
