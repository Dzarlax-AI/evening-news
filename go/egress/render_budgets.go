package egress

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/internal/jsonsingle"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

type renderBudget struct {
	mu                         sync.Mutex
	id                         string
	hash                       [32]byte
	limit, remaining, used     int64
	closed, exhausted, expired bool
	ops                        map[*operation]struct{}
	wg                         sync.WaitGroup
	expires                    time.Time
	timer                      *time.Timer
}
type renderReceipt struct {
	Version   string `json:"contract_version"`
	ID        string `json:"budget_id"`
	Limit     int64  `json:"wire_limit_bytes"`
	Bytes     int64  `json:"wire_bytes"`
	Exhausted bool   `json:"exhausted"`
	Expired   bool   `json:"expired"`
	Revoked   bool   `json:"revoked"`
	Active    int    `json:"active_tunnels"`
}

// attach registers an operation only while this render budget is live and has remaining
// allowance.
func (b *renderBudget) attach(op *operation) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || !time.Now().Before(b.expires) || b.remaining <= 0 {
		return false
	}
	b.ops[op] = struct{}{}
	b.wg.Add(1)
	return true
}
func (b *renderBudget) detach(op *operation) {
	b.mu.Lock()
	delete(b.ops, op)
	b.mu.Unlock()
	b.wg.Done()
}

// stop freezes the budget state and closes only its owned operations without holding the budget
// lock.
func (b *renderBudget) stop(exhausted, expired bool) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.exhausted = b.exhausted || exhausted
	b.expired = b.expired || expired
	b.closed = true
	if b.timer != nil {
		b.timer.Stop()
	}
	ops := make([]*operation, 0, len(b.ops))
	for op := range b.ops {
		ops = append(ops, op)
	}
	b.mu.Unlock()
	for _, op := range ops {
		op.cancel()
		op.close()
	}
}
func (b *renderBudget) receipt() renderReceipt {
	b.mu.Lock()
	defer b.mu.Unlock()
	return renderReceipt{"1", b.id, b.limit, b.used, b.exhausted, b.expired, b.closed, len(b.ops)}
}

// controlAuthorized accepts one bounded bearer credential matching the hashed control secret.
func (g *Gateway) controlAuthorized(r *http.Request) bool {
	values := r.Header.Values("Authorization")
	if len(values) != 1 || len(values[0]) > 512 || !strings.HasPrefix(values[0], "Bearer ") {
		return false
	}
	hash := sha256.Sum256([]byte(strings.TrimPrefix(values[0], "Bearer ")))
	return subtle.ConstantTimeCompare(hash[:], g.controlHash[:]) == 1
}

// scopedBudget resolves a bounded proxy credential only to its own unexpired, unrevoked render
// budget.
func (g *Gateway) scopedBudget(r *http.Request) *renderBudget {
	values := r.Header.Values("Proxy-Authorization")
	if len(values) != 1 || len(values[0]) > 512 {
		return nil
	}
	scheme, encoded, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Basic") {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || !strings.HasPrefix(string(raw), "extractor:") {
		return nil
	}
	hash := sha256.Sum256([]byte(strings.TrimPrefix(string(raw), "extractor:")))
	g.budgetMu.Lock()
	b := g.budgetTokens[hash]
	g.budgetMu.Unlock()
	if b == nil {
		return nil
	}
	b.mu.Lock()
	active := !b.closed && time.Now().Before(b.expires)
	b.mu.Unlock()
	if !active {
		return nil
	}
	return b
}
func budgetJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}

// serveBudgets authenticates bounded versioned control requests and allocates, inspects or
// revokes render budgets.
func (g *Gateway) serveBudgets(w http.ResponseWriter, r *http.Request) {
	if !g.controlAuthorized(r) {
		http.Error(w, "control_authentication_required", 401)
		return
	}
	if r.URL.RawQuery != "" || r.URL.EscapedPath() != r.URL.Path || r.ProtoMajor != 1 {
		http.Error(w, "budget_request_invalid", 400)
		return
	}
	const root = "/v1/render-budgets"
	if r.Method == http.MethodPost && r.URL.Path == root {
		var in struct {
			Version string `json:"contract_version"`
			Limit   int64  `json:"wire_limit_bytes"`
			TTL     int64  `json:"ttl_ms"`
		}
		raw, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
		if readErr != nil || jsonsingle.DecodeObject(raw, &in, "contract_version", "wire_limit_bytes", "ttl_ms") != nil || in.Version != "1" || in.Limit < 1 || in.Limit > transport.MaxPageBytes || in.TTL < 1 || in.TTL > 35000 {
			http.Error(w, "budget_request_invalid", 400)
			return
		}
		ttl := min(time.Duration(in.TTL)*time.Millisecond, g.cfg.MaxLifetime)
		var id [16]byte
		var secret [32]byte
		if _, err := rand.Read(id[:]); err != nil {
			http.Error(w, "budget_unavailable", 503)
			return
		}
		if _, err := rand.Read(secret[:]); err != nil {
			http.Error(w, "budget_unavailable", 503)
			return
		}
		token := hex.EncodeToString(secret[:])
		b := &renderBudget{id: hex.EncodeToString(id[:]), hash: sha256.Sum256([]byte(token)), limit: in.Limit, remaining: in.Limit, ops: map[*operation]struct{}{}, expires: time.Now().Add(ttl)}
		// Serialize registration with Drain's admission fence.
		g.mu.Lock()
		if g.draining {
			g.mu.Unlock()
			http.Error(w, "draining", 503)
			return
		}
		g.budgetMu.Lock()
		now := time.Now()
		for id, old := range g.budgets {
			if now.After(old.expires.Add(5 * time.Second)) {
				delete(g.budgets, id)
				delete(g.budgetTokens, old.hash)
			}
		}
		if len(g.budgets) >= max(128, g.cfg.MaxConcurrent*8) {
			g.budgetMu.Unlock()
			g.mu.Unlock()
			http.Error(w, "budget_busy", 429)
			return
		}
		g.budgets[b.id] = b
		g.budgetTokens[b.hash] = b
		b.mu.Lock()
		b.timer = time.AfterFunc(ttl, func() { b.stop(false, true) })
		b.mu.Unlock()
		g.budgetMu.Unlock()
		g.mu.Unlock()
		budgetJSON(w, 201, map[string]any{"contract_version": "1", "budget_id": b.id, "proxy_token": token, "wire_limit_bytes": b.limit, "ttl_ms": ttl.Milliseconds()})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, root+"/")
	if len(id) != 32 {
		http.Error(w, "budget_request_invalid", 400)
		return
	}
	if raw, err := hex.DecodeString(id); err != nil || len(raw) != 16 {
		http.Error(w, "budget_request_invalid", 400)
		return
	}
	g.budgetMu.Lock()
	b := g.budgets[id]
	g.budgetMu.Unlock()
	if b == nil {
		http.Error(w, "budget_not_found", 404)
		return
	}
	switch r.Method {
	case http.MethodDelete:
		b.stop(false, false)
		done := make(chan struct{})
		go func() { b.wg.Wait(); close(done) }()
		select {
		case <-done:
			budgetJSON(w, 200, b.receipt())
		case <-r.Context().Done():
			return
		}
	case http.MethodGet:
		budgetJSON(w, 200, b.receipt())
	default:
		http.Error(w, "budget_method_denied", 405)
	}
}

// Each reservation is shared by every tunnel and both directions of a render.
// Reserved bytes are never refunded; short writes can only tighten the bound.
func (g *Gateway) copyScopedTunnel(dst io.Writer, src io.Reader, tunnel *atomic.Int64, b *renderBudget) {
	buffer := make([]byte, 16*1024)
	for {
		n, readErr := src.Read(buffer)
		if n > 0 {
			allowed := int64(n)
			for {
				left := tunnel.Load()
				if left <= 0 {
					return
				}
				allowed = min(allowed, left)
				if tunnel.CompareAndSwap(left, left-allowed) {
					break
				}
			}
			b.mu.Lock()
			if b.closed {
				b.mu.Unlock()
				return
			}
			allowed = min(allowed, b.remaining)
			b.remaining -= allowed
			exhausted := b.remaining == 0
			b.exhausted = b.exhausted || exhausted
			b.mu.Unlock()
			if allowed <= 0 {
				b.stop(true, false)
				return
			}
			written, writeErr := dst.Write(buffer[:allowed])
			b.mu.Lock()
			b.used += int64(written)
			b.mu.Unlock()
			g.bytes.Add(uint64(written))
			if exhausted {
				b.stop(true, false)
			}
			if writeErr != nil || int64(written) != allowed || allowed < int64(n) || exhausted || tunnel.Load() == 0 {
				return
			}
		}
		if readErr != nil {
			return
		}
	}
}

// stopRenderBudgets revokes registered budgets after releasing the registry lock to avoid lock
// inversion.
func (g *Gateway) stopRenderBudgets() {
	g.budgetMu.Lock()
	all := make([]*renderBudget, 0, len(g.budgets))
	for _, b := range g.budgets {
		all = append(all, b)
	}
	g.budgetMu.Unlock()
	for _, b := range all {
		b.stop(false, false)
	}
}
