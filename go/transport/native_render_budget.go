package transport

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/internal/jsonsingle"
)

type nativeRenderBudget struct {
	id, token, url, control string
	limit                   int64
}

var nativeReceiptFields = []string{"contract_version", "budget_id", "wire_limit_bytes", "wire_bytes", "exhausted", "expired", "revoked", "active_tunnels"}

type nativeBudgetReceipt struct {
	Version   string `json:"contract_version"`
	ID        string `json:"budget_id"`
	Limit     int64  `json:"wire_limit_bytes"`
	Bytes     int64  `json:"wire_bytes"`
	Exhausted bool   `json:"exhausted"`
	Expired   bool   `json:"expired"`
	Revoked   bool   `json:"revoked"`
	Active    int    `json:"active_tunnels"`
}

// nativeBudgetHTTP makes bounded authenticated control requests without proxying or following
// redirects and strictly decodes replies.
func nativeBudgetHTTP(ctx context.Context, method, url, token string, payload any, out any, fields ...string) error {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return ErrNativeNotReady
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	tr := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext, ResponseHeaderTimeout: 2 * time.Second, MaxResponseHeaderBytes: 8192}
	defer tr.CloseIdleConnections()
	client := http.Client{Transport: tr, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrNativePolicy }}
	response, err := client.Do(req)
	if err != nil {
		return errors.Join(ErrNativeNotReady, err)
	}
	defer response.Body.Close()
	if response.StatusCode == 429 {
		return ErrNativeBusy
	}
	wantStatus := http.StatusOK
	if method == http.MethodPost {
		wantStatus = http.StatusCreated
	}
	if response.StatusCode != wantStatus {
		return ErrNativeNotReady
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1025))
	if err != nil || len(raw) > 1024 {
		return ErrNativeOwnershipLost
	}
	if jsonsingle.DecodeObject(raw, out, fields...) != nil {
		return ErrNativeOwnershipLost
	}
	return nil
}

// nativeAcquireBudget allocates a scoped credential bounded by the render deadline and validates
// the issued grant.
func nativeAcquireBudget(ctx context.Context, cfg NativeBrowserConfig, limit int64) (*nativeRenderBudget, error) {
	ttl := 35 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		ttl = min(ttl, time.Until(deadline)+5*time.Second)
	}
	if ttl <= 0 {
		return nil, ctx.Err()
	}
	var receipt struct {
		Version string `json:"contract_version"`
		ID      string `json:"budget_id"`
		Token   string `json:"proxy_token"`
		Limit   int64  `json:"wire_limit_bytes"`
		TTL     int64  `json:"ttl_ms"`
	}
	endpoint := cfg.ProxyURL
	if cfg.BudgetEndpoint != "" {
		endpoint = cfg.BudgetEndpoint
	}
	url := strings.TrimRight(endpoint, "/") + "/v1/render-budgets"
	if err := nativeBudgetHTTP(ctx, "POST", url, cfg.ProxyToken, map[string]any{"contract_version": "1", "wire_limit_bytes": limit, "ttl_ms": ttl.Milliseconds()}, &receipt, "contract_version", "budget_id", "proxy_token", "wire_limit_bytes", "ttl_ms"); err != nil {
		return nil, err
	}
	id, idErr := hex.DecodeString(receipt.ID)
	secret, tokenErr := hex.DecodeString(receipt.Token)
	if receipt.Version != "1" || idErr != nil || len(id) != 16 || tokenErr != nil || len(secret) != 32 || receipt.Limit != limit || receipt.TTL < 1 || receipt.TTL > ttl.Milliseconds() {
		return nil, ErrNativeOwnershipLost
	}
	return &nativeRenderBudget{receipt.ID, receipt.Token, url + "/" + receipt.ID, cfg.ProxyToken, limit}, nil
}

// revoke requires a frozen receipt for this exact budget with no remaining active tunnels.
func (b *nativeRenderBudget) revoke(ctx context.Context) (nativeBudgetReceipt, error) {
	var receipt nativeBudgetReceipt
	if err := nativeBudgetHTTP(ctx, "DELETE", b.url, b.control, nil, &receipt, nativeReceiptFields...); err != nil {
		return receipt, errors.Join(ErrNativeOwnershipLost, err)
	}
	if receipt.Version != "1" || receipt.ID != b.id || receipt.Limit != b.limit || receipt.Bytes < 0 || receipt.Bytes > b.limit || !receipt.Revoked || receipt.Active != 0 {
		return receipt, ErrNativeOwnershipLost
	}

	return receipt, nil
}

// open allocates the render budget before CDP resources and revokes it if browser allocation
// fails.
func (w *NativeBrowser) open(ctx context.Context, limit int64) (*nativeResources, error) {
	budget, err := nativeAcquireBudget(ctx, w.cfg, limit)
	if err != nil {
		if errors.Is(err, ErrNativeOwnershipLost) {
			w.taint()
		} else if errors.Is(err, ErrNativeNotReady) {
			w.unavailable()
		}
		return nil, err
	}
	resources, err := w.openCDP(ctx)
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, revokeErr := budget.revoke(cleanup)
		return nil, errors.Join(err, revokeErr)
	}
	resources.budget = budget
	return resources, nil
}

// nativeReplayResponse preserves decoded publisher bytes and policies while normalizing framing
// and adding the worker guard.
func nativeReplayResponse(ctx context.Context, client *browserCDP, session string, event nativeFetchPaused, params map[string]any, remaining *atomic.Int64) error {
	body, err := nativeBoundedReplayBody(ctx, client, session, event.ID, remaining)
	if err != nil {
		return err
	}
	headers := make([]nativeResponseHeader, 0, len(event.ResponseHeaders)+2)
	for _, h := range event.ResponseHeaders {
		switch strings.ToLower(h.Name) {
		case "content-length", "content-encoding", "transfer-encoding":
		default:
			headers = append(headers, h)
		}
	}
	headers = append(headers, nativeResponseHeader{"Content-Security-Policy", nativeWorkerPolicy}, nativeResponseHeader{"Content-Length", strconv.Itoa(len(body))})
	params["responseHeaders"] = headers
	params["body"] = base64.StdEncoding.EncodeToString(body)

	// Publisher bytes are bounded by the shared decoded-body budget. The normal
	// relay forwards bounded chunks; its synthetic parser has a separate test cap.
	if err := client.call(ctx, session, "Fetch.fulfillRequest", params, nil); err != nil {
		return errors.Join(ErrNativePolicy, ErrNativeOwnershipLost, err)
	}
	return nil
}

// nativeBoundedReplayBody charges every stream chunk to the shared decoded allowance and always
// confirms stream closure.
func nativeBoundedReplayBody(ctx context.Context, client *browserCDP, session, id string, remaining *atomic.Int64) (body []byte, err error) {
	var stream struct {
		ID string `json:"stream"`
	}
	if err = client.call(ctx, session, "Fetch.takeResponseBodyAsStream", map[string]any{"requestId": id}, &stream); err != nil {
		return nil, err
	}
	if stream.ID == "" || len(stream.ID) > 256 {
		return nil, ErrNativeOwnershipLost
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if closeErr := client.callCleanup(cleanup, session, "IO.close", map[string]any{"handle": stream.ID}, nil); closeErr != nil {
			body = nil
			err = errors.Join(err, ErrNativeOwnershipLost, closeErr)
		}
	}()
	for {
		var chunk struct {
			Data   string `json:"data"`
			Base64 bool   `json:"base64Encoded"`
			EOF    bool   `json:"eof"`
		}
		size := min(int64(64*1024), max(1, remaining.Load()+1))
		if err = client.call(ctx, session, "IO.read", map[string]any{"handle": stream.ID, "size": size}, &chunk); err != nil {
			return nil, err
		}
		data := []byte(chunk.Data)
		if chunk.Base64 {
			data, err = base64.StdEncoding.DecodeString(chunk.Data)
			if err != nil {
				return nil, err
			}
		}
		if len(data) > 64*1024 {
			return nil, ErrNativeOwnershipLost
		}
		for {
			left := remaining.Load()
			if int64(len(data)) > left {
				return nil, nativeResourceLimit("dom_output", extractor.ErrOutputLimit)
			}
			if remaining.CompareAndSwap(left, left-int64(len(data))) {
				break
			}
		}
		body = append(body, data...)
		if chunk.EOF {
			return body, nil
		}
		if len(data) == 0 {
			return nil, ErrNativeOwnershipLost
		}
	}
}

// watch polls only this budget and cancels its render on exhaustion, expiry or untrusted control
// outcomes.
func (b *nativeRenderBudget) watch(ctx context.Context, stop context.CancelCauseFunc) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				probe, cancel := context.WithTimeout(ctx, time.Second)
				var receipt nativeBudgetReceipt
				err := nativeBudgetHTTP(probe, "GET", b.url, b.control, nil, &receipt, nativeReceiptFields...)
				cancel()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					stop(err)
					return
				}
				if receipt.Version != "1" || receipt.ID != b.id || receipt.Limit != b.limit || receipt.Bytes < 0 || receipt.Bytes > b.limit || receipt.Active < 0 || receipt.Active > 128 {
					stop(ErrNativeOwnershipLost)
					return
				}
				if receipt.Exhausted {
					stop(nativeResourceLimit("wire_bytes", extractor.ErrWireLimit))
					return
				}
				if receipt.Expired {
					stop(context.DeadlineExceeded)
					return
				}
				if receipt.Revoked {
					stop(ErrNativeOwnershipLost)
					return
				}
			}
		}
	}()
	return done
}
