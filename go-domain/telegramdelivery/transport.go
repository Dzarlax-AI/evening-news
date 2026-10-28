// Package telegramdelivery sends a single Telegram request without retries.
package telegramdelivery

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type State string

const (
	Sent     State = "sent"
	Rejected State = "rejected"
	Unknown  State = "unknown"
)

type Result struct {
	State             State
	MessageID         int64
	StatusCode        int
	ErrorCode         int
	RetryAfterSeconds *float64
}
type Admission interface{ Acquire(context.Context) error }
type Transport struct {
	Client    *http.Client
	Admission Admission
	Endpoint  string
	Token     string
}

// Send performs at most one POST. The caller must persist a sending receipt
// before invoking it, and must treat a failed final receipt write as unknown.
// Neither the request body, token, destination nor response body enters errors.
func (t Transport) Send(ctx context.Context, method string, payload any) Result {
	unknown := Result{State: Unknown}
	if method != "sendMessage" && method != "sendRichMessage" {
		return Result{State: Rejected}
	}
	body, err := json.Marshal(payload)
	if err != nil || len(body) > 1<<20 {
		return Result{State: Rejected}
	}
	u, err := url.Parse(t.Endpoint)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || t.Token == "" || strings.ContainsAny(t.Token, "/?#") {
		return Result{State: Rejected}
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/bot" + t.Token + "/" + method
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return Result{State: Rejected}
	}
	req.Header.Set("Content-Type", "application/json")
	if t.Admission != nil {
		if t.Admission.Acquire(ctx) != nil {
			return Result{State: Rejected}
		}
	}
	client := http.Client{Timeout: 30 * time.Second}
	if t.Client != nil {
		client = *t.Client
		if client.Timeout == 0 {
			client.Timeout = 30 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return unknown
	}
	defer resp.Body.Close()
	out := Result{State: Unknown, StatusCode: resp.StatusCode}
	b, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if err != nil || len(b) > 64<<10 {
		return out
	}
	var raw struct {
		OK        *bool `json:"ok"`
		ErrorCode int   `json:"error_code"`
		Result    struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
		Parameters struct {
			RetryAfter *float64 `json:"retry_after"`
		} `json:"parameters"`
	}
	if json.Unmarshal(b, &raw) != nil || raw.OK == nil {
		return out
	}
	if resp.StatusCode == 200 && *raw.OK {
		out.State = Sent
		out.MessageID = raw.Result.MessageID
		return out
	}
	// A 4xx with explicit ok=false is definite rejection; 5xx or redirects
	// remain unknown, regardless of the response's contents.
	if resp.StatusCode >= 400 && resp.StatusCode < 500 && !*raw.OK {
		out.State = Rejected
		if raw.ErrorCode >= 100 && raw.ErrorCode <= 599 {
			out.ErrorCode = raw.ErrorCode
		}
		if resp.StatusCode == 429 {
			if raw.Parameters.RetryAfter != nil && finiteNonnegative(*raw.Parameters.RetryAfter) {
				out.RetryAfterSeconds = raw.Parameters.RetryAfter
			}
			if out.RetryAfterSeconds == nil {
				if n, e := strconv.ParseFloat(resp.Header.Get("Retry-After"), 64); e == nil && finiteNonnegative(n) {
					out.RetryAfterSeconds = &n
				}
			}
		}
	}
	return out
}

func finiteNonnegative(n float64) bool { return n >= 0 && !math.IsNaN(n) && !math.IsInf(n, 0) }
