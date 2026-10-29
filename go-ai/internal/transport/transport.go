package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const MaxRequest = 8 << 20
const MaxResponse = 2 << 20

var modelID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

// Validate rejects malformed inputs before any network activity.
func Validate(cfg ai.Config, r ai.Request, provider string) error {
	if strings.TrimSpace(cfg.APIKey) == "" || !modelID.MatchString(r.Model) {
		return ai.NewError(provider, "configuration", nil)
	}
	if r.Temperature != nil && (math.IsNaN(*r.Temperature) || math.IsInf(*r.Temperature, 0) || *r.Temperature < 0 || *r.Temperature > 2) {
		return ai.NewError(provider, "invalid_options", nil)
	}
	if r.MaxOutputTokens != nil && *r.MaxOutputTokens <= 0 {
		return ai.NewError(provider, "invalid_options", nil)
	}
	if cfg.BaseURL != "" {
		u, err := url.Parse(cfg.BaseURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
			return ai.NewError(provider, "configuration", nil)
		}
	}
	return nil
}

func contextError(provider string, err error) *ai.Error {
	kind := "transport"
	retry := true
	var cause error
	if errors.Is(err, context.Canceled) {
		kind = "cancelled"
		retry = false
		cause = context.Canceled
	} else if errors.Is(err, context.DeadlineExceeded) {
		kind = "timeout"
		cause = context.DeadlineExceeded
	}
	e := ai.NewError(provider, kind, cause)
	e.Retryable = retry
	return e
}
func safeID(s string) string {
	if len(s) > 256 {
		return ""
	}
	for _, c := range s {
		if c < 0x21 || c > 0x7e {
			return ""
		}
	}
	return s
}
func retryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	digits := true
	for _, r := range v {
		if r < '0' || r > '9' {
			digits = false
			break
		}
	}
	if digits {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n > uint64(math.MaxInt64/int64(time.Second)) {
			return time.Duration(math.MaxInt64)
		}
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}

func Do(ctx context.Context, cfg ai.Config, provider, endpoint, authHeader, authValue string, body any) ([]byte, string, error) {
	b, err := json.Marshal(body)
	if err != nil || len(b) > MaxRequest {
		return nil, "", ai.NewError(provider, "request_too_large_or_invalid", nil)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(b))
	if err != nil {
		return nil, "", ai.NewError(provider, "configuration", nil)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authHeader, authValue)
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := copyClient.Do(req)
	if err != nil {
		return nil, "", contextError(provider, err)
	}
	defer resp.Body.Close()
	id := safeID(resp.Header.Get("x-request-id"))
	if resp.StatusCode != http.StatusOK {
		e := ai.NewError(provider, "http", nil)
		e.StatusCode = resp.StatusCode
		e.RequestID = id
		e.Retryable = resp.StatusCode == 429 || resp.StatusCode >= 500
		e.RetryAfter = retryAfter(resp.Header.Get("Retry-After"))
		return nil, id, e
	}
	b, err = io.ReadAll(io.LimitReader(resp.Body, MaxResponse+1))
	if err != nil {
		if ctx.Err() != nil {
			e := contextError(provider, ctx.Err())
			e.RequestID = id
			return nil, id, e
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			e := contextError(provider, err)
			e.RequestID = id
			return nil, id, e
		}
		e := ai.NewError(provider, "read", nil)
		e.RequestID = id
		return nil, id, e
	}
	if len(b) > MaxResponse {
		return nil, id, ai.NewError(provider, "response_too_large", nil)
	}
	return b, id, nil
}
