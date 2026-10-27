package orchestration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-execution"
)

type ServiceConfig struct {
	URL          string
	Token        string
	HTTPClient   *http.Client
	PollInterval time.Duration
}

type serviceRunClient struct {
	cfg        ServiceConfig
	base, path string
	http       *http.Client
	poll       time.Duration
}

var errRunNotFound = errors.New("job service run not found")

func newServiceRunClient(cfg ServiceConfig, path string) (*serviceRunClient, error) {
	base, err := validatedBase(cfg.URL)
	if err != nil || cfg.Token == "" {
		return nil, errors.New("job service configuration is invalid")
	}
	h := cfg.HTTPClient
	if h == nil {
		h = &http.Client{Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	poll := cfg.PollInterval
	if poll <= 0 {
		poll = 250 * time.Millisecond
	}
	return &serviceRunClient{cfg, base, path, h, poll}, nil
}

type serviceEnvelope struct {
	Version            int    `json:"version"`
	JobID              string `json:"job_id"`
	AttemptGeneration  int64  `json:"attempt_generation"`
	OwnerID            string `json:"owner_id"`
	Generation         int64  `json:"generation"`
	BackendPID         int32  `json:"backend_pid"`
	ExecutorInstanceID string `json:"executor_instance_id"`
	DeadlineAt         string `json:"deadline_at"`
}
type serviceReceipt struct {
	RunID             string          `json:"run_id"`
	ID                string          `json:"id"`
	JobID             string          `json:"job_id"`
	AttemptGeneration int64           `json:"attempt_generation"`
	State             string          `json:"state"`
	Result            json.RawMessage `json:"result"`
}

func (c *serviceRunClient) execute(ctx context.Context, f execution.ExecutionContext) (map[string]any, error) {
	if f.Version != 1 || !canonicalUUID(f.JobID) || !canonicalUUID(f.OwnerID) || !canonicalUUID(f.ExecutorInstanceID) || f.AttemptGeneration < 1 || f.Generation < 1 || f.BackendPID < 1 {
		return nil, errors.New("execution fence is invalid")
	}
	deadline, err := time.Parse(time.RFC3339Nano, f.DeadlineAt)
	if err != nil || !deadline.After(time.Now()) {
		return nil, errors.New("execution deadline is invalid")
	}
	env := serviceEnvelope{f.Version, f.JobID, f.AttemptGeneration, f.OwnerID, f.Generation, f.BackendPID, f.ExecutorInstanceID, f.DeadlineAt}
	body, _ := json.Marshal(env)
	runID := deterministicRunID(c.path, f.JobID, f.AttemptGeneration)
	receipt, err := c.submit(ctx, runID, body)
	if err != nil {
		// Reconcile a lost acknowledgement. The same deterministic ID and exact
		// immutable envelope make a repeat safe only after a confirmed 404.
		readCtx := ctx
		if ctx.Err() != nil {
			var cleanup context.CancelFunc
			readCtx, cleanup = context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanup()
		}
		receipt, err = c.read(readCtx, runID)
		if errors.Is(err, errRunNotFound) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			receipt, err = c.submit(ctx, runID, body)
		}
		if err != nil {
			if ctx.Err() != nil {
				if c.cancelConfirmed(runID, f) {
					return nil, ctx.Err()
				}
				return nil, errors.New("job service cancellation was not confirmed")
			}
			return nil, fmt.Errorf("%s outcome is unknown", c.path)
		}
	}
	if err := c.validate(receipt, runID, f); err != nil {
		return nil, err
	}
	delay := c.poll
	for {
		if err := ctx.Err(); err != nil {
			if c.cancelConfirmed(runID, f) {
				return nil, err
			}
			return nil, errors.New("job service cancellation was not confirmed")
		}
		switch receipt.State {
		case "succeeded":
			if len(receipt.Result) == 0 || !json.Valid(receipt.Result) || len(receipt.Result) > 4096 {
				return nil, errors.New("job service result is invalid")
			}
			var result map[string]any
			if json.Unmarshal(receipt.Result, &result) != nil {
				return nil, errors.New("job service result is invalid")
			}
			return result, nil
		case "failed", "cancelled", "interrupted":
			return nil, errors.New("job service run failed")
		case "running":
		default:
			return nil, errors.New("job service state is invalid")
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			if c.cancelConfirmed(runID, f) {
				return nil, ctx.Err()
			}
			return nil, errors.New("job service cancellation was not confirmed")
		case <-t.C:
		}
		receipt, err = c.read(ctx, runID)
		if err != nil {
			if ctx.Err() != nil {
				if c.cancelConfirmed(runID, f) {
					return nil, ctx.Err()
				}
				return nil, errors.New("job service cancellation was not confirmed")
			}
			if c.cancelConfirmed(runID, f) {
				return nil, err
			}
			return nil, err
		}
		if err := c.validate(receipt, runID, f); err != nil {
			return nil, err
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

func (c *serviceRunClient) cancelConfirmed(runID string, f execution.ExecutionContext) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r, err := c.request(ctx, http.MethodPost, c.path+"/"+url.PathEscape(runID)+"/cancel", nil, 0)
	if err == nil && r.status == http.StatusOK {
		var receipt serviceReceipt
		if json.Unmarshal(r.body, &receipt) == nil && c.validate(receipt, runID, f) == nil && terminalServiceState(receipt.State) {
			return true
		}
	}
	receipt, err := c.read(ctx, runID)
	return err == nil && c.validate(receipt, runID, f) == nil && terminalServiceState(receipt.State)
}

func terminalServiceState(state string) bool {
	return state == "succeeded" || state == "failed" || state == "cancelled" || state == "interrupted"
}

func (c *serviceRunClient) submit(ctx context.Context, runID string, body []byte) (serviceReceipt, error) {
	// A 429 is a confirmed rejection and may be retried under the attempt deadline.
	for {
		r, err := c.request(ctx, http.MethodPost, c.path, body, 0)
		if err != nil {
			return serviceReceipt{}, err
		}
		if r.status == http.StatusTooManyRequests {
			wait := time.Second
			if v, e := time.ParseDuration(r.retryAfter + "s"); e == nil && r.retryAfter != "" {
				wait = v
			}
			if wait < 100*time.Millisecond {
				wait = 100 * time.Millisecond
			}
			if wait > 5*time.Second {
				wait = 5 * time.Second
			}
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return serviceReceipt{}, ctx.Err()
			case <-t.C:
			}
			continue
		}
		if r.status != http.StatusOK && r.status != http.StatusAccepted {
			return serviceReceipt{}, fmt.Errorf("job service rejected request with HTTP %d", r.status)
		}
		var rec serviceReceipt
		if json.Unmarshal(r.body, &rec) != nil {
			return serviceReceipt{}, errors.New("job service returned invalid receipt")
		}
		// Ingestion 202 may return only {run_id,state}; fetch the canonical receipt.
		if rec.JobID == "" {
			return c.read(ctx, runID)
		}
		return rec, nil
	}
}

type serviceResponse struct {
	status     int
	body       []byte
	retryAfter string
}

func (c *serviceRunClient) request(ctx context.Context, method, path string, body []byte, expected int) (serviceResponse, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return serviceResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return serviceResponse{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16385))
	if err != nil || len(data) > 16384 {
		return serviceResponse{}, errors.New("job service response exceeded its limit")
	}
	if expected != 0 && resp.StatusCode != expected {
		return serviceResponse{}, fmt.Errorf("job service returned HTTP %d", resp.StatusCode)
	}
	return serviceResponse{resp.StatusCode, data, resp.Header.Get("Retry-After")}, nil
}
func (c *serviceRunClient) read(ctx context.Context, runID string) (serviceReceipt, error) {
	path := c.path + "/" + url.PathEscape(runID)
	r, err := c.request(ctx, http.MethodGet, path, nil, 0)
	if err != nil {
		return serviceReceipt{}, err
	}
	if r.status == http.StatusNotFound {
		return serviceReceipt{}, errRunNotFound
	}
	if r.status != http.StatusOK {
		return serviceReceipt{}, fmt.Errorf("job service returned HTTP %d", r.status)
	}
	var rec serviceReceipt
	if json.Unmarshal(r.body, &rec) != nil {
		return serviceReceipt{}, errors.New("job service returned invalid receipt")
	}
	return rec, nil
}
func (c *serviceRunClient) validate(r serviceReceipt, runID string, f execution.ExecutionContext) error {
	receivedID := r.RunID
	if receivedID == "" {
		receivedID = r.ID
	}
	if receivedID != runID || r.JobID != f.JobID || r.AttemptGeneration != f.AttemptGeneration {
		return errors.New("job service returned a mismatched receipt")
	}
	return nil
}

func deterministicRunID(path, job string, attempt int64) string {
	service := "ingestion"
	if strings.Contains(path, "processing") {
		service = "processing"
	}
	sum := sha256.Sum256([]byte("evening-news-" + service + "-v1\x00" + job + "\x00" + fmt.Sprint(attempt)))
	b := append([]byte(nil), sum[:16]...)
	b[6] = b[6]&0x0f | 0x50
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
