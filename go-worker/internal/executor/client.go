package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var ErrNotFound = errors.New("executor receipt not found")

type HTTPError struct {
	Status int
}

func (e *HTTPError) Error() string { return fmt.Sprintf("executor returned HTTP %d", e.Status) }

type Client struct {
	base  string
	token string
	http  *http.Client
}

type Status struct {
	InstanceID       string `json:"executor_instance_id"`
	ActiveExecutions int    `json:"active_executions"`
	Ready            bool   `json:"ready"`
}

type Policy struct {
	TimeoutSeconds int `json:"timeout_seconds"`
}

type Envelope struct {
	Version           int       `json:"version"`
	JobID             string    `json:"job_id"`
	OwnerID           string    `json:"owner_id"`
	Generation        int64     `json:"generation"`
	AttemptGeneration int64     `json:"attempt_generation"`
	DeadlineAt        time.Time `json:"deadline_at"`
}

type Receipt struct {
	JobID             string          `json:"job_id"`
	OwnerID           string          `json:"owner_id"`
	Generation        int64           `json:"generation"`
	AttemptGeneration int64           `json:"attempt_generation"`
	DeadlineAt        string          `json:"deadline_at"`
	ExecutorInstance  string          `json:"executor_instance_id"`
	State             string          `json:"state"`
	AcceptedAt        *string         `json:"accepted_at"`
	StartedAt         *string         `json:"started_at"`
	FinishedAt        *string         `json:"finished_at"`
	Result            json.RawMessage `json:"result"`
	ErrorCode         *string         `json:"error_code"`
	LegacyApplied     bool            `json:"legacy_completion_applied"`
	ExecutionStopped  bool            `json:"execution_stopped"`
	CurrentInstance   string          `json:"current_executor_instance_id"`
}

type WorkerEvent struct {
	Event      string `json:"event"`
	OwnerID    string `json:"owner_id"`
	Generation int64  `json:"generation"`
}

func New(baseURL, token string, timeout time.Duration) (*Client, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || (parsed.Path != "" && parsed.Path != "/") {
		return nil, errors.New("invalid executor base URL")
	}
	return &Client{
		base: strings.TrimRight(baseURL, "/"), token: token,
		http: &http.Client{
			Timeout: timeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (c *Client) Status(ctx context.Context) (Status, error) {
	var status Status
	err := c.request(ctx, http.MethodGet, "/internal/v1/status", nil, &status, false)
	if err == nil && (!status.Ready || status.InstanceID == "") {
		return Status{}, errors.New("executor is not ready")
	}
	return status, err
}

func (c *Client) Policy(ctx context.Context, jobID string) (Policy, error) {
	var policy Policy
	err := c.request(ctx, http.MethodGet, "/internal/v1/job-executions/"+url.PathEscape(jobID)+"/policy", nil, &policy, false)
	if err == nil && (policy.TimeoutSeconds < 1 || policy.TimeoutSeconds > 86400) {
		return Policy{}, errors.New("executor returned an invalid timeout")
	}
	return policy, err
}

func (c *Client) Accept(ctx context.Context, envelope Envelope) error {
	return c.request(ctx, http.MethodPost, "/internal/v1/job-executions", envelope, nil, false)
}

func (c *Client) Receipt(ctx context.Context, jobID string, attempt int64) (Receipt, error) {
	var receipt Receipt
	path := "/internal/v1/job-executions/" + url.PathEscape(jobID) + "/" + strconv.FormatInt(attempt, 10)
	err := c.request(ctx, http.MethodGet, path, nil, &receipt, true)
	if err == nil && (receipt.JobID != jobID || receipt.AttemptGeneration != attempt || !validReceipt(receipt)) {
		return Receipt{}, errors.New("executor returned a mismatched receipt")
	}
	if err == nil && len(receipt.Result) > 4096 {
		return Receipt{}, errors.New("executor receipt result exceeds its safe bound")
	}
	return receipt, err
}

func (c *Client) Cancel(ctx context.Context, jobID string, attempt int64) (Receipt, error) {
	var receipt Receipt
	path := "/internal/v1/job-executions/" + url.PathEscape(jobID) + "/" + strconv.FormatInt(attempt, 10) + "/cancel"
	err := c.request(ctx, http.MethodPost, path, nil, &receipt, true)
	if err == nil && (receipt.JobID != jobID || receipt.AttemptGeneration != attempt || !validReceipt(receipt)) {
		return Receipt{}, errors.New("executor returned a mismatched cancellation receipt")
	}
	if err == nil && len(receipt.Result) > 4096 {
		return Receipt{}, errors.New("executor receipt result exceeds its safe bound")
	}
	return receipt, err
}

func knownReceiptState(state string) bool {
	switch state {
	case "accepted", "running", "succeeded", "failed", "interrupted", "delivery_unknown":
		return true
	default:
		return false
	}
}

func validReceipt(receipt Receipt) bool {
	if !knownReceiptState(receipt.State) || receipt.OwnerID == "" || receipt.Generation <= 0 ||
		receipt.ExecutorInstance == "" || receipt.DeadlineAt == "" {
		return false
	}
	deadline, err := time.Parse(time.RFC3339Nano, receipt.DeadlineAt)
	return err == nil && !deadline.IsZero()
}

func (c *Client) Finalized(ctx context.Context, jobID string, attempt int64) error {
	path := "/internal/v1/job-executions/" + url.PathEscape(jobID) + "/" + strconv.FormatInt(attempt, 10) + "/events"
	return c.request(ctx, http.MethodPost, path, map[string]string{"event": "finalized"}, nil, false)
}

func (c *Client) Recovered(ctx context.Context, ownerID string, generation int64) error {
	return c.request(ctx, http.MethodPost, "/internal/v1/worker-events",
		WorkerEvent{Event: "recovered", OwnerID: ownerID, Generation: generation}, nil, false)
}

func (c *Client) request(ctx context.Context, method, path string, body any, target any, missingOK bool) error {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return errors.New("could not encode executor request")
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		return errors.New("could not create executor request")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(req)
	if err != nil {
		return errors.New("executor request failed")
	}
	defer response.Body.Close()
	if missingOK && response.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
		return &HTTPError{Status: response.StatusCode}
	}
	if target == nil {
		encoded, err := io.ReadAll(io.LimitReader(response.Body, 8193))
		if err != nil {
			return errors.New("executor response could not be read")
		}
		if len(encoded) > 8192 {
			return errors.New("executor response exceeds its safe bound")
		}
		return nil
	}
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil {
		return errors.New("executor response could not be read")
	}
	if len(encoded) > 8192 {
		return errors.New("executor response exceeds its safe bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("executor response was invalid")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("executor response has trailing data")
	}
	return nil
}
