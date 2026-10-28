package authority

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/privateapi"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrAuthorityUnavailable = errors.New("execution authority unavailable")
var canonicalUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var deadlinePattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?(Z|[+-]([01][0-9]|2[0-3]):[0-5][0-9])$`)

type Envelope struct {
	Version            int    `json:"version"`
	JobID              string `json:"job_id"`
	AttemptGeneration  int    `json:"attempt_generation"`
	OwnerID            string `json:"owner_id"`
	Generation         int    `json:"generation"`
	BackendPID         int    `json:"backend_pid"`
	ExecutorInstanceID string `json:"executor_instance_id"`
	DeadlineAt         string `json:"deadline_at"`
}

func (e Envelope) deadline() (time.Time, error) {
	if e.Version != 1 || !canonicalUUID.MatchString(e.JobID) || !canonicalUUID.MatchString(e.OwnerID) || !canonicalUUID.MatchString(e.ExecutorInstanceID) || e.AttemptGeneration <= 0 || e.AttemptGeneration > 2147483647 || e.Generation <= 0 || e.Generation > 2147483647 || e.BackendPID <= 0 || e.BackendPID > 2147483647 || !deadlinePattern.MatchString(e.DeadlineAt) {
		return time.Time{}, ErrAuthorityUnavailable
	}
	d, err := time.Parse(time.RFC3339Nano, e.DeadlineAt)
	if err != nil {
		return time.Time{}, ErrAuthorityUnavailable
	}
	return d.UTC(), nil
}

// Deadline validates the full envelope and returns its UTC deadline.
func (e Envelope) Deadline() (time.Time, error) { return e.deadline() }
func (e Envelope) Matches(other Envelope) bool {
	a, err := e.deadline()
	if err != nil {
		return false
	}
	b, err := other.deadline()
	if err != nil {
		return false
	}
	e.DeadlineAt, other.DeadlineAt = "", ""
	return e == other && a.Equal(b)
}

type ExecutionContext struct {
	Version            int             `json:"version"`
	Envelope           Envelope        `json:"envelope"`
	ExecutorBackendPID int             `json:"executor_backend_pid"`
	Kind               string          `json:"kind"`
	Payload            json.RawMessage `json:"payload"`
}

func (c ExecutionContext) Fence(pool *pgxpool.Pool) Fence {
	d, _ := c.Envelope.deadline()
	return Fence{Pool: pool, JobID: c.Envelope.JobID, OwnerID: c.Envelope.OwnerID, ExecutorInstanceID: c.Envelope.ExecutorInstanceID, Kind: c.Kind, AttemptGeneration: c.Envelope.AttemptGeneration, Generation: c.Envelope.Generation, WorkerBackendPID: c.Envelope.BackendPID, ExecutorBackendPID: c.ExecutorBackendPID, Deadline: d, Payload: c.Payload}
}

type ContextClient struct {
	base, token string
	client      *http.Client
}

// Close releases idle authority connections during domain shutdown.
func (c *ContextClient) Close() { c.client.CloseIdleConnections() }

func NewContextClient(base, token string) (*ContextClient, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || strings.ContainsAny(base, " \t\r\n") || token == "" {
		return nil, ErrAuthorityUnavailable
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return &ContextClient{base: strings.TrimRight(base, "/"), token: token, client: &http.Client{Transport: transport, Timeout: 4 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *ContextClient) Get(ctx context.Context, jobID string, attempt int) (ExecutionContext, error) {
	var out ExecutionContext
	if !canonicalUUID.MatchString(jobID) || attempt <= 0 || attempt > 2147483647 {
		return out, ErrAuthorityUnavailable
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/internal/v1/job-executions/%s/%d/context", c.base, jobID, attempt), nil)
	if err != nil {
		return out, ErrAuthorityUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return out, ErrAuthorityUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return out, ErrAuthorityUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024+1))
	if err != nil || len(raw) > 16*1024 {
		return out, ErrAuthorityUnavailable
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&out); err != nil {
		return ExecutionContext{}, ErrAuthorityUnavailable
	}
	if err = dec.Decode(new(any)); err != io.EOF {
		return ExecutionContext{}, ErrAuthorityUnavailable
	}
	deadline, err := out.Envelope.deadline()
	var payload map[string]json.RawMessage
	if err != nil || !deadline.After(time.Now()) || out.Version != 1 || out.Envelope.JobID != jobID || out.Envelope.AttemptGeneration != attempt || out.ExecutorBackendPID <= 0 || out.Kind == "" || json.Unmarshal(out.Payload, &payload) != nil || payload == nil {
		return ExecutionContext{}, ErrAuthorityUnavailable
	}
	return out, nil
}

// Instance returns the currently ready Go execution instance for terminal
// events, which can outlive an individual execution context deadline.
func (c *ContextClient) Instance(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/internal/v1/status", nil)
	if err != nil {
		return "", ErrAuthorityUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return "", ErrAuthorityUnavailable
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", ErrAuthorityUnavailable
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
	if err != nil || len(raw) > 4096 {
		return "", ErrAuthorityUnavailable
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 3 || fields["active_executions"] == nil || bytes.Equal(bytes.TrimSpace(fields["active_executions"]), []byte("null")) {
		return "", ErrAuthorityUnavailable
	}
	var body struct {
		ExecutorInstanceID string `json:"executor_instance_id"`
		ActiveExecutions   int    `json:"active_executions"`
		Ready              bool   `json:"ready"`
	}
	if privateapi.DecodeJSON(raw, &body) != nil || !body.Ready || body.ActiveExecutions < 0 || !canonicalUUID.MatchString(body.ExecutorInstanceID) {
		return "", ErrAuthorityUnavailable
	}
	return body.ExecutorInstanceID, nil
}
