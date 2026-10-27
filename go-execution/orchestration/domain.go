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
	"regexp"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-execution"
)

const maxDomainBody = 8192
const domainOperationIDNamespace = "evening-news-domain-operation-v1\x00"

var safeCode = regexp.MustCompile(`^[a-z0-9_-]{1,64}$`)
var allowedDomainErrors = map[string]bool{
	"domain_operation_failed": true, "stale_fence": true, "authority_unavailable": true,
	"delivery_unknown": true, "external_outcome_unknown": true, "prepared_digest_missing": true, "resource_limit": true,
}

type DomainOperationError struct{ Code string }

func (e *DomainOperationError) Error() string { return "domain operation failed" }

var ErrStaleFence = errors.New("stale execution fence")
var ErrDomainAuth = errors.New("domain service rejected credentials")

type DomainConfig struct {
	BaseURL      string
	Token        string
	HTTPClient   *http.Client
	PollInterval time.Duration
}

type DomainStatus struct {
	DomainInstanceID string `json:"domain_instance_id"`
	ActiveOperations int    `json:"active_operations"`
	Ready            bool   `json:"ready"`
}

type DomainClient struct {
	cfg  DomainConfig
	http *http.Client
}

func NewDomainClient(cfg DomainConfig) (*DomainClient, error) {
	base, err := validatedBase(cfg.BaseURL)
	if err != nil || cfg.Token == "" {
		return nil, errors.New("domain service configuration is invalid")
	}
	cfg.BaseURL = base
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	return &DomainClient{cfg: cfg, http: client}, nil
}

type domainStatusWire struct {
	DomainInstanceID string `json:"domain_instance_id"`
	ActiveOperations int    `json:"active_operations"`
	Ready            bool   `json:"ready"`
}

func (d *DomainClient) Status(ctx context.Context) (DomainStatus, error) {
	var wire domainStatusWire
	if err := d.request(ctx, http.MethodGet, "/internal/v1/status", "", nil, http.StatusOK, &wire); err != nil {
		return DomainStatus{}, err
	}
	if !canonicalUUID(wire.DomainInstanceID) || wire.ActiveOperations < 0 || !wire.Ready {
		return DomainStatus{}, errors.New("domain service is not ready")
	}
	return DomainStatus{wire.DomainInstanceID, wire.ActiveOperations, wire.Ready}, nil
}

// Ready performs the authenticated domain readiness check used by embedded
// runtimes before they admit worker claims.
func (d *DomainClient) Ready(ctx context.Context) error {
	_, err := d.Status(ctx)
	return err
}

type domainRequest struct {
	Version          int                        `json:"version"`
	Operation        string                     `json:"operation"`
	OperationID      string                     `json:"operation_id"`
	DomainInstanceID string                     `json:"domain_instance_id"`
	Envelope         execution.ExecutionContext `json:"envelope"`
	InputFingerprint string                     `json:"input_fingerprint"`
	Input            json.RawMessage            `json:"input"`
}
type domainReceipt struct {
	Version           int             `json:"version"`
	JobID             string          `json:"job_id"`
	AttemptGeneration int64           `json:"attempt_generation"`
	Operation         string          `json:"operation"`
	OperationID       string          `json:"operation_id"`
	DomainInstanceID  string          `json:"domain_instance_id"`
	InputFingerprint  string          `json:"input_fingerprint"`
	State             string          `json:"state"`
	Result            json.RawMessage `json:"result"`
	ErrorCode         *string         `json:"error_code"`
}

// Execute pins one status-observed Python domain process UUID for this attempt's
// complete orchestration. It never re-posts a leaf operation after an ambiguous
// acknowledgement; only the exact receipt is read until terminal or deadline.
func (d *DomainClient) Execute(ctx context.Context, fence execution.ExecutionContext, op, opID string, input any) (json.RawMessage, error) {
	status, err := d.Status(ctx)
	if err != nil {
		return nil, err
	}
	return d.ExecutePinned(ctx, fence, status.DomainInstanceID, op, opID, input)
}

func (d *DomainClient) ExecutePinned(ctx context.Context, fence execution.ExecutionContext, instance, op, opID string, input any) (json.RawMessage, error) {
	if !allowedOperation(op) || !canonicalUUID(instance) || opID == "" || len(opID) > 96 {
		return nil, errors.New("domain operation is invalid")
	}
	opID = operationUUID(fence, op, opID)
	inputJSON, err := json.Marshal(input)
	if err != nil || len(inputJSON) > 4096 {
		return nil, errors.New("domain operation input is invalid")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, inputJSON); err != nil {
		return nil, errors.New("domain operation input is invalid")
	}
	fp := sha256.Sum256(compact.Bytes())
	fingerprint := hex.EncodeToString(fp[:])
	request := domainRequest{1, op, opID, instance, fence, fingerprint, compact.Bytes()}
	body, err := json.Marshal(request)
	if err != nil || len(body) > maxDomainBody {
		return nil, errors.New("domain operation request exceeded its limit")
	}
	path := "/internal/v1/domain-operations"
	var receipt domainReceipt
	err = d.request(ctx, http.MethodPost, path, instance, body, http.StatusAccepted, &receipt)
	if err != nil {
		// The POST may have committed. Reconcile the exact operation once; never
		// replay the POST, including when the status endpoint says 404.
		receipt, err = d.read(ctx, fence, instance, op, opID)
		if err != nil {
			return nil, fmt.Errorf("domain operation outcome is unknown: %w", err)
		}
	}
	// Validate receipts identically whether they came from the POST response or
	// an ambiguous-ack reconciliation GET.
	if err = validateReceipt(receipt, fence, instance, op, opID, fingerprint); err != nil {
		return nil, err
	}
	delay := d.cfg.PollInterval
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if receipt.State == "succeeded" {
			var resultObject map[string]json.RawMessage
			if len(receipt.Result) == 0 || string(receipt.Result) == "null" || !json.Valid(receipt.Result) || len(receipt.Result) > 4096 || json.Unmarshal(receipt.Result, &resultObject) != nil || resultObject == nil {
				return nil, errors.New("domain operation result is invalid")
			}
			return append(json.RawMessage(nil), receipt.Result...), nil
		}
		if receipt.State == "failed" {
			if receipt.ErrorCode != nil && *receipt.ErrorCode == "stale_fence" {
				return nil, ErrStaleFence
			}
			return nil, &DomainOperationError{Code: stringValue(receipt.ErrorCode)}
		}
		if receipt.State != "accepted" && receipt.State != "running" {
			return nil, errors.New("domain operation state is invalid")
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		case <-t.C:
		}
		receipt, err = d.read(ctx, fence, instance, op, opID)
		if err != nil {
			return nil, fmt.Errorf("domain operation outcome is unknown: %w", err)
		}
		if err := validateReceipt(receipt, fence, instance, op, opID, fingerprint); err != nil {
			return nil, err
		}
		if delay < 2*time.Second {
			delay *= 2
		}
	}
}

func operationUUID(f execution.ExecutionContext, operation, logicalID string) string {
	data, _ := json.Marshal([]string{f.JobID, fmt.Sprint(f.AttemptGeneration), operation, logicalID})
	sum := sha256.Sum256(append([]byte(domainOperationIDNamespace), data...))
	b := append([]byte(nil), sum[:16]...)
	b[6] = b[6]&0x0f | 0x50
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func (d *DomainClient) read(ctx context.Context, fence execution.ExecutionContext, instance, op, opID string) (domainReceipt, error) {
	path := fmt.Sprintf("/internal/v1/domain-operations/%s/%d/%s/%s", url.PathEscape(fence.JobID), fence.AttemptGeneration, url.PathEscape(op), url.PathEscape(opID))
	var receipt domainReceipt
	if err := d.request(ctx, http.MethodGet, path, instance, nil, http.StatusOK, &receipt); err != nil {
		return domainReceipt{}, err
	}
	return receipt, nil
}

func (d *DomainClient) request(ctx context.Context, method, path, instance string, body []byte, want int, out any) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, d.cfg.BaseURL+path, reader)
	if err != nil {
		return errors.New("domain service request failed")
	}
	req.Header.Set("Authorization", "Bearer "+d.cfg.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if instance != "" {
		req.Header.Set("X-Domain-Instance-ID", instance)
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return errors.New("domain service unavailable")
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxDomainBody+1))
	if err != nil || len(data) > maxDomainBody {
		return errors.New("domain service response exceeded its limit")
	}
	if resp.StatusCode != want {
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return ErrDomainAuth
		}
		return fmt.Errorf("domain service returned HTTP %d", resp.StatusCode)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		return errors.New("domain service returned invalid JSON")
	}
	if dec.Decode(new(any)) != io.EOF {
		return errors.New("domain service returned invalid JSON")
	}
	return nil
}

func validateReceipt(r domainReceipt, f execution.ExecutionContext, instance, op, opID, fingerprint string) error {
	if r.Version != 1 || r.JobID != f.JobID || r.AttemptGeneration != f.AttemptGeneration || r.Operation != op || r.OperationID != opID || r.DomainInstanceID != instance || r.InputFingerprint != fingerprint {
		return errors.New("domain service returned a mismatched operation receipt")
	}
	if r.ErrorCode != nil && (!safeCode.MatchString(*r.ErrorCode) || !allowedDomainErrors[*r.ErrorCode]) {
		return errors.New("domain service returned an invalid error code")
	}
	if r.State == "succeeded" && (r.ErrorCode != nil || len(r.Result) == 0 || string(r.Result) == "null") {
		return errors.New("domain service returned an invalid success receipt")
	}
	if r.State == "failed" && (r.ErrorCode == nil || len(r.Result) != 0 && string(r.Result) != "null") {
		return errors.New("domain service returned an invalid failure receipt")
	}
	if r.State != "accepted" && r.State != "running" && r.State != "succeeded" && r.State != "failed" {
		return errors.New("domain service returned an invalid operation state")
	}
	if r.State == "accepted" || r.State == "running" {
		if r.ErrorCode != nil || len(r.Result) != 0 && string(r.Result) != "null" {
			return errors.New("domain service returned an invalid pending receipt")
		}
	}
	return nil
}

func allowedOperation(op string) bool {
	switch op {
	case "summary_status", "summaries_generate", "digest_prepare", "digest_publish", "stats_publish", "media_refresh", "source_test", "backup":
		return true
	}
	return false
}
func validatedBase(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(u.Host, "\r\n") {
		return "", errors.New("service URL is invalid")
	}
	return strings.TrimRight(raw, "/"), nil
}
func canonicalUUID(s string) bool {
	if len(s) != 36 || strings.ToLower(s) != s {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
