package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/jsonstrict"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/store"
)

const (
	maxEnvelopeBytes = 8192
	maxReceiptBytes  = 8192
	maxBackoff       = 5 * time.Minute
	maxRetryAfter    = 10 * time.Minute
)

type Dispatcher struct {
	Store       *store.Store
	Targets     map[string]model.Target
	HTTPTimeout time.Duration
	Concurrency int
	client      *http.Client
}

func New(s *store.Store, targets map[string]model.Target, timeout time.Duration, concurrency int) *Dispatcher {
	return &Dispatcher{
		Store: s, Targets: targets, HTTPTimeout: timeout, Concurrency: concurrency,
		client: &http.Client{Timeout: timeout, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

func (d *Dispatcher) Cycle(ctx context.Context, now time.Time) error {
	lease := 30 * time.Second
	if twice := 2 * d.HTTPTimeout; twice > lease {
		lease = twice
	}
	storeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	claimed, err := d.Store.Claim(storeCtx, now.UTC().Truncate(time.Microsecond), lease, d.Concurrency)
	cancel()
	if err != nil {
		return err
	}
	sem := make(chan struct{}, d.Concurrency)
	var wg sync.WaitGroup
	for _, item := range claimed {
		sem <- struct{}{}
		item := item
		wg.Add(1)
		go func() { defer wg.Done(); defer func() { <-sem }(); d.send(ctx, item) }()
	}
	wg.Wait()
	return nil
}

func (d *Dispatcher) send(parent context.Context, item store.Claimed) {
	if parent.Err() != nil {
		return
	}
	target, ok := d.Targets[item.TargetAlias]
	if !ok {
		d.block(parent, item, "target_unavailable", clockNow())
		return
	}
	body, err := json.Marshal(item.Envelope)
	if err != nil || len(body) > maxEnvelopeBytes {
		d.block(parent, item, "invalid_envelope", clockNow())
		return
	}
	ctx, cancel := context.WithTimeout(parent, d.HTTPTimeout)
	defer cancel()
	storeCtx, storeCancel := context.WithTimeout(parent, 5*time.Second)
	_ = d.Store.MarkDispatch(storeCtx, clockNow())
	storeCancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.URL, bytes.NewReader(body))
	if err != nil {
		d.retry(parent, item, "request_failed", "")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+target.Token)
	req.Header.Set("Idempotency-Key", item.Envelope.OccurrenceID)
	resp, err := d.client.Do(req)
	if err != nil {
		d.retry(parent, item, "network_error", "")
		return
	}
	defer resp.Body.Close()
	responseAt := clockNow()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnprocessableEntity {
		d.block(parent, item, "target_rejected", responseAt)
		return
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode >= 500 {
		d.retry(parent, item, "target_unavailable", resp.Header.Get("Retry-After"))
		return
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		d.block(parent, item, "target_rejected", responseAt)
		return
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		d.retry(parent, item, "invalid_receipt", "")
		return
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxReceiptBytes+1))
	if err != nil || len(data) > maxReceiptBytes {
		d.retry(parent, item, "invalid_receipt", "")
		return
	}
	var receipt model.Receipt
	if err := jsonstrict.RejectDuplicateKeys(data); err != nil {
		d.retry(parent, item, "invalid_receipt", "")
		return
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&receipt); err != nil {
		d.retry(parent, item, "invalid_receipt", "")
		return
	}
	var extra any
	if err = dec.Decode(&extra); !errors.Is(err, io.EOF) {
		d.retry(parent, item, "invalid_receipt", "")
		return
	}
	if receipt.Version != 1 || receipt.OccurrenceID != item.Envelope.OccurrenceID || receipt.Status != "accepted" || !canonicalUUID(receipt.JobID) {
		d.retry(parent, item, "invalid_receipt", "")
		return
	}
	completedAt := clockNow()
	storeCtx, storeCancel = cleanupContext(parent)
	defer storeCancel()
	if err = d.Store.Ack(storeCtx, item.Envelope.OccurrenceID, item.LeaseToken, receipt.JobID, completedAt); err != nil {
		return
	}
}

func (d *Dispatcher) retry(parent context.Context, item store.Claimed, code, retryAfter string) {
	ctx, cancel := cleanupContext(parent)
	defer cancel()
	dbNow, err := d.Store.DatabaseNow(ctx)
	if err != nil {
		return
	}
	next := parseRetryAfter(retryAfter, dbNow)
	if next.IsZero() {
		next = dbNow.Add(backoff(item.Attempt))
	}
	if next.After(dbNow.Add(maxRetryAfter)) {
		next = dbNow.Add(maxRetryAfter)
	}
	_ = d.Store.Retry(ctx, item.Envelope.OccurrenceID, item.LeaseToken, code, next, dbNow)
}
func (d *Dispatcher) block(parent context.Context, item store.Claimed, code string, completedAt time.Time) {
	ctx, cancel := cleanupContext(parent)
	defer cancel()
	_ = d.Store.Block(ctx, item.Envelope.OccurrenceID, item.LeaseToken, code, completedAt)
}
func cleanupContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil || parent.Err() != nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, 5*time.Second)
}
func clockNow() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }
func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Second
	for i := 1; i < attempt && d < maxBackoff; i++ {
		d *= 2
	}
	if d > maxBackoff {
		d = maxBackoff
	}
	factor := 0.8 + rand.Float64()*0.4
	return time.Duration(float64(d) * factor)
}
func parseRetryAfter(value string, now time.Time) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	if isDecimal(value) {
		seconds, err := strconv.ParseUint(value, 10, 64)
		maxSeconds := uint64(maxRetryAfter / time.Second)
		if err != nil || seconds > maxSeconds {
			return now.Add(maxRetryAfter)
		}
		return now.Add(time.Duration(seconds) * time.Second)
	}
	if at, err := http.ParseTime(value); err == nil {
		if at.Before(now) {
			return now
		}
		if at.After(now.Add(maxRetryAfter)) {
			return now.Add(maxRetryAfter)
		}
		return at
	}
	return time.Time{}
}
func isDecimal(value string) bool {
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
func canonicalUUID(v string) bool {
	if len(v) != 36 {
		return false
	}
	for i, c := range v {
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
