package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	execution "github.com/Dzarlax-AI/evening-news/go-execution"
	orchestration "github.com/Dzarlax-AI/evening-news/go-execution/orchestration"
	processing "github.com/Dzarlax-AI/evening-news/go-processing/runner"
)

func operationalCallbacks(base, token string) (orchestration.JobConfigPreparer, processing.OperationalSnapshotReader) {
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request := func(ctx context.Context, method string, path string, body []byte, out *orchestration.JobOperationalConfig) error {
		req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, bytes.NewReader(body))
		if err != nil {
			return errors.New("job operational settings unavailable")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			return errors.New("job operational settings unavailable")
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("job operational settings unavailable: HTTP %d", resp.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 4097))
		if err != nil || len(data) > 4096 {
			return errors.New("job operational settings response exceeded its limit")
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if dec.Decode(out) != nil || dec.Decode(new(any)) != io.EOF {
			return errors.New("job operational settings response is invalid")
		}
		return nil
	}
	prepare := func(ctx context.Context, f execution.ExecutionContext) (orchestration.JobOperationalConfig, error) {
		body, err := json.Marshal(struct {
			JobID             string `json:"job_id"`
			AttemptGeneration int64  `json:"attempt_generation"`
		}{f.JobID, f.AttemptGeneration})
		if err != nil {
			return orchestration.JobOperationalConfig{}, err
		}
		var out orchestration.JobOperationalConfig
		err = request(ctx, http.MethodPost, "/internal/v1/job-configuration", body, &out)
		return out, err
	}
	read := func(ctx context.Context, jobID string, attempt int64) (processing.OperationalSnapshot, error) {
		query := url.Values{"job_id": []string{jobID}, "attempt_generation": []string{fmt.Sprint(attempt)}}
		var out orchestration.JobOperationalConfig
		if err := request(ctx, http.MethodGet, "/internal/v1/job-configuration?"+query.Encode(), nil, &out); err != nil {
			return processing.OperationalSnapshot{}, err
		}
		if out.JobID != jobID || out.AttemptGeneration != attempt {
			return processing.OperationalSnapshot{}, errors.New("job operational settings response does not match the attempt")
		}
		return processing.OperationalSnapshot{Revision: out.Settings.Revision, PinnedAt: out.PinnedAt, LimitsEnabled: out.Settings.LimitsEnabled, MaxArticles: out.Settings.MaxArticles, PerSource: out.Settings.PerSource, Days: out.Settings.Days, NewestDate: out.Settings.NewestDate, ProcessingEnabled: out.Settings.ProcessingEnabled, DeliveryEnabled: out.Settings.DeliveryEnabled, SerbianEnabled: out.Settings.SerbianEnabled}, nil
	}
	return prepare, read
}
