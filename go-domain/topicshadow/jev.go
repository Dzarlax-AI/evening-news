package topicshadow

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

const JevModel = "jev-latest"
const jevEndpoint = "https://api.typesafe.ai/v1/systemone"

type JevTrace struct {
	RequestBody    []byte
	ResponseBody   []byte
	RequestSHA256  string
	ResponseSHA256 string
	StatusCode     int
}
type jevTraceKey struct{}

func WithJevTrace(ctx context.Context, observer func(JevTrace)) context.Context {
	return context.WithValue(ctx, jevTraceKey{}, observer)
}
func jevHash(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }

type JevClient struct {
	apiKey string
	http   *http.Client
}

func NewJevClient(apiKey string, client *http.Client) *JevClient {
	if client == nil {
		client = http.DefaultClient
	}
	isolated := *client
	isolated.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if isolated.Timeout == 0 {
		isolated.Timeout = 30 * time.Second
	}
	return &JevClient{apiKey, &isolated}
}
func (c *JevClient) Decide(ctx context.Context, input string, questions []ChoiceQuestion) (DecisionsResult, error) {
	bad := func() (DecisionsResult, error) { return DecisionsResult{}, &DecisionsError{Code: "invalid_response"} }
	if c == nil || strings.TrimSpace(c.apiKey) == "" || strings.ContainsAny(c.apiKey, "\r\n") {
		return DecisionsResult{}, &DecisionsError{Code: "configuration"}
	}
	if len(input) == 0 || len(input) > maxInputBytes || !utf8Valid(input) {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_request"}
	}
	if err := validateQuestions(questions); err != nil {
		return DecisionsResult{}, err
	}
	type question struct {
		Type         string            `json:"type"`
		Instructions string            `json:"instructions"`
		Criteria     map[string]string `json:"criteria"`
	}
	qs := map[string]question{}
	for _, q := range questions {
		criteria := map[string]string{}
		for _, v := range q.Choices {
			criteria[v.Value] = v.Description
		}
		qs[q.Name] = question{"choice", q.Instructions, criteria}
	}
	body, err := json.Marshal(struct {
		Model     string              `json:"model"`
		State     any                 `json:"state"`
		Questions map[string]question `json:"questions"`
	}{JevModel, map[string]string{"input": input}, qs})
	if err != nil || len(body) > maxRequestBytes {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_request"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, jevEndpoint, bytes.NewReader(body))
	if err != nil {
		return DecisionsResult{}, &DecisionsError{Code: "request"}
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return DecisionsResult{}, &DecisionsError{Code: "transport"}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return DecisionsResult{}, &DecisionsError{Code: statusCode(resp.StatusCode), StatusCode: resp.StatusCode, RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())}
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if observer, ok := ctx.Value(jevTraceKey{}).(func(JevTrace)); ok && observer != nil && err == nil && len(data) <= maxResponseBytes {
		observer(JevTrace{RequestBody: append([]byte(nil), body...), ResponseBody: append([]byte(nil), data...), RequestSHA256: jevHash(body), ResponseSHA256: jevHash(data), StatusCode: resp.StatusCode})
	}
	if err != nil || len(data) > maxResponseBytes || jevDuplicateKeys(data) != nil {
		return bad()
	}
	var wire struct {
		Model   string `json:"model"`
		Answers map[string]struct {
			Type          string             `json:"type"`
			Choice        string             `json:"choice"`
			Probabilities map[string]float64 `json:"probabilities"`
			Confidence    *float64           `json:"confidence"`
			Refusal       string             `json:"refusal"`
		} `json:"answers"`
		Usage struct {
			Input  int `json:"input_tokens"`
			Output int `json:"output_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(data, &wire) != nil || strings.TrimSpace(wire.Model) == "" || len(wire.Answers) != len(qs) || wire.Usage.Input < 0 || wire.Usage.Output < 0 {
		return bad()
	}
	out := DecisionsResult{Model: wire.Model, Usage: TokenUsage{InputTokens: wire.Usage.Input, OutputTokens: wire.Usage.Output, TotalTokens: wire.Usage.Input + wire.Usage.Output}}
	for _, q := range questions {
		a, ok := wire.Answers[q.Name]
		if !ok {
			return bad()
		}
		if a.Type == "refusal" || a.Refusal != "" {
			out.Refusal = "question_refused"
			continue
		}
		if a.Type != "choice" || a.Confidence == nil || math.IsNaN(*a.Confidence) || *a.Confidence < 0 || *a.Confidence > 1 || len(a.Probabilities) != len(q.Choices) {
			return bad()
		}
		allowed := map[string]bool{}
		for _, ch := range q.Choices {
			allowed[ch.Value] = true
		}
		if !allowed[a.Choice] {
			return bad()
		}
		var sum float64
		var probs []ChoiceProbability
		for k, v := range a.Probabilities {
			if !allowed[k] || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1 {
				return bad()
			}
			sum += v
			probs = append(probs, ChoiceProbability{Value: k, Probability: v})
		}
		if math.Abs(sum-1) > 0.02 {
			return bad()
		}
		sort.Slice(probs, func(i, j int) bool { return probs[i].Value < probs[j].Value })
		out.Answers = append(out.Answers, ChoiceAnswer{Name: q.Name, Choice: a.Choice, Probabilities: probs, Confidence: a.Confidence})
	}
	return out, nil
}

// jevDuplicateKeys checks every JSON object before unmarshalling into maps.
func jevDuplicateKeys(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		t, e := d.Token()
		if e != nil {
			return e
		}
		delim, ok := t.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return &DecisionsError{Code: "invalid_response"}
				}
				seen[s] = true
				if e = walk(); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case '[':
			for d.More() {
				if e = walk(); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		}
		return &DecisionsError{Code: "invalid_response"}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return &DecisionsError{Code: "invalid_response"}
	}
	return nil
}
