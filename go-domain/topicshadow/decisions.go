package topicshadow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	decisionsEndpoint = "https://api.openai.com/v1/decisions"
	DecisionsModel    = "gpt-6-luna"
	maxInputBytes     = 64 << 10
	maxQuestions      = 20
	maxChoices        = 32
	maxResponseBytes  = 1 << 20
	maxRequestBytes   = 128 << 10
)

var questionNamePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,63}$`)

type ChoiceOption struct {
	Value       string `json:"value"`
	Description string `json:"description"`
}

type ChoiceQuestion struct {
	Name         string         `json:"name"`
	Instructions string         `json:"instructions"`
	Choices      []ChoiceOption `json:"choices"`
}

type ChoiceProbability struct {
	Value       string  `json:"value"`
	Probability float64 `json:"probability"`
}

type ChoiceAnswer struct {
	Name          string              `json:"name"`
	Choice        string              `json:"choice"`
	Probabilities []ChoiceProbability `json:"probabilities,omitempty"`
	Confidence    *float64            `json:"confidence,omitempty"`
}

type TokenUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type DecisionsResult struct {
	Answers []ChoiceAnswer
	Refusal string
	Usage   TokenUsage
	Model   string
}

// DecisionsError contains only safe, normalized error information. Provider
// response bodies and request credentials are never retained in its fields.
type DecisionsError struct {
	Code       string
	StatusCode int
	RetryAfter time.Duration
}

func (e *DecisionsError) Error() string {
	if e == nil {
		return "decisions request failed"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("decisions request failed (%s, HTTP %d)", e.Code, e.StatusCode)
	}
	return "decisions request failed (" + e.Code + ")"
}

type DecisionsClient struct {
	apiKey string
	http   *http.Client
}

func NewDecisionsClient(apiKey string, client *http.Client) *DecisionsClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &DecisionsClient{apiKey: apiKey, http: client}
}

// Decide makes exactly one HTTP request. Retry policy belongs to the caller.
func (c *DecisionsClient) Decide(ctx context.Context, input string, questions []ChoiceQuestion) (DecisionsResult, error) {
	if c == nil || strings.TrimSpace(c.apiKey) == "" {
		return DecisionsResult{}, &DecisionsError{Code: "configuration"}
	}
	if len(input) == 0 || len(input) > maxInputBytes || !utf8Valid(input) {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_request"}
	}
	if err := validateQuestions(questions); err != nil {
		return DecisionsResult{}, err
	}
	payload := struct {
		Model     string `json:"model"`
		Input     string `json:"input"`
		Questions []struct {
			Type         string         `json:"type"`
			Name         string         `json:"name"`
			Instructions string         `json:"instructions"`
			Choices      []ChoiceOption `json:"choices"`
		} `json:"questions"`
	}{Model: DecisionsModel, Input: input}
	for _, q := range questions {
		payload.Questions = append(payload.Questions, struct {
			Type         string         `json:"type"`
			Name         string         `json:"name"`
			Instructions string         `json:"instructions"`
			Choices      []ChoiceOption `json:"choices"`
		}{Type: "choice", Name: q.Name, Instructions: q.Instructions, Choices: q.Choices})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_request"}
	}
	if len(body) > maxRequestBytes {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_request"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, decisionsEndpoint, bytes.NewReader(body))
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
	if err != nil || len(data) > maxResponseBytes {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_response"}
	}
	var wire struct {
		Model   string `json:"model"`
		Refusal string `json:"refusal"`
		Answers []struct {
			Type          string              `json:"type"`
			Name          string              `json:"name"`
			Choice        string              `json:"choice"`
			Probabilities []ChoiceProbability `json:"probabilities"`
			Confidence    *float64            `json:"confidence"`
			Refusal       string              `json:"refusal"`
		} `json:"answers"`
		Usage TokenUsage `json:"usage"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_response"}
	}
	result := DecisionsResult{Model: wire.Model, Usage: wire.Usage, Refusal: wire.Refusal}
	if result.Model == "" {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_response"}
	}
	if result.Refusal != "" {
		return result, nil
	}
	known := make(map[string]map[string]bool, len(questions))
	for _, q := range questions {
		choices := make(map[string]bool, len(q.Choices))
		for _, ch := range q.Choices {
			choices[ch.Value] = true
		}
		known[q.Name] = choices
	}
	seen := make(map[string]bool, len(questions))
	for _, answer := range wire.Answers {
		if answer.Refusal != "" {
			result.Refusal = answer.Refusal
			return result, nil
		}
		if answer.Type != "choice" || known[answer.Name] == nil || seen[answer.Name] || !known[answer.Name][answer.Choice] {
			return DecisionsResult{}, &DecisionsError{Code: "invalid_response"}
		}
		if answer.Confidence != nil && (*answer.Confidence < 0 || *answer.Confidence > 1) {
			return DecisionsResult{}, &DecisionsError{Code: "invalid_response"}
		}
		seen[answer.Name] = true
		probabilityValues := make(map[string]bool, len(answer.Probabilities))
		for _, p := range answer.Probabilities {
			if !known[answer.Name][p.Value] || probabilityValues[p.Value] || p.Probability < 0 || p.Probability > 1 {
				return DecisionsResult{}, &DecisionsError{Code: "invalid_response"}
			}
			probabilityValues[p.Value] = true
		}
		result.Answers = append(result.Answers, ChoiceAnswer{Name: answer.Name, Choice: answer.Choice, Probabilities: answer.Probabilities, Confidence: answer.Confidence})
	}
	if len(seen) != len(questions) {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_response"}
	}
	return result, nil
}

func validateQuestions(questions []ChoiceQuestion) error {
	if len(questions) == 0 || len(questions) > maxQuestions {
		return &DecisionsError{Code: "invalid_request"}
	}
	seen := map[string]bool{}
	for _, q := range questions {
		if !questionNamePattern.MatchString(q.Name) || seen[q.Name] || strings.TrimSpace(q.Instructions) == "" || len(q.Instructions) > 4096 || len(q.Choices) < 2 || len(q.Choices) > maxChoices {
			return &DecisionsError{Code: "invalid_request"}
		}
		seen[q.Name] = true
		values := map[string]bool{}
		for _, choice := range q.Choices {
			if strings.TrimSpace(choice.Value) == "" || len(choice.Value) > 128 || strings.TrimSpace(choice.Description) == "" || len(choice.Description) > 2048 || values[choice.Value] {
				return &DecisionsError{Code: "invalid_request"}
			}
			values[choice.Value] = true
		}
	}
	return nil
}

func statusCode(status int) string {
	switch status {
	case http.StatusTooManyRequests:
		return "rate_limited"
	case http.StatusUnauthorized, http.StatusForbidden:
		return "authentication"
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return "invalid_request"
	default:
		return "provider_error"
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

func utf8Valid(s string) bool { return utf8.ValidString(s) }
