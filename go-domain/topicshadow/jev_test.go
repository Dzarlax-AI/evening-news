package topicshadow

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

type jevRoundTrip func(*http.Request) (*http.Response, error)

func (f jevRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestJevWireAndValidation(t *testing.T) {
	response := `{"model":"jev-1.13.0","answers":{"x":{"type":"choice","choice":"yes","probabilities":{"yes":0.9,"no":0.1},"confidence":0.9}},"usage":{"input_tokens":2,"output_tokens":1}}`
	transport := jevRoundTrip(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		var v map[string]json.RawMessage
		if json.Unmarshal(b, &v) != nil || len(v["state"]) == 0 || len(v["questions"]) == 0 {
			t.Error("invalid request shape")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(response))}, nil
	})
	c := NewJevClient("key", &http.Client{Transport: transport})
	q := []ChoiceQuestion{{Name: "x", Instructions: "test", Choices: []ChoiceOption{{Value: "yes", Description: "yes"}, {Value: "no", Description: "no"}}}}
	var trace JevTrace
	got, err := c.Decide(WithJevTrace(context.Background(), func(x JevTrace) { trace = x }), "input", q)
	if err != nil || len(got.Answers) != 1 || len(got.Answers[0].Probabilities) != 2 || trace.StatusCode != 200 || trace.RequestSHA256 == "" {
		t.Fatalf("result: %+v, %v", got, err)
	}
	response = strings.Replace(response, `"yes":0.9`, `"yes":0.9,"yes":0.9`, 1)
	if _, err = c.Decide(context.Background(), "input", q); err == nil {
		t.Fatal("accepted duplicate key")
	}
	response = strings.Repeat("x", maxResponseBytes+1)
	if _, err = c.Decide(context.Background(), "input", q); err == nil {
		t.Fatal("accepted oversized response")
	}
}

type jevFake struct{ rumor bool }

func (f jevFake) Decide(_ context.Context, _ string, qs []ChoiceQuestion) (DecisionsResult, error) {
	out := DecisionsResult{Model: JevModel}
	for _, q := range qs {
		value := q.Choices[0].Value
		switch q.Name {
		case "decision":
			value = MatchOutcomeNewCandidate
		case "relation":
			value = NoveltyOutcomeMaterialUpdate
		case "official_change":
			value = "yes"
		case "evidence":
			value = q.Choices[0].Value
		}
		probs := make([]ChoiceProbability, 0, len(q.Choices))
		for _, c := range q.Choices {
			p := 0.0
			if c.Value == value {
				p = 1
			}
			probs = append(probs, ChoiceProbability{Value: c.Value, Probability: p})
		}
		out.Answers = append(out.Answers, ChoiceAnswer{Name: q.Name, Choice: value, Probabilities: probs})
	}
	return out, nil
}
func TestJevNewSeedRequiresConfirmedEvidence(t *testing.T) {
	for _, tc := range []struct {
		summary string
		want    string
	}{{"The council approved the new law.", MatchOutcomeNewCandidate}, {"Rumor says the council approved the new law.", MatchOutcomeReview}} {
		m, n, err := ClassifyJev(context.Background(), jevFake{}, ArticleInput{Summary: tc.summary}, nil)
		if err != nil || m.Outcome != tc.want {
			t.Fatalf("%q: %+v %+v %v", tc.summary, m, n, err)
		}
		if tc.want == MatchOutcomeNewCandidate && (!n.ConcreteStatusChange || n.Outcome != NoveltyOutcomeMaterialUpdate || n.EvidenceSpan == "") {
			t.Fatalf("missing novelty: %+v", n)
		}
	}
}
