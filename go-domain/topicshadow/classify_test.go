package topicshadow

import (
	"context"
	"strings"
	"testing"
)

type fakeDecisions struct {
	result  DecisionsResult
	results []DecisionsResult
	err     error
	calls   int
	input   string
	qs      []ChoiceQuestion
	inputs  []string
	allQs   [][]ChoiceQuestion
}

func (f *fakeDecisions) Decide(_ context.Context, input string, questions []ChoiceQuestion) (DecisionsResult, error) {
	f.calls++
	f.input, f.qs = input, questions
	f.inputs = append(f.inputs, input)
	f.allQs = append(f.allQs, questions)
	if len(f.results) > 0 {
		result := f.results[0]
		f.results = f.results[1:]
		return result, f.err
	}
	return f.result, f.err
}

func ans(name, choice string, p float64) ChoiceAnswer {
	return ChoiceAnswer{Name: name, Choice: choice, Probabilities: []ChoiceProbability{{Value: choice, Probability: p}, {Value: "runner-up", Probability: 1 - p}}}
}

func TestMatchRequiresAnchoredExactReferentAndThreshold(t *testing.T) {
	article := ArticleInput{ArticleID: 8, Title: "Belgrade opens new bridge", Summary: "The new bridge opened today."}
	card := TopicCard{ID: "bridge-1", Title: "Belgrade bridge project", Aliases: []string{"new bridge"}}
	result := DecisionsResult{Model: "gpt-6-luna-2026-11-20", Usage: TokenUsage{TotalTokens: 42}, Answers: []ChoiceAnswer{ans("decision", "topic:bridge-1", .91), ans("evidence", "title_01", .9)}}
	fake := &fakeDecisions{result: result}
	got, err := (Classifier{Client: fake}).Match(context.Background(), article, []TopicCard{card})
	if err != nil {
		t.Fatal(err)
	}
	if fake.calls != 1 || got.Outcome != MatchOutcomeMatch || got.TopicID != card.ID || got.EvidenceExcerptID != "title_01" || got.Model != result.Model || got.Usage.TotalTokens != 42 {
		t.Fatalf("unexpected match: %+v calls=%d", got, fake.calls)
	}
	if len(got.ProbabilityJudgments) != 2 {
		t.Fatalf("probability judgments missing: %+v", got.ProbabilityJudgments)
	}

	result.Answers[0] = ans("decision", "topic:bridge-1", .84)
	fake = &fakeDecisions{result: result}
	got, err = (Classifier{Client: fake}).Match(context.Background(), article, []TopicCard{card})
	if err != nil || got.Outcome != MatchOutcomeReview {
		t.Fatalf("below threshold should review: %+v err=%v", got, err)
	}

	article.Title = "It opened after years of delay"
	result.Answers[0] = ans("decision", "topic:bridge-1", .99)
	fake = &fakeDecisions{result: result}
	got, err = (Classifier{Client: fake}).Match(context.Background(), article, []TopicCard{card})
	if err != nil || got.Outcome != MatchOutcomeReview {
		t.Fatalf("unanchored referent must review: %+v err=%v", got, err)
	}
}

func TestMatchNewCandidateAndRefusal(t *testing.T) {
	article := ArticleInput{ArticleID: 2, Title: "Town plans a recurring night market"}
	result := DecisionsResult{Model: DecisionsModel, Answers: []ChoiceAnswer{ans("decision", MatchOutcomeNewCandidate, .52), ans("evidence", "title_01", .8)}}
	fake := &fakeDecisions{result: result}
	got, err := (Classifier{Client: fake}).Match(context.Background(), article, nil)
	if err != nil || got.Outcome != MatchOutcomeNewCandidate || fake.calls != 1 {
		t.Fatalf("unexpected candidate: %+v err=%v", got, err)
	}
	fake = &fakeDecisions{result: DecisionsResult{Model: DecisionsModel, Refusal: "cannot classify", Usage: TokenUsage{TotalTokens: 4}}}
	got, err = (Classifier{Client: fake}).Match(context.Background(), article, nil)
	if err != nil || got.Outcome != MatchOutcomeReview || got.Usage.TotalTokens != 4 {
		t.Fatalf("refusal should review: %+v err=%v", got, err)
	}
}

func TestMatchReviewsUnresolvedCyrillicDeathReferent(t *testing.T) {
	article := ArticleInput{ArticleID: 22, Title: "N1: информация о смерти не подтверждена", Summary: "Не уточняется, о чьей смерти идет речь."}
	card := TopicCard{ID: "n1-death", Title: "N1 death report", Aliases: []string{"информация о смерти"}}
	fake := &fakeDecisions{result: DecisionsResult{Model: DecisionsModel, Answers: []ChoiceAnswer{ans("decision", "topic:n1-death", .99), ans("evidence", "title_01", .98)}}}
	got, err := (Classifier{Client: fake}).Match(context.Background(), article, []TopicCard{card})
	if err != nil {
		t.Fatal(err)
	}
	if got.Outcome != MatchOutcomeReview {
		t.Fatalf("ambiguous death referent must be reviewed: %+v", got)
	}
}

func TestMatchAllowsConservativeHeadlineOverlap(t *testing.T) {
	article := ArticleInput{ArticleID: 9, Title: "Belgrade bridge project adds transit"}
	card := TopicCard{ID: "bridge-1", Title: "Belgrade bridge project opens", Context: "Focal topic: the municipal bridge construction and its transit connections."}
	fake := &fakeDecisions{result: DecisionsResult{Model: DecisionsModel, Answers: []ChoiceAnswer{ans("decision", "topic:bridge-1", .99), ans("evidence", "title_01", .98)}}}
	got, err := (Classifier{Client: fake}).Match(context.Background(), article, []TopicCard{card})
	if err != nil || got.Outcome != MatchOutcomeMatch {
		t.Fatalf("conservative multi-token overlap should match: %+v err=%v", got, err)
	}
	if got.Margin < .5 || !strings.Contains(fake.input, "Candidate bridge-1 | title: Belgrade bridge project opens") || !strings.Contains(fake.input, card.Context) {
		t.Fatalf("margin or candidate context missing: %+v input=%q", got, fake.input)
	}
}

func TestMatchRequiresWinnerMargin(t *testing.T) {
	article := ArticleInput{ArticleID: 12, Title: "Belgrade bridge project adds transit"}
	card := TopicCard{ID: "bridge-1", Title: "Belgrade bridge project opens"}
	decision := ChoiceAnswer{Name: "decision", Choice: "topic:bridge-1", Probabilities: []ChoiceProbability{{Value: "topic:bridge-1", Probability: .85}, {Value: "new_candidate", Probability: .40}, {Value: "review", Probability: .05}}}
	fake := &fakeDecisions{result: DecisionsResult{Model: DecisionsModel, Answers: []ChoiceAnswer{decision, ans("evidence", "title_01", .98)}}}
	got, err := (Classifier{Client: fake}).Match(context.Background(), article, []TopicCard{card})
	if err != nil || got.Outcome != MatchOutcomeReview || got.Margin >= .5 {
		t.Fatalf("narrow top choice should review: %+v err=%v", got, err)
	}
}

func TestMatchCorrectionRequiresExactAlias(t *testing.T) {
	article := ArticleInput{ArticleID: 11, Title: "Belgrade bridge project was not approved"}
	card := TopicCard{ID: "bridge-1", Title: "Belgrade bridge project opens"}
	fake := &fakeDecisions{result: DecisionsResult{Model: DecisionsModel, Answers: []ChoiceAnswer{ans("decision", "topic:bridge-1", .99), ans("evidence", "title_01", .98)}}}
	got, err := (Classifier{Client: fake}).Match(context.Background(), article, []TopicCard{card})
	if err != nil || got.Outcome != MatchOutcomeReview {
		t.Fatalf("correction without exact anchor should review: %+v err=%v", got, err)
	}
}

func TestMatchSplitUsesOneOrTwoCalls(t *testing.T) {
	article := ArticleInput{ArticleID: 10, Title: "Belgrade opens new bridge"}
	card := TopicCard{ID: "bridge-1", Title: "Belgrade bridge project opens", Aliases: []string{"new bridge"}, Context: "The municipality's city bridge construction."}
	first := DecisionsResult{Model: DecisionsModel, Usage: TokenUsage{InputTokens: 20, OutputTokens: 2, TotalTokens: 22}, Answers: []ChoiceAnswer{ans("evidence", "title_01", .96)}}
	second := DecisionsResult{Model: DecisionsModel, Usage: TokenUsage{InputTokens: 24, OutputTokens: 2, TotalTokens: 26}, Answers: []ChoiceAnswer{ans("decision", "topic:bridge-1", .97)}}
	fake := &fakeDecisions{results: []DecisionsResult{first, second}}
	got, err := (Classifier{Client: fake}).MatchSplit(context.Background(), article, []TopicCard{card})
	if err != nil || got.Outcome != MatchOutcomeMatch || got.TopicID != card.ID || fake.calls != 2 {
		t.Fatalf("expected grounded split match in two calls: result=%+v calls=%d err=%v", got, fake.calls, err)
	}
	if len(fake.allQs[0]) != 1 || fake.allQs[0][0].Name != "evidence" || len(fake.allQs[1]) != 1 || fake.allQs[1][0].Name != "decision" {
		t.Fatalf("split stages should each ask exactly one question: %#v", fake.allQs)
	}
	if !strings.Contains(fake.inputs[1], "Evidence chosen in the first stage: title_01: Belgrade opens new bridge") || !strings.Contains(fake.inputs[0], card.Context) {
		t.Fatalf("second stage did not carry selected evidence: %q", fake.inputs[1])
	}
	if got.Usage.TotalTokens != first.Usage.TotalTokens+second.Usage.TotalTokens || len(got.ProbabilityJudgments) != 2 {
		t.Fatalf("split usage or probabilities not aggregated: %+v", got)
	}

	fake = &fakeDecisions{result: DecisionsResult{Model: DecisionsModel, Answers: []ChoiceAnswer{ans("evidence", "review", .99)}}}
	got, err = (Classifier{Client: fake}).MatchSplit(context.Background(), article, []TopicCard{card})
	if err != nil || got.Outcome != MatchOutcomeReview || fake.calls != 1 {
		t.Fatalf("ungrounded evidence should end after one call: result=%+v calls=%d err=%v", got, fake.calls, err)
	}
}

func TestNoveltyIndependentJudgmentsAndOfficialStateChange(t *testing.T) {
	article := ArticleInput{ArticleID: 3, Title: "Council approved the bridge project today"}
	answers := []ChoiceAnswer{
		ans("update", NoveltyOutcomeMaterialUpdate, .92),
		ans("disagreement", "no", .89),
		ans("official_change", "yes", .96),
		ans("subject_kind", NoveltySubjectCandidate, .9),
		ans("evidence", "title_01", .93),
	}
	fake := &fakeDecisions{result: DecisionsResult{Model: DecisionsModel, Usage: TokenUsage{InputTokens: 20, OutputTokens: 8, TotalTokens: 28}, Answers: answers}}
	got, err := (Classifier{Client: fake}).Novelty(context.Background(), article, TopicCard{ID: "t1", Title: "Bridge project"}, []string{"Proposal remains under review"})
	if err != nil {
		t.Fatal(err)
	}
	if fake.calls != 1 || len(fake.qs) != 5 || got.Outcome != NoveltyOutcomeMaterialUpdate || got.Disagreement != "no" || !got.ConcreteStatusChange || !got.SubjectCandidate || got.Model != DecisionsModel || got.Usage.TotalTokens != 28 || len(got.ProbabilityJudgments) != 5 {
		t.Fatalf("unexpected novelty result: %+v", got)
	}
	if !strings.Contains(fake.qs[2].Instructions, "not merely proposed") {
		t.Fatalf("official change criterion missing: %q", fake.qs[2].Instructions)
	}

	answers[0] = ans("update", NoveltyOutcomeMaterialUpdate, .84)
	fake = &fakeDecisions{result: DecisionsResult{Model: DecisionsModel, Answers: answers}}
	got, err = (Classifier{Client: fake}).Novelty(context.Background(), article, TopicCard{}, nil)
	if err != nil || got.Outcome != NoveltyOutcomeReview {
		t.Fatalf("low confidence material update should review: %+v err=%v", got, err)
	}
}

func TestClassifierRejectsInvalidResponseAndOversizedInputs(t *testing.T) {
	article := ArticleInput{ArticleID: 1, Title: "A named topic"}
	fake := &fakeDecisions{result: DecisionsResult{Answers: []ChoiceAnswer{ans("decision", "topic:unknown", .99), ans("evidence", "title_01", .99)}}}
	_, err := (Classifier{Client: fake}).Match(context.Background(), article, []TopicCard{{ID: "known", Title: "Known"}})
	if d, ok := err.(*DecisionsError); !ok || d.Code != "invalid_response" {
		t.Fatalf("unknown topic should fail validation: %v", err)
	}
	tooLarge := ArticleInput{Summary: string(make([]byte, 33<<10))}
	_, err = (Classifier{Client: fake}).Match(context.Background(), tooLarge, nil)
	if d, ok := err.(*DecisionsError); !ok || d.Code != "invalid_request" {
		t.Fatalf("oversized article should be rejected: %v", err)
	}
}
