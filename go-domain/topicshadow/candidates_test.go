package topicshadow

import (
	"reflect"
	"testing"
	"time"
)

func TestRankCandidatesNormalizesRussianAndSerbianScripts(t *testing.T) {
	asOf := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	article := ArticleInput{Title: "Протестима блокада моста", SerbianSummary: "Građani protestuju zbog blokade mosta"}
	topics := []CandidateTopic{
		{Card: TopicCard{ID: "bridge", Title: "Blokada Mosta"}, Status: LifecycleActive, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "cyrillic", Title: "Блокада моста"}, Status: LifecycleActive, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "noise", Title: "Sportski rezultat"}, Status: LifecycleActive, LastMaterialAt: asOf},
	}
	got := RankCandidates(article, topics, asOf, 8)
	if len(got) != 2 || got[0].ID != "bridge" || got[1].ID != "cyrillic" {
		t.Fatalf("expected RU/SR matches in ID tie order, got %+v", got)
	}
}

func TestRankCandidatesUsesDistinctTermsAndAliasPhrases(t *testing.T) {
	asOf := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	article := ArticleInput{Title: "NVIDIA најавила нови чип", Summary: "NVIDIA razvija čip za AI"}
	topics := []CandidateTopic{
		{Card: TopicCard{ID: "nvidia", Title: "NVIDIA", Aliases: []string{"green team"}}, Status: LifecycleActive, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "chip", Title: "čip za AI"}, Status: LifecycleCandidate, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "repeat", Title: "NVIDIA NVIDIA NVIDIA"}, Status: LifecycleActive, LastMaterialAt: asOf},
	}
	got := RankCandidates(article, topics, asOf, 8)
	if len(got) != 3 || got[0].ID != "chip" || got[1].ID != "nvidia" || got[2].ID != "repeat" {
		t.Fatalf("alias and distinct-term ranking unexpected: %+v", got)
	}
}

func TestRankCandidatesThirtyDayWindowInclusive(t *testing.T) {
	asOf := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	article := ArticleInput{Title: "Belgrade bridge blockade"}
	topics := []CandidateTopic{
		{Card: TopicCard{ID: "edge", Title: "Belgrade bridge"}, Status: LifecycleActive, LastMaterialAt: asOf.Add(-30 * 24 * time.Hour)},
		{Card: TopicCard{ID: "old", Title: "Belgrade bridge"}, Status: LifecycleActive, LastMaterialAt: asOf.Add(-30*24*time.Hour - time.Nanosecond)},
		{Card: TopicCard{ID: "future", Title: "Belgrade bridge"}, Status: LifecycleActive, LastMaterialAt: asOf.Add(time.Second)},
	}
	got := RankCandidates(article, topics, asOf, 8)
	if len(got) != 1 || got[0].ID != "edge" {
		t.Fatalf("expected inclusive 30-day cutoff and no future leakage, got %+v", got)
	}
}

func TestRankCandidatesReservesAtMostTwoDormantInFirstFourAndPrefixes(t *testing.T) {
	asOf := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	article := ArticleInput{Title: "Belgrade bridge blockade protests"}
	topics := []CandidateTopic{
		{Card: TopicCard{ID: "d1", Title: "Belgrade bridge blockade"}, Status: LifecycleDormant, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "d2", Title: "Belgrade bridge protests"}, Status: LifecycleDormant, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "d3", Title: "Belgrade bridge blockade protests"}, Status: LifecycleDormant, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "a1", Title: "Belgrade bridge"}, Status: LifecycleActive, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "a2", Title: "bridge blockade"}, Status: LifecycleActive, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "a3", Title: "bridge protests"}, Status: LifecycleCandidate, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "a4", Title: "Belgrade protests"}, Status: LifecycleActive, LastMaterialAt: asOf},
	}
	four := RankCandidates(article, topics, asOf, 4)
	six := RankCandidates(article, topics, asOf, 6)
	eight := RankCandidates(article, topics, asOf, 8)
	if len(four) != 4 || len(six) != 6 || len(eight) != len(topics) {
		t.Fatalf("wrong result sizes: four=%d six=%d eight=%d", len(four), len(six), len(eight))
	}
	dormant := 0
	for _, card := range four {
		if card.ID == "d1" || card.ID == "d2" || card.ID == "d3" {
			dormant++
		}
	}
	if dormant != 2 {
		t.Fatalf("expected exactly two dormant reservations in first four, got %+v", four)
	}
	if !reflect.DeepEqual(four, six[:4]) || !reflect.DeepEqual(six, eight[:6]) {
		t.Fatalf("4/6/8 results must be prefixes: four=%+v six=%+v eight=%+v", four, six, eight)
	}
}

func TestRankCandidatesStableTiesAndNoMutation(t *testing.T) {
	asOf := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	article := ArticleInput{Title: "Belgrade bridge"}
	topics := []CandidateTopic{
		{Card: TopicCard{ID: "z", Title: "Belgrade bridge", Aliases: []string{"old name"}}, Status: LifecycleActive, LastMaterialAt: asOf},
		{Card: TopicCard{ID: "a", Title: "Belgrade bridge", Aliases: []string{"old name"}}, Status: LifecycleActive, LastMaterialAt: asOf},
	}
	first := RankCandidates(article, topics, asOf, 2)
	second := RankCandidates(article, topics, asOf, 2)
	if first[0].ID != "a" || !reflect.DeepEqual(first, second) {
		t.Fatalf("expected stable ID tie ordering, got first=%+v second=%+v", first, second)
	}
	first[0].Aliases[0] = "changed"
	if topics[1].Card.Aliases[0] != "old name" {
		t.Fatal("returned cards must not alias candidate alias storage")
	}
}
