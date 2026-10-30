package editorial

import (
	"context"
	"encoding/json"
	ai "github.com/Dzarlax-AI/go-ai"
	"strings"
	"testing"
)

type localeProvider func(context.Context, ai.Request) (ai.Response, error)

func (f localeProvider) Generate(ctx context.Context, r ai.Request) (ai.Response, error) {
	return f(ctx, r)
}
func bilingualFixture() (Snapshot, Snapshot) {
	ru := syntheticSnapshot(2)
	sr := ru
	sr.Articles = append([]Article(nil), ru.Articles...)
	sr.EditionURL = "https://news.example/digest/2026-10-27?edition=sr&locale=sr-Latn"
	for i := range sr.Articles {
		sr.Articles[i].Title = "Izgradnja mosta počinje"
		sr.Articles[i].Summary = "Grad je najavio izgradnju novog mosta."
		sr.Articles[i].Content = ""
		sr.Articles[i].CategoryName = "Tehnologija"
	}
	return ru, sr
}
func TestBilingualPartialAndPlanValidation(t *testing.T) {
	ru, sr := bilingualFixture()
	for _, mode := range []string{"complete", "missing_sr", "invalid_plan", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			provider := localeProvider(func(_ context.Context, request ai.Request) (ai.Response, error) {
				calls++
				if !strings.Contains(request.Prompt, "sr-Latn") || !strings.Contains(request.Prompt, sr.Articles[0].Summary) {
					t.Fatal("missing bilingual evidence")
				}
				envelope := map[string]any{"ru": syntheticDraft(ru), "sr-Latn": syntheticDraft(sr)}
				if mode == "missing_sr" {
					delete(envelope, "sr-Latn")
				}
				if mode == "invalid_plan" {
					d := syntheticDraft(sr)
					d.MainStories = []modelMainStory{}
					envelope["sr-Latn"] = d
				}
				raw, _ := json.Marshal(envelope)
				if mode == "malformed" {
					raw = []byte(`{"ru":`)
				}
				return ai.Response{Status: ai.Complete, Provider: "fixture", Model: "fixture", Text: string(raw)}, nil
			})
			result, err := GenerateBilingual(context.Background(), ru, sr, ProviderConfig{Provider: provider, Model: "fixture"})
			if calls != 1 {
				t.Fatal("expected one shared call")
			}
			if mode == "malformed" {
				if err == nil {
					t.Fatal("malformed envelope accepted")
				}
				return
			}
			if err != nil || ValidateEdition(ru, "ru", result.Russian) != nil {
				t.Fatalf("Russian result lost: %v", err)
			}
			if mode != "complete" {
				if result.Serbian != nil || result.SerbianError == "" {
					t.Fatal("invalid Serbian marked ready")
				}
				return
			}
			if result.Serbian == nil || !SameEditorialPlan(result.Russian, *result.Serbian) {
				t.Fatal("plan mismatch")
			}
			render, err := Render(sr, "sr-Latn", *result.Serbian)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"Glavno", "Ceo pregled", "?locale=sr-Latn"} {
				if !strings.Contains(render.RichHTML, want) {
					t.Errorf("render missing %s", want)
				}
			}
			if !strings.Contains(strings.Join(render.PlainParts, ""), "Ceo pregled") {
				t.Fatal("plain footer not localized")
			}
		})
	}
}
func TestSerbianRetryCannotChangePlanOrEvidenceLocale(t *testing.T) {
	ru, sr := bilingualFixture()
	plan, err := resolveLocalized(ru, "ru", syntheticDraft(ru))
	if err != nil {
		t.Fatal(err)
	}
	provider := localeProvider(func(_ context.Context, r ai.Request) (ai.Response, error) {
		if !strings.Contains(r.Prompt, "immutable") {
			t.Fatal("missing fixed plan")
		}
		raw, _ := json.Marshal(syntheticDraft(sr))
		return ai.Response{Status: ai.Complete, Text: string(raw)}, nil
	})
	result, err := GenerateForPlan(context.Background(), sr, plan, ProviderConfig{Provider: provider, Model: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	if !SameEditorialPlan(plan, result.Edition) {
		t.Fatal("changed editorial plan")
	}
	result.Edition.Stories[0].Evidence[0].Locale = "ru"
	if ValidateEdition(sr, "sr-Latn", result.Edition) == nil {
		t.Fatal("wrong evidence locale accepted")
	}
}
