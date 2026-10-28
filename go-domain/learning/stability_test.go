package learning

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStabilityPreservesAndUpdatesLegacyJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "domain_stats.json")
	original := `{"domain_stats":{"example.test":{"domain":"example.test","total_attempts":2,"successful_attempts":1,"failed_attempts":1,"last_success_timestamp":100,"last_failure_timestamp":101,"average_extraction_time_ms":20,"success_rate":0.5,"method_success_counts":{"readability":1},"method_failure_counts":{"readability":1},"method_avg_times":{"readability":10},"average_content_length":100,"average_quality_score":0.5,"consecutive_all_methods_failures":2,"last_all_methods_failure_timestamp":101}},"ai_credits_used":2,"ai_credits_saved":3,"last_cleanup_time":9999999999}`
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	s := newStability(path)
	if err := s.success("example.test", "readability", 30, 300, 0.9); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var d stabilityData
	if err = json.Unmarshal(raw, &d); err != nil {
		t.Fatal(err)
	}
	p := d.Domains["example.test"]
	if p.Total != 3 || p.Success != 2 || p.Failed != 1 || p.AllFailures != 0 || p.LastAllFailure != nil || p.MethodSuccess["readability"] != 2 || p.AverageLength != 200 || p.AverageQuality != 0.7 || d.CreditsUsed != 2 || d.CreditsSaved != 3 {
		t.Fatalf("state=%+v data=%+v", p, d)
	}
}
func TestStabilityDoesNotOverwriteMalformedExistingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "domain_stats.json")
	original := []byte("{invalid")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if err := newStability(path).success("example.test", "readability", 1, 1, 0.5); err == nil {
		t.Fatal("expected invalid state error")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(original) {
		t.Fatal("existing state changed")
	}
}

func TestStabilityRejectedPublicationPreservesState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stats.json")
	s := newStability(path)
	denied := errors.New("stale fixture")
	err := s.success("example.test", "readability", 1, 10, 0.5, func(_, _ string) error { return denied })
	if !errors.Is(err, denied) {
		t.Fatalf("error=%v", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("unfenced publication: %v", err)
	}
	if len(s.data.Domains) != 0 {
		t.Fatalf("stale in-memory state retained: %+v", s.data)
	}
}
