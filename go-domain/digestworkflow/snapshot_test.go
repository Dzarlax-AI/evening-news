package digestworkflow

import (
	"encoding/json"
	"os"
	"testing"
)

// Frozen against Python uuid.uuid5 and the legacy category mapping.
func TestLegacyEditionIDAndLabels(t *testing.T) {
	got := EditionID("11111111-1111-1111-1111-111111111111", 7, "22222222-2222-2222-2222-222222222222")
	if got != "78de5eb5-931c-5c40-b959-117e7450c0ae" {
		t.Fatalf("edition ID %q", got)
	}
	for _, tc := range []struct{ name, display, want string }{{" Tech ", "Custom", "Технологии"}, {"unknown", " Display ", "Display"}, {" Unknown ", "", "Unknown"}} {
		if got := categoryLabel(tc.name, tc.display); got != tc.want {
			t.Fatalf("label %q", got)
		}
	}
}

func TestCapturedLegacySnapshotIdentity(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy-snapshot.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Identity struct {
			Job       string `json:"job_id"`
			Attempt   int    `json:"attempt_generation"`
			Operation string `json:"operation_id"`
		} `json:"identity"`
		EditionID string `json:"edition_id"`
		Rows      []struct {
			Category string  `json:"category_name"`
			Display  *string `json:"category_display_name"`
		} `json:"rows"`
		Labels []string `json:"category_labels"`
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if EditionID(fixture.Identity.Job, fixture.Identity.Attempt, fixture.Identity.Operation) != fixture.EditionID {
		t.Fatal("legacy UUID differs")
	}
	for i, r := range fixture.Rows {
		display := ""
		if r.Display != nil {
			display = *r.Display
		}
		if categoryLabel(r.Category, display) != fixture.Labels[i] {
			t.Fatal("legacy label differs")
		}
	}
}
