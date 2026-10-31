package appsettings

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

// The producer and background consumer use the same wire fixture without
// introducing a runtime dependency between their independently built modules.
func TestJobOperationalWireFixture(t *testing.T) {
	raw, err := os.ReadFile("../../tests/fixtures/operational-snapshot-v1/job-configuration.json")
	if err != nil {
		t.Fatal(err)
	}
	var pinned JobOperational
	if err := json.Unmarshal(raw, &pinned); err != nil {
		t.Fatal(err)
	}
	if pinned.Viber == nil || !pinned.Viber.DeliveryEnabled || pinned.Viber.EnabledFrom == nil {
		t.Fatal("fixture must include a complete Viber snapshot")
	}
	encoded, err := json.Marshal(pinned)
	if err != nil {
		t.Fatal(err)
	}
	var want, got any
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("producer wire shape differs from the shared consumer fixture")
	}
}
