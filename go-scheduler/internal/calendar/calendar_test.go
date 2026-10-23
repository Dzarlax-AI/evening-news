package calendar

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
)

type goldenCase struct {
	Name            string `json:"name"`
	Type            string `json:"type"`
	Timezone        string `json:"timezone"`
	Now             string `json:"now"`
	Hour            *int   `json:"hour"`
	Minute          *int   `json:"minute"`
	Weekdays        []int  `json:"weekdays"`
	IntervalMinutes *int   `json:"interval_minutes"`
	Want            string `json:"want"`
}

func TestCalendarGolden(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "calendar_golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			now, err := time.Parse(time.RFC3339Nano, tc.Now)
			if err != nil {
				t.Fatal(err)
			}
			want, err := time.Parse(time.RFC3339Nano, tc.Want)
			if err != nil {
				t.Fatal(err)
			}
			got, err := NextAfter(model.Calendar{Type: tc.Type, Timezone: tc.Timezone, Hour: tc.Hour, Minute: tc.Minute, Weekdays: tc.Weekdays, IntervalMinutes: tc.IntervalMinutes}, now)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(want) {
				t.Fatalf("got %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
			}
			if tc.Name == "interval-keeps-microseconds" && got.Nanosecond() != 728869000 {
				t.Fatalf("microseconds lost: %s", got.Format(time.RFC3339Nano))
			}
		})
	}
}

func TestValidateRejectsInvalidCalendar(t *testing.T) {
	minute := 90
	if err := Validate(model.Calendar{Type: "hourly", Timezone: "Europe/Belgrade", Minute: &minute}); err == nil {
		t.Fatal("expected invalid minute to be rejected")
	}
}

func TestValidateRejectsNonIanaTimezoneAliases(t *testing.T) {
	hour, minute := 18, 30
	for _, timezone := range []string{"", "Local"} {
		t.Run("timezone="+timezone, func(t *testing.T) {
			if err := Validate(model.Calendar{Type: "daily", Timezone: timezone, Hour: &hour, Minute: &minute}); err == nil {
				t.Fatalf("expected timezone %q to be rejected", timezone)
			}
		})
	}
}
