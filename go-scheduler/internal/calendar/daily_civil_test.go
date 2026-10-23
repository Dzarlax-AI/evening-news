package calendar

import (
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
)

func TestDailyUsesCivilDatesAcrossDST(t *testing.T) {
	cases := []struct {
		name     string
		timezone string
		hour     int
		minute   int
		weekdays []int
		now      string
		want     string
	}{
		{
			name:     "Belgrade autumn fold chooses first occurrence before it",
			timezone: "Europe/Belgrade", hour: 2, minute: 15,
			now: "2026-10-24T23:00:00Z", want: "2026-10-25T00:15:00Z",
		},
		{
			name:     "Belgrade autumn fold does not run second occurrence",
			timezone: "Europe/Belgrade", hour: 2, minute: 15,
			now: "2026-10-25T00:30:00Z", want: "2026-10-26T01:15:00Z",
		},
		{
			name:     "Belgrade spring gap shifts by one hour",
			timezone: "Europe/Belgrade", hour: 2, minute: 30,
			now: "2026-03-29T00:30:00Z", want: "2026-03-29T01:30:00Z",
		},
		{
			name:     "Lord Howe spring gap shifts by thirty minutes",
			timezone: "Australia/Lord_Howe", hour: 2, minute: 15,
			now: "2026-10-03T14:00:00Z", want: "2026-10-03T15:45:00Z",
		},
		{
			name:     "weekday filter skips a fold date after its first occurrence",
			timezone: "Europe/Belgrade", hour: 2, minute: 15, weekdays: []int{7},
			now: "2026-10-25T00:30:00Z", want: "2026-11-01T01:15:00Z",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			now := parseTestTime(t, tc.now)
			want := parseTestTime(t, tc.want)
			schedule := dailyCalendar(tc.timezone, tc.hour, tc.minute, tc.weekdays)
			got, err := NextAfter(schedule, now)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(want) {
				t.Fatalf("NextAfter(%s) = %s, want %s", now.Format(time.RFC3339), got.Format(time.RFC3339), want.Format(time.RFC3339))
			}
		})
	}
}

func TestDailyBelgradeAutumnSequentialCallsAdvanceLocalDateOnce(t *testing.T) {
	schedule := dailyCalendar("Europe/Belgrade", 19, 0, nil)
	now := parseTestTime(t, "2026-10-25T00:30:00Z")
	want := []string{"2026-10-25T18:00:00Z", "2026-10-26T18:00:00Z"}
	for i, expected := range want {
		got, err := NextAfter(schedule, now)
		if err != nil {
			t.Fatal(err)
		}
		if wantTime := parseTestTime(t, expected); !got.Equal(wantTime) {
			t.Fatalf("call %d: NextAfter(%s) = %s, want %s", i+1, now.Format(time.RFC3339), got.Format(time.RFC3339), expected)
		}
		now = got
	}
}

func TestDailyBelgradeSpringSequentialCallsAdvanceLocalDateOnce(t *testing.T) {
	schedule := dailyCalendar("Europe/Belgrade", 19, 0, nil)
	now := parseTestTime(t, "2026-03-28T18:00:00Z")
	want := []string{"2026-03-29T17:00:00Z", "2026-03-30T17:00:00Z"}
	for i, expected := range want {
		got, err := NextAfter(schedule, now)
		if err != nil {
			t.Fatal(err)
		}
		if wantTime := parseTestTime(t, expected); !got.Equal(wantTime) {
			t.Fatalf("call %d: NextAfter(%s) = %s, want %s", i+1, now.Format(time.RFC3339), got.Format(time.RFC3339), expected)
		}
		now = got
	}
}

func TestDailyOccurrenceConsumesImportedScheduledLocalDate(t *testing.T) {
	schedule := dailyCalendar("Europe/Belgrade", 19, 0, nil)
	importedDue := parseTestTime(t, "2026-10-25T17:00:00Z") // Imported instant renders as 18:00 local.
	got, err := NextAfterOccurrence(schedule, importedDue, importedDue)
	if err != nil {
		t.Fatal(err)
	}
	want := parseTestTime(t, "2026-10-26T18:00:00Z")
	if !got.Equal(want) {
		t.Fatalf("NextAfterOccurrence = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestDailySkipsSkippedCivilDate(t *testing.T) {
	schedule := dailyCalendar("Pacific/Apia", 9, 0, nil)
	now := parseTestTime(t, "2011-12-29T19:00:00Z") // Dec 29 09:00, before the date-line jump.
	got, err := NextAfter(schedule, now)
	if err != nil {
		t.Fatal(err)
	}
	want := parseTestTime(t, "2011-12-30T19:00:00Z") // Dec 31 09:00 after Dec 30 was skipped.
	if !got.Equal(want) {
		t.Fatalf("NextAfter(%s) = %s, want %s", now.Format(time.RFC3339), got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func dailyCalendar(timezone string, hour, minute int, weekdays []int) model.Calendar {
	return model.Calendar{Type: "daily", Timezone: timezone, Hour: &hour, Minute: &minute, Weekdays: weekdays}
}

func parseTestTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
