package calendar

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
)

var (
	keyPattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	operationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	aliasPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
)

func Validate(s model.Calendar) error {
	if _, err := time.LoadLocation(s.Timezone); err != nil {
		return errors.New("timezone must be an IANA timezone")
	}
	weekdays := append([]int(nil), s.Weekdays...)
	for i, d := range weekdays {
		if d < 1 || d > 7 || (i > 0 && weekdays[i-1] >= d) {
			return errors.New("weekdays must be unique, ascending ISO weekdays 1 through 7")
		}
	}
	validTime := func() bool {
		return s.Hour != nil && s.Minute != nil && *s.Hour >= 0 && *s.Hour <= 23 && *s.Minute >= 0 && *s.Minute <= 59
	}
	switch s.Type {
	case "daily":
		if !validTime() || s.IntervalMinutes != nil {
			return errors.New("daily requires hour and minute only")
		}
	case "weekly":
		if !validTime() || len(weekdays) == 0 || s.IntervalMinutes != nil {
			return errors.New("weekly requires hour, minute, and weekdays")
		}
	case "hourly":
		if s.Minute == nil || *s.Minute < 0 || *s.Minute > 59 || s.Hour != nil || s.IntervalMinutes != nil {
			return errors.New("hourly requires minute only")
		}
	case "interval":
		if s.IntervalMinutes == nil || *s.IntervalMinutes < 1 || *s.IntervalMinutes > 1440 || s.Hour != nil || s.Minute != nil || len(weekdays) != 0 {
			return errors.New("interval requires interval_minutes from 1 through 1440 only")
		}
	default:
		return errors.New("calendar type must be daily, weekly, hourly, or interval")
	}
	return nil
}

func ValidateResource(key, target, operation string, payload map[string]any, targets map[string]model.Target) error {
	if !keyPattern.MatchString(key) {
		return errors.New("invalid schedule key")
	}
	if !aliasPattern.MatchString(target) {
		return errors.New("invalid target alias")
	}
	if _, ok := targets[target]; !ok {
		return errors.New("unknown target alias")
	}
	if !operationPattern.MatchString(operation) {
		return errors.New("invalid operation")
	}
	if payload == nil {
		return errors.New("payload must be an object")
	}
	return nil
}

// Normalize removes representation-only empty optional lists so configuration
// comparisons remain stable across JSON persistence with omitempty fields.
func Normalize(schedule model.Calendar) model.Calendar {
	if len(schedule.Weekdays) == 0 {
		schedule.Weekdays = nil
	}
	return schedule
}

func NextAfter(schedule model.Calendar, now time.Time) (time.Time, error) {
	if err := Validate(schedule); err != nil {
		return time.Time{}, err
	}
	now = now.UTC().Truncate(time.Microsecond)
	location, _ := time.LoadLocation(schedule.Timezone)
	local := now.UTC().In(location)
	if schedule.Type == "weekly" {
		for days := 0; days <= 7; days++ {
			candidateDate := local.AddDate(0, 0, days)
			if !contains(schedule.Weekdays, isoWeekday(candidateDate)) {
				continue
			}
			candidate := time.Date(candidateDate.Year(), candidateDate.Month(), candidateDate.Day(), *schedule.Hour, *schedule.Minute, 0, 0, location)
			if candidate.After(now) {
				return candidate.UTC(), nil
			}
		}
		return time.Time{}, errors.New("could not calculate next weekly occurrence")
	}

	_, offset := local.Zone()
	legacyLocation := time.FixedZone("legacy-offset", offset)
	legacyNow := now.UTC().In(legacyLocation)
	switch schedule.Type {
	case "daily":
		candidate := time.Date(legacyNow.Year(), legacyNow.Month(), legacyNow.Day(), *schedule.Hour, *schedule.Minute, 0, 0, legacyLocation)
		if !candidate.After(legacyNow) {
			candidate = candidate.Add(24 * time.Hour)
		}
		for len(schedule.Weekdays) > 0 && !contains(schedule.Weekdays, isoWeekday(candidate)) {
			candidate = candidate.Add(24 * time.Hour)
		}
		return candidate.UTC(), nil
	case "hourly":
		candidate := time.Date(legacyNow.Year(), legacyNow.Month(), legacyNow.Day(), legacyNow.Hour(), *schedule.Minute, 0, 0, legacyLocation)
		if !candidate.After(legacyNow) {
			candidate = candidate.Add(time.Hour)
		}
		if len(schedule.Weekdays) > 0 && !contains(schedule.Weekdays, isoWeekday(candidate)) {
			for !contains(schedule.Weekdays, isoWeekday(candidate)) {
				candidate = candidate.Add(24 * time.Hour)
			}
			candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(), 0, *schedule.Minute, 0, 0, legacyLocation)
		}
		return candidate.UTC(), nil
	case "interval":
		return now.Add(time.Duration(*schedule.IntervalMinutes) * time.Minute), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported calendar type %q", schedule.Type)
	}
}

func contains(values []int, value int) bool {
	i := sort.SearchInts(values, value)
	return i < len(values) && values[i] == value
}

func isoWeekday(t time.Time) int {
	if t.Weekday() == time.Sunday {
		return 7
	}
	return int(t.Weekday())
}
