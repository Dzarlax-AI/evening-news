package calendar

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"
	_ "time/tzdata" // Keep IANA transition rules available in the distroless runtime.

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
)

var (
	keyPattern       = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	operationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	aliasPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
)

func Validate(s model.Calendar) error {
	if s.Timezone == "" || s.Timezone == "Local" {
		return errors.New("timezone must be an IANA timezone")
	}
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
	if schedule.Type == "daily" {
		return nextDailyCivil(schedule, now, local, location, nil)
	}
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

// NextAfterOccurrence advances a schedule after storing an occurrence. For
// daily schedules, the local date of scheduledFor is consumed even if its
// imported UTC offset does not match the current timezone rules. Other calendar
// types retain NextAfter's behavior.
func NextAfterOccurrence(schedule model.Calendar, now, scheduledFor time.Time) (time.Time, error) {
	if schedule.Type != "daily" {
		return NextAfter(schedule, now)
	}
	if err := Validate(schedule); err != nil {
		return time.Time{}, err
	}
	now = now.UTC().Truncate(time.Microsecond)
	location, _ := time.LoadLocation(schedule.Timezone)
	localNow := now.In(location)
	consumedDate := scheduledFor.UTC().In(location)
	return nextDailyCivil(schedule, now, localNow, location, &consumedDate)
}

// nextDailyCivil calculates daily schedules from local calendar dates. A fold
// chooses the first instant for the wall time, while a gap shifts the wall time
// forward by that transition's actual offset change. If a timezone skips the
// entire date, that date has no occurrence.
func nextDailyCivil(schedule model.Calendar, now, local time.Time, location *time.Location, afterDate *time.Time) (time.Time, error) {
	date := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, time.UTC)
	var exclusiveDate time.Time
	if afterDate != nil {
		exclusiveDate = time.Date(afterDate.Year(), afterDate.Month(), afterDate.Day(), 0, 0, 0, 0, time.UTC)
		if exclusiveDate.After(date) {
			date = exclusiveDate
		}
	}
	for days := 0; days < 3700; days++ {
		candidateDate := date.AddDate(0, 0, days)
		if afterDate != nil && !candidateDate.After(exclusiveDate) {
			continue
		}
		if len(schedule.Weekdays) > 0 && !contains(schedule.Weekdays, isoWeekday(candidateDate)) {
			continue
		}
		candidate, exists := civilWallTime(candidateDate, *schedule.Hour, *schedule.Minute, location)
		if exists && candidate.After(now) {
			return candidate, nil
		}
	}
	return time.Time{}, errors.New("could not calculate next daily occurrence")
}

// civilWallTime resolves a minute-precision wall time without relying on
// time.Date's unspecified choice for ambiguous or nonexistent local times.
// IANA transitions are sampled over a window wider than the maximum civil
// offset; transition boundaries are then located to the second.
func civilWallTime(date time.Time, hour, minute int, location *time.Location) (time.Time, bool) {
	wall := time.Date(date.Year(), date.Month(), date.Day(), hour, minute, 0, 0, time.UTC)
	start := wall.Add(-72 * time.Hour)
	end := wall.Add(72 * time.Hour)
	offsets := make(map[int]struct{})
	var transitionGaps [][3]int64 // transition Unix second, old offset, new offset

	step := time.Hour
	previous := start
	_, previousOffset := previous.In(location).Zone()
	offsets[previousOffset] = struct{}{}
	for current := start.Add(step); !current.After(end); current = current.Add(step) {
		_, currentOffset := current.In(location).Zone()
		offsets[currentOffset] = struct{}{}
		if currentOffset != previousOffset {
			low, high := previous.Unix(), current.Unix()
			for low+1 < high {
				middle := low + (high-low)/2
				_, middleOffset := time.Unix(middle, 0).In(location).Zone()
				if middleOffset == previousOffset {
					low = middle
				} else {
					high = middle
				}
			}
			transitionGaps = append(transitionGaps, [3]int64{high, int64(previousOffset), int64(currentOffset)})
		}
		previous, previousOffset = current, currentOffset
	}

	candidates := make([]time.Time, 0, 2)
	for offset := range offsets {
		candidate := wall.Add(-time.Duration(offset) * time.Second)
		if matchesCivilMinute(candidate.In(location), date, hour, minute) {
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) > 0 {
		first := candidates[0]
		for _, candidate := range candidates[1:] {
			if candidate.Before(first) {
				first = candidate
			}
		}
		return first.UTC(), true
	}

	for _, gap := range transitionGaps {
		transition := time.Unix(gap[0], 0).UTC()
		oldOffset, newOffset := gap[1], gap[2]
		if newOffset <= oldOffset {
			continue
		}
		gapStart := transition.Add(time.Duration(oldOffset) * time.Second)
		gapEnd := transition.Add(time.Duration(newOffset) * time.Second)
		if wall.Before(gapStart) || !wall.Before(gapEnd) {
			continue
		}
		shiftedWall := wall.Add(time.Duration(newOffset-oldOffset) * time.Second)
		candidate := shiftedWall.Add(-time.Duration(newOffset) * time.Second).UTC()
		// Do not let a skipped civil date, or a gap crossing midnight, create a
		// second occurrence on the following local date.
		if matchesCivilMinute(candidate.In(location), date, shiftedWall.Hour(), shiftedWall.Minute()) {
			return candidate, true
		}
		return time.Time{}, false
	}
	return time.Time{}, false
}

func matchesCivilMinute(local, date time.Time, hour, minute int) bool {
	return local.Year() == date.Year() && local.Month() == date.Month() && local.Day() == date.Day() &&
		local.Hour() == hour && local.Minute() == minute && local.Second() == 0 && local.Nanosecond() == 0
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
