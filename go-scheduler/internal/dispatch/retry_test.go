package dispatch

import (
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestParseRetryAfterClampsLargeSecondsBeforeDuration(t *testing.T) {
	now := time.Date(2026, 10, 23, 17, 30, 0, 0, time.UTC)
	for _, value := range []string{strconv.FormatInt(math.MaxInt64, 10), "18446744073709551616", "99999999999999999999999999999999999999"} {
		if got := parseRetryAfter(value, now); !got.Equal(now.Add(maxRetryAfter)) {
			t.Fatalf("Retry-After %q did not clamp to the maximum delay", value)
		}
	}
	if got := parseRetryAfter("2", now); !got.Equal(now.Add(2 * time.Second)) {
		t.Fatal("ordinary integer Retry-After was not preserved")
	}
}

func TestParseRetryAfterHTTPDateUsesDatabaseClockUnderApplicationSkew(t *testing.T) {
	databaseNow := time.Date(2026, 10, 23, 17, 30, 0, 0, time.UTC)
	header := databaseNow.Add(3 * time.Second).Format(http.TimeFormat)
	for _, skew := range []time.Duration{-2 * time.Minute, 2 * time.Minute} {
		appNow := databaseNow.Add(skew)
		got := parseRetryAfter(header, databaseNow)
		if delay := got.Sub(databaseNow); delay != 3*time.Second {
			t.Fatalf("app clock skew %s changed HTTP-date Retry-After delay to %s", appNow.Sub(databaseNow), delay)
		}
		if appNow.Add(3 * time.Second).Equal(got) {
			t.Fatalf("test setup failed: application clock skew %s accidentally matched database time", skew)
		}
	}
}
