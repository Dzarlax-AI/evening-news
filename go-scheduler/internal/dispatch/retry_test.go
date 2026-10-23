package dispatch

import (
	"math"
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
