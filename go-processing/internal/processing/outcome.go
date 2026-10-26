package processing

import (
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	StatePending   = "pending"
	StateRetryWait = "retry_wait"
	StateCompleted = "completed"
	StateRejected  = "rejected"
	StateExhausted = "exhausted"
)

type Outcome struct {
	State             string         `json:"state"`
	Fields            map[string]any `json:"fields,omitempty"`
	Error             string         `json:"error,omitempty"`
	RetryAfterSeconds any            `json:"retry_after_seconds,omitempty"`
}

type Limits struct {
	MaxAttempts    int
	Initial, Later time.Duration
}

// Persisted represents the same bounded outcome_fields policy used by Python.
type Persisted struct {
	State         string
	Fields        map[string]any
	Attempts      int
	NextAttemptAt *time.Time
	Error         *string
	Processed     bool
}

func (o Outcome) Persist(attempt int, now time.Time, limits Limits) Persisted {
	if limits.MaxAttempts < 1 {
		limits.MaxAttempts = 3
	}
	if limits.Initial <= 0 {
		limits.Initial = 30 * time.Minute
	}
	if limits.Later <= 0 {
		limits.Later = 6 * time.Hour
	}
	state, attempts := o.State, attempt
	var next *time.Time
	if state == StateRetryWait {
		if o.Error == "busy" || o.Error == "native_browser_busy" {
			attempts = max(0, attempt-1)
			delay := max(time.Minute, retryAfter(o.RetryAfterSeconds))
			t := now.Add(delay)
			next = &t
		} else if attempt >= limits.MaxAttempts {
			state = StateExhausted
		} else {
			delay := limits.Initial
			if attempt != 1 {
				delay = limits.Later
			}
			delay = max(delay, retryAfter(o.RetryAfterSeconds))
			t := now.Add(delay)
			next = &t
		}
	}
	var err *string
	if strings.TrimSpace(o.Error) != "" {
		v := runeLimit(o.Error, 2000)
		err = &v
	}
	return Persisted{State: state, Fields: o.Fields, Attempts: attempts, NextAttemptAt: next, Error: err, Processed: state == StateCompleted || state == StateRejected || state == StateExhausted}
}
func retryAfter(v any) time.Duration {
	var seconds float64
	switch n := v.(type) {
	case float64:
		seconds = n
	case float32:
		seconds = float64(n)
	case int:
		seconds = float64(n)
	case int64:
		seconds = float64(n)
	case string:
		parsed, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0
		}
		seconds = parsed
	default:
		return 0
	}
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 {
		return 0
	}
	maxSeconds := float64((time.Duration(1<<63-1) / time.Second) - 1)
	if seconds > maxSeconds {
		return time.Duration(1<<63-1) - time.Second
	}
	return time.Duration(seconds * float64(time.Second))
}
func runeLimit(s string, n int) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}
func IsChallengeText(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	if utf8.RuneCountInString(s) > 1500 {
		return false
	}
	if s == "just a moment" || s == "just a moment..." || s == "access denied" || s == "forbidden" {
		return true
	}
	for _, marker := range []string{"enable javascript and cookies to continue", "checking your browser before accessing", "checking if the site connection is secure", "verify you are human", "verify that you are human", "performing security verification", "проверка вашего браузера", "включите javascript и файлы cookie"} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}
