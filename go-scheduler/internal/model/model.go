package model

import "time"

type Calendar struct {
	Type            string `json:"type"`
	Timezone        string `json:"timezone"`
	Hour            *int   `json:"hour,omitempty"`
	Minute          *int   `json:"minute,omitempty"`
	Weekdays        []int  `json:"weekdays,omitempty"`
	IntervalMinutes *int   `json:"interval_minutes,omitempty"`
}

type Schedule struct {
	Key       string         `json:"schedule_key"`
	Revision  int64          `json:"revision"`
	Enabled   bool           `json:"enabled"`
	Calendar  Calendar       `json:"calendar"`
	Target    string         `json:"target_alias"`
	Operation string         `json:"operation"`
	Payload   map[string]any `json:"payload"`
	NextDueAt *time.Time     `json:"next_due_at"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type Mutation struct {
	ExpectedRevision int64          `json:"expected_revision"`
	Enabled          bool           `json:"enabled"`
	Calendar         Calendar       `json:"calendar"`
	Target           string         `json:"target_alias"`
	Operation        string         `json:"operation"`
	Payload          map[string]any `json:"payload"`
}

type Envelope struct {
	Version          int            `json:"version"`
	OccurrenceID     string         `json:"occurrence_id"`
	ScheduleKey      string         `json:"schedule_key"`
	ScheduleRevision int64          `json:"schedule_revision"`
	ScheduledFor     time.Time      `json:"scheduled_for"`
	Operation        string         `json:"operation"`
	Payload          map[string]any `json:"payload"`
}

type Occurrence struct {
	ID               string     `json:"occurrence_id"`
	ScheduleKey      string     `json:"schedule_key"`
	ScheduleRevision int64      `json:"schedule_revision"`
	ScheduledFor     time.Time  `json:"scheduled_for"`
	TargetAlias      string     `json:"target_alias"`
	Status           string     `json:"status"`
	LeaseExpiresAt   *time.Time `json:"lease_expires_at"`
	LeaseActive      bool       `json:"lease_active"`
	Attempts         int        `json:"attempts"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	JobID            *string    `json:"job_id"`
}

type Receipt struct {
	Version      int    `json:"version"`
	OccurrenceID string `json:"occurrence_id"`
	JobID        string `json:"job_id"`
	Status       string `json:"status"`
}

type Target struct {
	URL   string
	Token string
}
