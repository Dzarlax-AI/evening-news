package orchestration

import (
	"context"
	"errors"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-execution"
)

type OperationalSettings struct {
	Revision          int64  `json:"revision"`
	LimitsEnabled     bool   `json:"limits_enabled"`
	MaxArticles       int    `json:"max_articles"`
	PerSource         int    `json:"per_source"`
	Days              int    `json:"days"`
	NewestDate        string `json:"newest_date"`
	ProcessingEnabled bool   `json:"processing_enabled"`
	DeliveryEnabled   bool   `json:"delivery_enabled"`
}

type JobOperationalConfig struct {
	JobID             string              `json:"job_id"`
	AttemptGeneration int64               `json:"attempt_generation"`
	PinnedAt          time.Time           `json:"pinned_at"`
	Settings          OperationalSettings `json:"settings"`
}

type JobConfigPreparer func(context.Context, execution.ExecutionContext) (JobOperationalConfig, error)

func (s OperationalSettings) validate() error {
	if s.Revision < 0 || s.MaxArticles < 1 || s.MaxArticles > 100000 || s.PerSource < 1 || s.PerSource > 100000 || s.Days < 0 || s.Days > 36500 {
		return errors.New("invalid operational settings")
	}
	if s.NewestDate != "" {
		if _, err := time.Parse("2006-01-02", s.NewestDate); err != nil {
			return errors.New("invalid operational settings")
		}
	}
	return nil
}
