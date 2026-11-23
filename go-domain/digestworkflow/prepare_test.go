package digestworkflow

import (
	"errors"
	"testing"
	"time"
)

func TestPreparedIdentityAndSingleClaim(t *testing.T) {
	c := new(PreparedCache)
	id := Identity{JobID: "job", Attempt: 1, DomainInstanceID: "instance", Deadline: time.Now().Add(time.Minute)}
	token, err := c.add(Prepared{Identity: id})
	if err != nil {
		t.Fatal(err)
	}
	changed := id
	changed.Attempt++
	if _, err = c.Claim(token, changed); !errors.Is(err, ErrPrepared) {
		t.Fatalf("unexpected claim: %v", err)
	}
	if _, err = c.Claim(token, id); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Claim(token, id); !errors.Is(err, ErrPrepared) {
		t.Fatalf("repeated claim: %v", err)
	}
}

func TestEditionDateUsesScheduledUTCDate(t *testing.T) {
	scheduled := time.Date(2026, 11, 12, 0, 30, 0, 0, time.FixedZone("offset", 3600))
	cutoff := time.Date(2026, 11, 12, 2, 0, 0, 0, time.UTC)
	if got := (Workflow{EditionDate: scheduled}).editionDate(cutoff); got != "2026-11-11" {
		t.Fatalf("scheduled date = %s", got)
	}
	if got := (Workflow{}).editionDate(cutoff); got != "2026-11-12" {
		t.Fatalf("manual date = %s", got)
	}
}
