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
