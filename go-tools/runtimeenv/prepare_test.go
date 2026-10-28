package runtimeenv

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestLegacyGolden(t *testing.T) {
	raw, e := os.ReadFile("testdata/legacy-golden.json")
	if e != nil {
		t.Fatal(e)
	}
	var g struct {
		Cases []struct {
			Name      string
			Input     json.RawMessage
			Candidate map[string]any
			Dotenv    string
			Audit     Audit
			Error     string
		}
		Quotes []struct{ Input, Quoted, Error string }
	}
	if e = json.Unmarshal(raw, &g); e != nil {
		t.Fatal(e)
	}
	for _, c := range g.Cases {
		t.Run(c.Name, func(t *testing.T) {
			candidate, dotenv, audit, e := Prepare(c.Input, "news")
			if c.Error != "" {
				if e == nil || e.Error() != c.Error {
					t.Fatalf("error %v", e)
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			candidateJSON, _ := json.Marshal(candidate)
			var normalized map[string]any
			if err := json.Unmarshal(candidateJSON, &normalized); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(normalized, c.Candidate) {
				t.Fatal("candidate differs")
			}
			if dotenv != c.Dotenv {
				t.Fatalf("dotenv differs:\n%q\n%q", dotenv, c.Dotenv)
			}
			if !reflect.DeepEqual(audit, c.Audit) {
				t.Fatalf("audit differs: %#v %#v", audit, c.Audit)
			}
		})
	}
	for _, q := range g.Quotes {
		actual, e := Quote(q.Input)
		if q.Error != "" {
			if e == nil || e.Error() != q.Error {
				t.Fatalf("quote error %v", e)
			}
		} else if e != nil || actual != q.Quoted {
			t.Fatalf("quote %q %v", actual, e)
		}
	}
}
