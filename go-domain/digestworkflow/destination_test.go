package digestworkflow

import "testing"

func TestDestinationFallback(t *testing.T) {
	cases := []struct{ mo, so, md, sd, m, s string }{{"db-main", "", "env-main", "", "db-main", "env-main"}, {"db-main", "db-service", "env-main", "env-service", "db-main", "db-service"}, {"db-main", "", "", "", "db-main", "db-main"}, {"", "", "env-main", "env-service", "env-main", "env-service"}}
	for _, c := range cases {
		m, s, err := Destinations(c.mo, c.so, c.md, c.sd)
		if err != nil || m != c.m || s != c.s {
			t.Fatalf("main=%q service=%q err=%v", m, s, err)
		}
	}
}
