package main

import "testing"

func TestRequiresFixtureAndFailsClosedLive(t *testing.T) {
	if run(nil) != 2 {
		t.Fatal("fixture required")
	}
	t.Setenv("EVENING_NEWS_SSH_HOST", "")
	if run([]string{"--live"}) != 2 {
		t.Fatal("invalid live config must fail closed")
	}
}
