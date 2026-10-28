package main

import (
	"errors"
	"strings"
	"testing"
)

func TestDatabaseReadinessRequiresTCPAndAuthenticatedExecutorQuery(t *testing.T) {
	calls := 0
	probe := func(_ []byte, args ...string) (string, error) {
		calls++
		joined := strings.Join(args, " ")
		if calls == 1 {
			if !strings.Contains(joined, "pg_isready -h 127.0.0.1 -U gate_user -d gate_db") {
				t.Fatal("readiness does not check TCP")
			}
			return "", nil
		}
		if !strings.Contains(joined, "--entrypoint /usr/libexec/postgresql17/psql executor") || !strings.Contains(joined, "SELECT 1") || !strings.Contains(joined, "@postgres:5432/gate_db") || !strings.Contains(joined, "connect_timeout=2") {
			t.Fatal("executor network query missing")
		}
		return "1", nil
	}
	if !databaseReady(probe) || calls != 2 {
		t.Fatal("healthy database rejected")
	}
	calls = 0
	if databaseReady(func(_ []byte, _ ...string) (string, error) { calls++; return "", errors.New("fixture") }) || calls != 1 {
		t.Fatal("socket failure not rejected before query")
	}
	calls = 0
	if databaseReady(func(_ []byte, _ ...string) (string, error) {
		calls++
		if calls == 1 {
			return "", nil
		}
		return "", errors.New("fixture")
	}) || calls != 2 {
		t.Fatal("failed executor query accepted")
	}
	calls = 0
	if databaseReady(func(_ []byte, _ ...string) (string, error) {
		calls++
		if calls == 1 {
			return "", nil
		}
		return "0", nil
	}) {
		t.Fatal("invalid query result accepted")
	}
}
