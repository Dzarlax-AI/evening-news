package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestSafeInvalidArguments(t *testing.T) {
	if os.Getenv("SCHEDULER_BRIDGE_HELPER") == "1" {
		os.Exit(run([]string{"rollback-inventory", "--timeout", "synthetic-secret"}))
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSafeInvalidArguments$")
	cmd.Env = []string{"SCHEDULER_BRIDGE_HELPER=1"}
	out, e := cmd.CombinedOutput()
	exit, ok := e.(*exec.ExitError)
	if !ok || exit.ExitCode() != 1 || strings.Contains(string(out), "synthetic-secret") || !strings.Contains(string(out), "invalid_arguments") {
		t.Fatalf("exit=%v output=%s", e, out)
	}
}
