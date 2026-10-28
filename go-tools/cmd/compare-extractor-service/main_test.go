package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMissingTokenExitsSafely(t *testing.T) {
	if os.Getenv("EXTRACTOR_COMPARE_CLI_HELPER") == "1" {
		os.Args = []string{"compare-extractor-service", "--corpus", "fixture-private-path", "--endpoint", "http://127.0.0.1:1", "--output", "unused"}
		os.Exit(run())
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMissingTokenExitsSafely$")
	cmd.Env = []string{"EXTRACTOR_COMPARE_CLI_HELPER=1"}
	out, err := cmd.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 2 {
		t.Fatalf("exit=%v output=%s", err, out)
	}
	if strings.Contains(string(out), "fixture-private-path") || !strings.Contains(string(out), `"code":"comparison_failed"`) {
		t.Fatalf("unsafe output=%s", out)
	}
}
