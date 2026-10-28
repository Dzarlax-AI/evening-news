package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestGateSignalContextAllowsCleanup(t *testing.T) {
	if os.Getenv("NEWS_FIXTURE_SIGNAL_HELPER") == "1" {
		ctx, stop := gateSignalContext()
		defer stop()
		fmt.Println("ready")
		<-ctx.Done()
		fmt.Println("cleanup")
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestGateSignalContextAllowsCleanup$")
	cmd.Env = append(os.Environ(), "NEWS_FIXTURE_SIGNAL_HELPER=1")
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(pipe)
	if !lines.Scan() || lines.Text() != "ready" {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal("helper not ready")
	}
	if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	if !lines.Scan() || !strings.Contains(lines.Text(), "cleanup") {
		t.Fatal("signal bypassed cleanup")
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("helper did not exit")
	}
}
