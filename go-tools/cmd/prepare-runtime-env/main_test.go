package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIPrivateCandidate(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if e := os.Mkdir(bin, 0700); e != nil {
		t.Fatal(e)
	}
	docker := filepath.Join(bin, "docker")
	script := "#!/bin/sh\nprintf '%s' '{\"services\":{\"executor\":{\"environment\":{\"DATABASE_URL\":\"postgres://fixture\",\"TOKEN\":\"fixture-secret\"}}}}'\n"
	if e := os.WriteFile(docker, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	compose := filepath.Join(dir, "compose.yml")
	if e := os.WriteFile(compose, []byte("services: {}"), 0600); e != nil {
		t.Fatal(e)
	}
	out := filepath.Join(dir, "private")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCLIHelper$", "--", "--compose", compose, "--output-dir", out, "--project", "news")
	cmd.Env = append(os.Environ(), "TEST_CLI_HELPER=1", "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	result, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("failed: %v %s", e, result)
	}
	if strings.Contains(string(result), "fixture-secret") {
		t.Fatal("secret in diagnostics")
	}
	for _, name := range []string{".env", "audit.json", "compose.candidate.json"} {
		info, e := os.Stat(filepath.Join(out, name))
		if e != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("%s mode error %v", name, e)
		}
	}
	info, e := os.Stat(out)
	if e != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("directory mode error %v", e)
	}
	audit, e := os.ReadFile(filepath.Join(out, "audit.json"))
	if e != nil || strings.Contains(string(audit), "fixture-secret") {
		t.Fatal("secret in audit")
	}
	cmd = exec.Command(os.Args[0], "-test.run=^TestCLIHelper$", "--", "--compose", compose, "--output-dir", out, "--project", "news")
	cmd.Env = append(os.Environ(), "TEST_CLI_HELPER=1", "PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	result, e = cmd.CombinedOutput()
	if e == nil || !strings.Contains(string(result), "output_directory_exists") {
		t.Fatalf("must not overwrite: %v %s", e, result)
	}
}
func TestCLIHelper(t *testing.T) {
	if os.Getenv("TEST_CLI_HELPER") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{os.Args[0]}, os.Args[i+1:]...)
			break
		}
	}
	main()
	os.Exit(0)
}
