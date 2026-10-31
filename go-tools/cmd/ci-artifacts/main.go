package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/ciartifact"
	"io"
	"os"
)

func output(p string, b []byte) error {
	if p == "" {
		_, e := os.Stdout.Write(b)
		if e != nil {
			return fmt.Errorf("output_failed")
		}
		return nil
	}
	f, e := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return fmt.Errorf("output_unavailable")
	}
	if _, e = f.Write(b); e != nil {
		f.Close()
		os.Remove(p)
		return fmt.Errorf("output_failed")
	}
	if f.Close() != nil {
		os.Remove(p)
		return fmt.Errorf("output_failed")
	}
	return nil
}
func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("arguments_invalid")
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	repo := fs.String("repository", "", "repository owner/name")
	head := fs.String("head", "", "full commit SHA")
	root := fs.String("root", ".", "repository checkout")
	out := fs.String("output", "", "output file or directory")
	kind := fs.String("kind", "", "candidate, gate, or fixture")
	key := fs.String("key", "", "fingerprint")
	name := fs.String("name", "", "artifact name")
	runID := fs.Int64("run-id", 0, "artifact workflow run ID")
	allowPR := fs.Bool("allow-current-pr-only-for-gate", false, "allow current PR candidate for isolated gate")
	current := fs.Int64("current-run-id", 0, "current GitHub run ID")
	if fs.Parse(args[1:]) != nil || fs.NArg() != 0 {
		return fmt.Errorf("arguments_invalid")
	}
	c := ciartifact.New(os.Getenv("GITHUB_TOKEN"))
	switch args[0] {
	case "baseline":
		b, e := c.Baseline(*repo, *root, *head)
		if e != nil {
			return e
		}
		return output(*out, b)
	case "find":
		m, e := c.Find(*repo, *kind, *head, *key, *root, *current)
		if e != nil {
			return e
		}
		b, e := json.Marshal(m)
		if e != nil {
			return fmt.Errorf("output_failed")
		}
		return output(*out, append(b, '\n'))
	case "download":
		return c.Download(*repo, *kind, *head, *name, *root, *runID, *current, *out, *allowPR)
	default:
		return fmt.Errorf("arguments_invalid")
	}
}
func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
