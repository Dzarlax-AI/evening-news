// extract-replay processes a local, captured corpus only. It has no network or sender adapter.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
)

type expectation struct {
	Contains      []string       `json:"contains"`
	NotContains   []string       `json:"not_contains"`
	Fields        map[string]any `json:"fields"`
	Method        string         `json:"method"`
	State         string         `json:"state"`
	TraceContains []string       `json:"trace_contains"`
	Error         string         `json:"error"`
}
type caseDef struct {
	ID         string          `json:"id"`
	SourceType string          `json:"source_type"`
	SourceName string          `json:"source_name"`
	Methods    []string        `json:"methods"`
	Input      extractor.Input `json:"input"`
	Expected   expectation     `json:"expected"`
	Provenance json.RawMessage `json:"provenance"`
}
type manifest struct {
	Version int       `json:"version"`
	Cases   []caseDef `json:"cases"`
}
type record struct {
	CaseID     string           `json:"case_id"`
	ID         string           `json:"id"`
	Cycle      int              `json:"cycle"`
	Methods    []string         `json:"methods"`
	Result     extractor.Result `json:"result"`
	DurationNS int64            `json:"duration_ns"`
	Passed     bool             `json:"passed"`
	Failures   []string         `json:"failures"`
	Memory     memoryMetrics    `json:"memory"`
}

// Fields assert a recursive projection. Array length and order remain strict;
// omitted object fields permit process-specific synthetic identifiers.
func matches(got, want any) bool {
	if want == nil {
		return got == nil
	}
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range w {
			value, exists := g[k]
			if !exists || !matches(value, v) {
				return false
			}
		}
		return true
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			return false
		}
		for i, v := range w {
			if !matches(g[i], v) {
				return false
			}
		}
		return true
	default:
		a, _ := json.Marshal(got)
		b, _ := json.Marshal(want)
		return string(a) == string(b)
	}
}

func check(r extractor.Result, e expectation) []string {
	var fails []string
	if e.Error != "" && r.Error != e.Error {
		fails = append(fails, fmt.Sprintf("error: got %q want %q", r.Error, e.Error))
	}
	for _, s := range e.Contains {
		if !strings.Contains(r.Content, s) {
			fails = append(fails, "missing content: "+s)
		}
	}
	for _, s := range e.NotContains {
		if strings.Contains(r.Content, s) {
			fails = append(fails, "forbidden content: "+s)
		}
	}
	if e.State != "" && r.State != e.State {
		fails = append(fails, fmt.Sprintf("state: got %q want %q", r.State, e.State))
	}
	if e.Method != "" && r.Method != e.Method {
		fails = append(fails, fmt.Sprintf("method: got %q want %q", r.Method, e.Method))
	}
	raw, _ := json.Marshal(r)
	var fields map[string]any
	_ = json.Unmarshal(raw, &fields)
	for k, want := range e.Fields {
		got, ok := fields[k]
		if !ok {
			fails = append(fails, "unknown expected field: "+k)
			continue
		}
		a, _ := json.Marshal(got)
		b, _ := json.Marshal(want)
		if !matches(got, want) {
			fails = append(fails, fmt.Sprintf("field %s: got %s want %s", k, a, b))
		}
	}
	for _, s := range e.TraceContains {
		found := false
		for _, t := range r.Trace {
			if t == s {
				found = true
				break
			}
		}
		if !found {
			fails = append(fails, "missing trace: "+s)
		}
	}
	if e.State == "" && r.State != "extracted" {
		fails = append(fails, "non-extracted result: "+r.State)
	}
	return fails
}
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("extract-replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	corpus := fs.String("corpus", "", "path to version 1 JSON manifest")
	cycles := fs.Int("cycles", 1, "repetitions of each case")
	output := fs.String("output", "-", "JSONL path or - for stdout")
	workers := fs.Int("workers", 2, "bounded concurrent extraction workers (1..8)")
	if fs.Parse(args) != nil {
		return 2
	}
	if *corpus == "" || *cycles < 1 || *workers < 1 || *workers > 8 {
		fmt.Fprintln(stderr, "require -corpus, positive -cycles, and -workers in 1..8")
		return 2
	}
	f, err := os.Open(*corpus)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	defer f.Close()
	// Reserve one sentinel byte so the decoder's synthetic EOF cannot hide overflow.
	bounded := &io.LimitedReader{R: f, N: 64*1024*1024 + 1}
	dec := json.NewDecoder(bounded)
	dec.DisallowUnknownFields()
	var m manifest
	if err = dec.Decode(&m); err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if m.Version != 1 || len(m.Cases) == 0 {
		fmt.Fprintln(stderr, "manifest must have version 1 and nonempty cases")
		return 2
	}
	trailingErr := dec.Decode(new(any))
	if bounded.N == 0 {
		fmt.Fprintln(stderr, "manifest exceeds 64 MiB byte limit")
		return 2
	}
	if trailingErr != io.EOF {
		fmt.Fprintln(stderr, "unexpected trailing manifest data")
		return 2
	}
	seen := map[string]bool{}
	for _, c := range m.Cases {
		if c.ID == "" || seen[c.ID] {
			fmt.Fprintln(stderr, "missing or duplicate case id")
			return 2
		}
		if c.Expected.State == "" {
			fmt.Fprintf(stderr, "case %q requires expected.state\n", c.ID)
			return 2
		}
		if c.Expected.State != "extracted" && c.Expected.State != "failed" && c.Expected.State != "resource_limit" && c.Expected.State != "unsupported" {
			fmt.Fprintf(stderr, "case %q has invalid expected.state\n", c.ID)
			return 2
		}
		seen[c.ID] = true
	}
	var writer io.Writer = stdout
	if *output != "-" {
		out, err := os.Create(*output)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		defer out.Close()
		writer = out
	}
	enc := json.NewEncoder(writer)
	enc.SetEscapeHTML(false)
	type job struct {
		index, cycle int
		c            caseDef
	}
	jobs := make(chan job)
	results := make(chan record, *workers)
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				in := j.c.Input
				in.SourceType = j.c.SourceType
				in.SourceName = j.c.SourceName
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				start := time.Now()
				r := extractor.Extract(ctx, in)
				duration := time.Since(start)
				cancel()
				fails := check(r, j.c.Expected)
				results <- record{CaseID: j.c.ID, ID: j.c.ID, Cycle: j.cycle, Methods: j.c.Methods, Result: r, DurationNS: duration.Nanoseconds(), Passed: len(fails) == 0, Failures: fails, Memory: readMemory()}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for cycle := 1; cycle <= *cycles; cycle++ {
			for i, c := range m.Cases {
				jobs <- job{i, cycle, c}
			}
		}
	}()
	go func() { wg.Wait(); close(results) }()
	failed, total := 0, 0
	for r := range results {
		total++
		if !r.Passed {
			failed++
		}
		if err := enc.Encode(r); err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
	}
	fmt.Fprintf(stderr, "replayed=%d failed=%d workers=%d\n", total, failed, *workers)
	if failed > 0 {
		return 1
	}
	return 0
}
func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
