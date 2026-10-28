package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/extractorsmoke"
	"os"
)

func main() {
	endpoint := flag.String("endpoint", "", "")
	build := flag.String("expected-build", "", "")
	corpus := flag.String("corpus", "tests/fixtures/go-parity/manifest.json", "")
	flag.Parse()
	raw, e := os.ReadFile(*corpus)
	if e != nil {
		fmt.Fprintln(os.Stderr, "extractor smoke failed (fixture_unavailable)")
		os.Exit(1)
	}
	id, e := extractorsmoke.Run(extractorsmoke.HTTPClient(), *endpoint, os.Getenv("EXTRACTOR_TEST_TOKEN"), *build, raw)
	if e != nil {
		fmt.Fprintln(os.Stderr, "extractor smoke failed ("+e.Error()+")")
		os.Exit(1)
	}
	json.NewEncoder(os.Stdout).Encode(map[string]string{"api": "1", "build": *build, "fixture": id, "status": "passed"})
}
