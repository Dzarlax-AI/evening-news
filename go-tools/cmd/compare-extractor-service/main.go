package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/extractorcompare"
	"os"
)

type selections []string

func (s *selections) String() string     { return fmt.Sprint([]string(*s)) }
func (s *selections) Set(v string) error { *s = append(*s, v); return nil }
func main()                              { os.Exit(run()) }
func run() int {
	var corpus, endpoint, output, tokenEnv string
	var cycles int
	var cases selections
	flag.StringVar(&corpus, "corpus", "", "Captured corpus JSON")
	flag.StringVar(&endpoint, "endpoint", "", "Extractor service endpoint")
	flag.StringVar(&output, "output", "", "Redacted report path")
	flag.StringVar(&tokenEnv, "token-env", "EXTRACTOR_TEST_TOKEN", "Environment variable containing service token")
	flag.IntVar(&cycles, "cycles", 1, "Replay cycles (1-6)")
	flag.Var(&cases, "case", "Case ID; repeat to select")
	flag.Parse()
	if corpus == "" || endpoint == "" || output == "" {
		fmt.Fprintln(os.Stderr, `{"error":"ValueError","code":"comparison_failed"}`)
		return 2
	}
	report, err := extractorcompare.Run(context.Background(), extractorcompare.Options{Corpus: corpus, Endpoint: endpoint, Output: output, Token: os.Getenv(tokenEnv), Cycles: cycles, CaseIDs: cases})
	if err != nil {
		fmt.Fprintf(os.Stderr, "{\"error\":\"%s\",\"code\":\"comparison_failed\"}\n", extractorcompare.ErrorClass(err))
		return 2
	}
	if report.Mismatches > 0 {
		return 1
	}
	return 0
}
