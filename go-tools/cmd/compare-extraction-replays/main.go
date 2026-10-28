package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/replaycheck"
	"os"
	"path/filepath"
)

func main() {
	goPath := flag.String("go", "", "")
	corpus := flag.String("corpus", "", "")
	cycles := flag.Int("cycles", 0, "")
	golden := flag.String("golden", "", "")
	output := flag.String("output", "", "")
	flag.Parse()
	report, e := replaycheck.Check(*goPath, *corpus, *golden, *cycles)
	if e != nil || *output == "" {
		fmt.Fprintln(os.Stderr, "replay comparison failed")
		os.Exit(1)
	}
	if e = os.MkdirAll(filepath.Dir(*output), 0755); e != nil {
		fmt.Fprintln(os.Stderr, "replay comparison output failed")
		os.Exit(1)
	}
	data, _ := json.MarshalIndent(report, "", "  ")
	if e = os.WriteFile(*output, append(data, '\n'), 0644); e != nil {
		fmt.Fprintln(os.Stderr, "replay comparison output failed")
		os.Exit(1)
	}
	fmt.Printf("cases=%d cycles=%d records=%d failures=%d\n", report.CaseCount, report.Cycles, report.RecordCount, len(report.Failures))
	if len(report.Failures) > 0 {
		os.Exit(1)
	}
}
