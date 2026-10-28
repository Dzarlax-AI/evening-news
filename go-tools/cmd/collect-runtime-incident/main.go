package main

import (
	"flag"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/incidentbundle"
	"os"
)

func main() { os.Exit(run(os.Args[1:])) }
func run(args []string) int {
	f := flag.NewFlagSet("collect-runtime-incident", flag.ContinueOnError)
	fixture := f.String("fixture", "", "offline JSONL fixture")
	live := f.Bool("live", false, "explicit live collection")
	output := f.String("output", "runtime-incidents", "output directory")
	duration := f.Int("duration-seconds", incidentbundle.MaxSeconds, "duration limit")
	max := f.Int("max-bundles", incidentbundle.MaxBundles, "bundle limit")
	bytes := f.Int("max-output-bytes", incidentbundle.MaxOutputBytes, "output limit")
	dbName := f.String("db-name", "", "database name")
	dbSchema := f.String("db-schema", "", "database schema")
	if f.Parse(args) != nil {
		return 2
	}
	if f.NArg() != 0 || *duration < 1 || *duration > incidentbundle.MaxSeconds || *max < 1 || *max > incidentbundle.MaxBundles || *bytes < 1 || *bytes > incidentbundle.MaxOutputBytes {
		fmt.Fprintln(os.Stderr, "invalid arguments or safety bounds")
		return 2
	}
	if *live {
		name := *dbName
		if name == "" {
			name = os.Getenv("EVENING_NEWS_DB_NAME")
		}
		schema := *dbSchema
		if schema == "" {
			schema = os.Getenv("EVENING_NEWS_DB_SCHEMA")
		}
		config := incidentbundle.LiveConfig{Host: os.Getenv("EVENING_NEWS_SSH_HOST"), Background: os.Getenv("EVENING_NEWS_BACKGROUND_CONTAINER"), Domain: os.Getenv("EVENING_NEWS_DOMAIN_CONTAINER"), Extractor: os.Getenv("EVENING_NEWS_EXTRACTOR_CONTAINER"), DB: os.Getenv("EVENING_NEWS_DB_CONTAINER"), DBUser: os.Getenv("EVENING_NEWS_DB_USER"), DBName: name, Schema: schema}
		count, size, err := incidentbundle.CollectLive(*output, config, *duration, *max, *bytes, incidentbundle.SSHRunner)
		if err != nil {
			fmt.Fprintln(os.Stderr, "collector stopped: configuration or output error")
			return 2
		}
		fmt.Printf("bundles=%d output_bytes=%d\n", count, size)
		return 0
	}
	if *fixture == "" {
		fmt.Fprintln(os.Stderr, "offline mode requires --fixture; live collection requires explicit --live")
		return 2
	}
	count, size, err := incidentbundle.Replay(*fixture, *output, *duration, *max, *bytes)
	if err != nil {
		fmt.Fprintln(os.Stderr, "collector stopped: output or fixture error")
		return 2
	}
	fmt.Printf("bundles=%d output_bytes=%d\n", count, size)
	return 0
}
