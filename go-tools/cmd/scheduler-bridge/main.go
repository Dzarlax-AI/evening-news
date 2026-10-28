package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/schedulerbridge"
	"io"
	"os"
	"strings"
	"time"
)

func main() { os.Exit(run(os.Args[1:])) }
func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "scheduler bridge failed: invalid_command")
		return 1
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	output := fs.String("output", "", "")
	snapshot := fs.String("snapshot", "", "")
	api := fs.String("api-url", "", "")
	revision := fs.String("expected-control-revision", "", "")
	allow := fs.Bool("allow-http", false, "")
	timeout := fs.Float64("timeout", 10, "")
	if fs.Parse(args[1:]) != nil || len(fs.Args()) != 0 {
		fmt.Fprintln(os.Stderr, "scheduler bridge failed: invalid_arguments")
		return 1
	}
	dsn := os.Getenv("NEWS_SNAPSHOT_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("TEST_DATABASE_URL")
	}
	cfg := schedulerbridge.ServiceConfig{URL: *api, Token: os.Getenv("SCHEDULER_API_TOKEN"), AllowHTTP: *allow, Timeout: time.Duration(*timeout * float64(time.Second))}
	ctx := context.Background()
	var err error
	switch args[0] {
	case "export":
		if *output == "" {
			err = fmt.Errorf("invalid_arguments")
		} else {
			err = schedulerbridge.Export(ctx, dsn, *output)
		}
	case "import":
		if *snapshot == "" || *api == "" || *revision == "" {
			err = fmt.Errorf("invalid_arguments")
		} else {
			var value any
			value, err = schedulerbridge.Import(ctx, cfg, *snapshot, *revision)
			if err == nil {
				data, e := json.MarshalIndent(value, "", "  ")
				if e != nil {
					err = e
				} else {
					fmt.Println(string(data))
				}
			}
		}
	case "rollback-inventory":
		if *output == "" || *api == "" {
			err = fmt.Errorf("invalid_arguments")
		} else {
			err = schedulerbridge.RollbackInventory(ctx, cfg, dsn, *output)
		}
	default:
		err = fmt.Errorf("invalid_command")
	}
	if err != nil {
		code := err.Error()
		if strings.ContainsAny(code, " \t\r\n") || len(code) > 80 {
			code = "operation_failed"
		}
		fmt.Fprintln(os.Stderr, "scheduler bridge failed:", code)
		return 1
	}
	return 0
}
