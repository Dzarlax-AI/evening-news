package main

import (
	"flag"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/corerelease"
	"os"
)

func main() {
	rev := flag.String("revision", "", "")
	platform := flag.String("platform", "linux/amd64", "")
	output := flag.String("output", "", "")
	acceptance := flag.String("acceptance-dir", "", "")
	flag.Parse()
	if *output == "" || *acceptance == "" {
		fmt.Fprintln(os.Stderr, "core image release failed (missing_required_argument)")
		os.Exit(1)
	}
	_, e := corerelease.Publish(*rev, *platform, *output, *acceptance, corerelease.Docker)
	if e != nil {
		fmt.Fprintln(os.Stderr, "core image release failed ("+e.Error()+")")
		os.Exit(1)
	}
	fmt.Printf("Recorded three verified core image digests in %s\n", *output)
}
