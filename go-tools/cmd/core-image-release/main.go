package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"

	"github.com/Dzarlax-AI/evening-news/go-tools/corerelease"
)

func main() {
	if err := run(); err != nil {
		code := err.Error()
		if !regexp.MustCompile(`^[a-z0-9_]{1,64}$`).MatchString(code) {
			code = "release_failed"
		}
		fmt.Fprintln(os.Stderr, "core image release failed ("+code+")")
		os.Exit(1)
	}
}
func decode(path string, value any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, value)
}
func run() error {
	rev := flag.String("revision", "", "")
	platform := flag.String("platform", "linux/amd64", "")
	output := flag.String("output", "", "")
	acceptance := flag.String("acceptance-dir", "", "")
	catalogPath := flag.String("catalog", "", "")
	impactPath := flag.String("impact-plan", "", "")
	baselinePath := flag.String("baseline", "", "")
	resolver := flag.String("baseline-resolver", "", "executable that emits the current trusted baseline JSON")
	flag.Parse()
	if *output == "" || *acceptance == "" || flag.NArg() != 0 {
		return errors.New("missing_required_argument")
	}
	if *catalogPath == "" && *impactPath == "" && *baselinePath == "" && *resolver == "" {
		return errors.New("selective_arguments_required")
	}
	if *catalogPath == "" || *impactPath == "" || *baselinePath == "" || *resolver == "" {
		return errors.New("selective_arguments_required")
	}
	c, e := corerelease.ReadCatalog(*catalogPath)
	if e != nil {
		return e
	}
	if c.Revision != *rev || c.Platform != *platform {
		return errors.New("revision_or_platform_mismatch")
	}
	var impact corerelease.Impact
	if e = decode(*impactPath, &impact); e != nil {
		return e
	}
	baseline, e := corerelease.ReadCatalog(*baselinePath)
	if e != nil {
		return e
	}
	reports, e := filepath.Glob(filepath.Join(*acceptance, "*.json"))
	if e != nil || len(reports) != 1 {
		return errors.New("acceptance_report_missing_or_ambiguous")
	}
	var receipt corerelease.ReceiptV2
	if e = decode(reports[0], &receipt); e != nil {
		return e
	}
	if e = corerelease.ValidateInputs(c, baseline, impact); e != nil {
		return e
	}
	getBaseline := func() (corerelease.Catalog, error) {
		var v corerelease.Catalog
		out, err := exec.Command(*resolver).Output()
		if err != nil {
			return v, err
		}
		if err = json.Unmarshal(out, &v); err != nil {
			return v, err
		}
		return v, v.Validate()
	}
	reg := registry{run: corerelease.Docker}
	_, e = corerelease.PublishSelective(corerelease.SelectiveOptions{Catalog: c, ExpectedBaseline: baseline, Impact: impact, Receipt: receipt, Baseline: getBaseline, Lookup: reg.lookup, Reconcile: reg.reconcile, Run: corerelease.Docker, Output: *output})
	return e
}
