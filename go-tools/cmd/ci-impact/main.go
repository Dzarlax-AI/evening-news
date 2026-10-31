package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/Dzarlax-AI/evening-news/go-tools/ciimpact"
)

func run() error {
	root := flag.String("root", "..", "repository root")
	base := flag.String("base", "", "trusted base commit")
	head := flag.String("head", "", "head commit")
	output := flag.String("output", "", "JSON plan output path")
	github := flag.String("github-output", "", "optional GitHub output path")
	summary := flag.String("summary", "", "optional GitHub summary path")
	flag.Parse()
	if *base == "" || *head == "" || *output == "" {
		return fmt.Errorf("--base, --head and --output are required")
	}
	p, err := ciimpact.PlanChanges(*root, *base, *head)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(*output, append(data, '\n'), 0600); err != nil {
		return err
	}
	if *github != "" {
		f, e := os.OpenFile(*github, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
		if e != nil {
			return e
		}
		defer f.Close()
		for _, v := range []struct {
			name  string
			value any
		}{{"modules", p.Modules}, {"targets", p.Targets}, {"fixture_targets", p.FixtureTargets}, {"integration_required", p.IntegrationRequired}, {"fallback_full", p.FallbackFull}, {"image_changed_browser", p.ImageChangedBrowser}} {
			b, e := json.Marshal(v.value)
			if e != nil {
				return e
			}
			if _, e = fmt.Fprintf(f, "%s=%s\n", v.name, b); e != nil {
				return e
			}
		}
		for _, k := range ciimpact.CheckNames {
			if _, e = fmt.Fprintf(f, "check_%s=%t\n", k, p.Checks[k]); e != nil {
				return e
			}
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	if *summary != "" {
		f, e := os.OpenFile(*summary, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
		if e != nil {
			return e
		}
		defer f.Close()
		if _, e = fmt.Fprintf(f, "## CI impact\n\nBase: `%s` · Head: `%s`\n\nModules: `%v`\n\nTargets: `%v` · Fixtures: `%v`\n\nIntegration required: **%t** · Full fallback: **%t**\n\n", p.Base, p.Head, p.Modules, p.Targets, p.FixtureTargets, p.IntegrationRequired, p.FallbackFull); e != nil {
			return e
		}
		keys := make([]string, 0, len(p.Reasons))
		for k := range p.Reasons {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if _, e = fmt.Fprintf(f, "- **%s**: %v\n", k, p.Reasons[k]); e != nil {
				return e
			}
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ci-impact:", err)
		os.Exit(1)
	}
}
