// check-core-image-inventory checks the payload of locally built runtime images.
package main

import (
	"fmt"
	"os"
	"os/exec"
)

type imageCheck struct{ target, image, command string }

var checks = []imageCheck{
	{"web", "evening-news-web:consolidation-fixture", `test -x /news-web && test -s /app/go-web/contracts/openapi.json && test -d /app/go-web/templates && test -d /app/web/static && test ! -e /news-background && test ! -e /usr/local/bin/news-editorial && test ! -e /usr/local/bin/news-ai-runtime && test ! -e /usr/local/bin/news-article-analysis && ! command -v python >/dev/null 2>&1 && ! command -v go >/dev/null 2>&1`},
	{"background", "evening-news-background:consolidation-fixture", `test -x /news-background && test -s /usr/share/zoneinfo/Europe/Belgrade && test ! -e /news-web && test ! -d /app && test ! -e /usr/local/bin/news-editorial && test ! -e /usr/local/bin/news-ai-runtime && test ! -e /usr/local/bin/news-article-analysis && ! command -v python >/dev/null 2>&1 && ! command -v go >/dev/null 2>&1`},
	{"executor", "evening-news-executor:consolidation-fixture", `test -x /usr/local/bin/news-domain && test -x /usr/local/bin/news-migrate && command -v pg_dump >/dev/null && command -v psql >/dev/null && test ! -d /app/news_aggregator && test ! -e /usr/local/bin/news-editorial && test ! -e /usr/local/bin/news-ai-runtime && test ! -e /usr/local/bin/news-article-analysis && test ! -d /app/scripts && test ! -d /app/web && test ! -e /news-web && test ! -e /news-background && ! command -v python >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1 && ! command -v pip >/dev/null 2>&1 && ! command -v go >/dev/null 2>&1`},
}

func main() {
	for _, c := range checks {
		cmd := exec.Command("docker", "run", "--rm", "--network", "none", "--read-only", "--entrypoint", "/bin/sh", c.image, "-ec", c.command)
		if cmd.Run() != nil {
			fmt.Fprintln(os.Stderr, "core image inventory check failed ("+c.target+"_inventory_failed)")
			os.Exit(1)
		}
	}
	fmt.Println("Core runtime image inventory checks passed")
}
