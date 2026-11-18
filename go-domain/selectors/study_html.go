package selectors

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"golang.org/x/net/html"
)

type outlineRow struct {
	value string
	score int
	count int
}
type outlineStat struct{ score, count int }

// PrepareStudyHTML adds a bounded, content-free DOM outline to the legacy
// preview. It preserves selector-bearing structure even on long pages.
func PrepareStudyHTML(input string) (string, error) {
	preview, err := PrepareHTML(input)
	if err != nil {
		return "", err
	}
	root, err := html.Parse(strings.NewReader(input))
	if err != nil {
		return "", err
	}
	rows := map[string]outlineStat{}
	coverRows := map[string]bool{}
	var label func(*html.Node) (string, int)
	label = func(n *html.Node) (string, int) {
		if n == nil || n.Type != html.ElementNode {
			return "", 0
		}
		s := n.Data
		score := 0
		for _, a := range n.Attr {
			if a.Key != "class" && a.Key != "id" {
				continue
			}
			for _, token := range strings.Fields(a.Val) {
				if len(token) > 80 || token == "" {
					continue
				}
				valid := true
				for _, r := range token {
					if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_') {
						valid = false
						break
					}
				}
				if !valid {
					continue
				}
				low := strings.ToLower(token)
				weight := 0
				for _, term := range []string{"article", "body", "prose", "author", "cover", "image", "related", "share", "content", "news", "clear", "card", "listing", "post", "entry"} {
					if strings.Contains(low, term) {
						weight = 2
						break
					}
				}
				if weight == 0 {
					continue
				}
				score += weight
				if a.Key == "id" {
					s += "#" + token
				} else {
					s += "." + token
				}
				if len(s) > 180 {
					break
				}
			}
		}
		if n.Data == "article" || n.Data == "main" || n.Data == "img" || n.Data == "picture" {
			score++
		}
		return s, score
	}
	var visit func(*html.Node)
	visit = func(n *html.Node) {
		if isNoise(n) {
			return
		}
		if n.Type == html.ElementNode {
			if n.Data == "img" && len(coverRows) < 6 {
				var parts []string
				header := false
				for p, depth := n, 0; p != nil && depth < 12; p, depth = p.Parent, depth+1 {
					if p.Type != html.ElementNode {
						continue
					}
					if p.Data == "aside" || p.Data == "nav" || p.Data == "button" {
						break
					}
					label, _ := label(p)
					parts = append([]string{label}, parts...)
					if p.Data == "header" {
						header = true
					}
					if p.Data == "article" {
						if header && hasHeading(p) {
							row := strings.Join(parts, " > ")
							if len(row) <= 300 {
								coverRows[row] = true
							}
						}
						break
					}
				}
			}
			current, score := label(n)
			if score > 0 {
				parent, ps := label(n.Parent)
				grand, gs := "", 0
				if n.Parent != nil {
					grand, gs = label(n.Parent.Parent)
				}
				row := current
				if parent != "" {
					row = parent + " > " + row
				}
				if grand != "" {
					row = grand + " > " + row
				}
				value := score + ps + gs
				if n.Data == "img" {
					value += 2
				}
				stat, exists := rows[row]
				if exists || len(rows) < 5000 {
					stat.count++
					if value > stat.score {
						stat.score = value
					}
					rows[row] = stat
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			visit(c)
		}
	}
	visit(root)
	sorted := make([]outlineRow, 0, len(rows))
	for v, stat := range rows {
		score := stat.score
		if stat.count >= 2 {
			score += 2
		}
		sorted = append(sorted, outlineRow{v, score, stat.count})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].score == sorted[j].score {
			return sorted[i].value < sorted[j].value
		}
		return sorted[i].score > sorted[j].score
	})
	var b strings.Builder
	b.WriteString("STRUCTURAL OUTLINE (element classes; no content):\n")
	coverKeys := make([]string, 0, len(coverRows))
	for key := range coverRows {
		coverKeys = append(coverKeys, key)
	}
	sort.Strings(coverKeys)
	for _, key := range coverKeys {
		line := "ARTICLE HEADER IMAGE: " + key + "\n"
		if b.Len()+len(line) > 900 {
			break
		}
		b.WriteString(line)
	}
	for _, r := range sorted {
		line := fmt.Sprintf("%s [count=%d]\n", r.value, r.count)
		if b.Len()+len(line) > 4900 {
			continue
		}
		b.WriteString(line)
	}
	p := []rune(preview)
	if len(p) > 3000 {
		p = p[:3000]
	}
	return string(p) + "\n" + b.String(), nil
}

func hasHeading(n *html.Node) bool {
	if n.Type == html.ElementNode && n.Data == "h1" {
		return true
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if c.Type == html.ElementNode && c.Data == "article" {
			continue
		}
		if hasHeading(c) {
			return true
		}
	}
	return false
}
