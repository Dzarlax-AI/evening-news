// Package selectors implements Page Monitor selector discovery.
package selectors

import (
	"bytes"
	"io"
	"sort"
	"strings"

	"golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

// PrepareHTML removes non-content nodes before passing an HTML5-normalized
// snapshot to the selector prompt. It keeps the beginning and end at the
// legacy 8,000-character boundary. HTML5 tree repair can differ from the old
// Python parser for malformed markup.
func PrepareHTML(input string) (string, error) {
	var nodes []*html.Node
	if isDocument(input) {
		root, err := html.Parse(strings.NewReader(input))
		if err != nil {
			return "", err
		}
		nodes = []*html.Node{root}
	} else {
		ctx := &html.Node{Type: html.ElementNode, Data: "div", DataAtom: atom.Div}
		var err error
		nodes, err = html.ParseFragment(strings.NewReader(input), ctx)
		if err != nil {
			return "", err
		}
	}
	var buf bytes.Buffer
	for _, node := range nodes {
		if isNoise(node) {
			continue
		}
		clean(node)
		if err := html.Render(&buf, node); err != nil && err != io.EOF {
			return "", err
		}
	}
	out := []rune(buf.String())
	if len(out) > 8000 {
		return string(out[:4000]) + "\n\n... [TRUNCATED] ...\n\n" + string(out[len(out)-4000:]), nil
	}
	return string(out), nil
}
func isDocument(input string) bool {
	t := html.NewTokenizer(strings.NewReader(input))
	for {
		switch t.Next() {
		case html.ErrorToken:
			return false
		case html.DoctypeToken:
			return true
		case html.StartTagToken, html.SelfClosingTagToken:
			if t.Token().Data == "html" {
				return true
			}
		}
	}
}
func isNoise(n *html.Node) bool {
	return n.Type == html.ElementNode && (n.Data == "script" || n.Data == "style" || n.Data == "noscript" || n.Data == "iframe" || n.Data == "embed")
}
func clean(n *html.Node) {
	if n.Type == html.ElementNode {
		sort.SliceStable(n.Attr, func(i, j int) bool { return n.Attr[i].Key < n.Attr[j].Key })
	}
	for child := n.FirstChild; child != nil; {
		next := child.NextSibling
		if isNoise(child) {
			n.RemoveChild(child)
		} else {
			clean(child)
		}
		child = next
	}
}
