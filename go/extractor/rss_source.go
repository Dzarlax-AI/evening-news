package extractor

import (
	"context"
	"encoding/xml"
	"errors"
	stdhtml "html"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"github.com/itlightning/dateparse"
	"github.com/mmcdole/gofeed"
	nethtml "golang.org/x/net/html"
	"golang.org/x/net/html/atom"
)

const MaxRSSItems = 1000

var (
	ErrInvalidRSSFeed   = errors.New("invalid_feed")
	ErrInvalidRSSInput  = errors.New("invalid_input")
	ErrRSSResourceLimit = errors.New("resource_limit")
)

// RSSArticle is the Python Article-compatible output of RSS/Atom collection.
// Nullable Python fields use pointers so JSON encodes absent values as null.
type RSSArticle struct {
	Title       string         `json:"title"`
	URL         string         `json:"url"`
	Content     *string        `json:"content"`
	Summary     *string        `json:"summary"`
	ImageURL    *string        `json:"image_url"`
	PublishedAt string         `json:"published_at"`
	SourceType  string         `json:"source_type"`
	SourceName  string         `json:"source_name"`
	RawData     map[string]any `json:"raw_data"`
}

// ParseRSSSource parses one RSS or Atom document without network access. A
// positive limit keeps the first limit entries that have a link. A zero limit
// means no caller limit, bounded by MaxRSSItems; larger feeds return an error
// rather than being silently truncated.
func ParseRSSSource(ctx context.Context, xmlText string, sourceURL, sourceName string, limit int, now time.Time) ([]RSSArticle, error) {
	if ctx == nil || limit < 0 || limit > MaxRSSItems {
		return nil, ErrInvalidRSSInput
	}
	if len(xmlText) > 5*1024*1024 {
		return nil, ErrRSSResourceLimit
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	xmlText = normalizeRSSXMLDeclaration(xmlText)
	feedURL := safeRSSHTTPURL(sourceURL, "")
	if feedURL == "" {
		return nil, ErrInvalidRSSInput
	}
	entryMetadata, err := scanRSSXML(xmlText, feedURL)
	if err != nil {
		return nil, ErrInvalidRSSFeed
	}

	feed, err := gofeed.NewParser().ParseString(xmlText)
	if err != nil || feed == nil || (feed.FeedType != "rss" && feed.FeedType != "atom") {
		return nil, ErrInvalidRSSFeed
	}
	if limit == 0 && len(feed.Items) > MaxRSSItems {
		return nil, ErrRSSResourceLimit
	}
	capHint := len(feed.Items)
	if limit > 0 && capHint > limit {
		capHint = limit
	}
	articles := make([]RSSArticle, 0, capHint)
	for i, entry := range feed.Items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry == nil || entry.Link == "" {
			continue
		}
		if limit > 0 && len(articles) >= limit {
			break
		}
		meta := rssEntryMetadata{}
		if i < len(entryMetadata) {
			meta = entryMetadata[i]
		}
		articleURL := entry.Link
		if resolved := meta.links[entry.Link]; resolved != "" {
			articleURL = resolved
		} else if meta.linkCount == 1 && meta.singleLink != "" {
			// gofeed can omit the host while resolving a lone relative Atom link;
			// use its only declared alternate when there is no ambiguity.
			articleURL = meta.singleLink
		}
		entryBase := meta.entryBase
		if entryBase == "" {
			entryBase = feedURL
		}
		articleURL = safeRSSHTTPURL(articleURL, entryBase)
		if articleURL == "" {
			continue
		}
		article := RSSArticle{
			Title:       entry.Title,
			URL:         articleURL,
			PublishedAt: rssPublishedAt(entry, now),
			SourceType:  "rss",
			SourceName:  sourceName,
			RawData: map[string]any{
				"guid":   rssGUID(entry),
				"author": rssAuthor(entry),
				"tags":   append([]string{}, entry.Categories...),
			},
		}
		if article.Title == "" && !meta.titleSeen {
			article.Title = "No title"
		}
		if entry.Content != "" {
			article.Content = stringPointer(sanitizeRSSHTML(entry.Content, meta.contentBase))
		} else if entry.Description != "" {
			article.Summary = stringPointer(sanitizeRSSHTML(entry.Description, meta.descriptionBase))
		}
		if image := rssImage(entry, meta, feedURL); image != "" {
			article.ImageURL = stringPointer(image)
		}
		articles = append(articles, article)
	}
	return articles, nil
}

type rssEntryMetadata struct {
	entryBase       string
	singleLink      string
	linkCount       int
	links           map[string]string
	titleSeen       bool
	contentBase     string
	descriptionBase string
}

type rssXMLFrame struct {
	name string
	base string
}

// gofeed's pull parser accepts some mismatched closing tags. feedparser marks
// those documents bozo and the Python source rejects them. This strict token
// pass also records Atom xml:base data for canonical entry links; gofeed still
// translates the feed once for its typed fields.
func scanRSSXML(xmlText, feedURL string) ([]rssEntryMetadata, error) {
	decoder := xml.NewDecoder(strings.NewReader(xmlText))
	decoder.Strict = true
	stack := make([]rssXMLFrame, 0, 16)
	entries := make([]rssEntryMetadata, 0)
	activeEntry := -1
	rootSeen := false
	rootClosed := false
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			if len(stack) != 0 || activeEntry >= 0 || !rootSeen || !rootClosed {
				return nil, ErrInvalidRSSFeed
			}
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		switch value := token.(type) {
		case xml.StartElement:
			if len(stack) == 0 {
				if rootSeen {
					return nil, ErrInvalidRSSFeed
				}
				rootSeen = true
			}
			base := feedURL
			if len(stack) > 0 {
				base = stack[len(stack)-1].base
			}
			for _, attr := range value.Attr {
				if (attr.Name.Space == "xml" || attr.Name.Space == "http://www.w3.org/XML/1998/namespace") && attr.Name.Local == "base" {
					base = resolveRSSURI(base, strings.TrimSpace(attr.Value))
					break
				}
			}
			localName := strings.ToLower(value.Name.Local)
			if activeEntry < 0 && (localName == "entry" || localName == "item") {
				entries = append(entries, rssEntryMetadata{entryBase: base, contentBase: base, descriptionBase: base, links: map[string]string{}})
				activeEntry = len(entries) - 1
			}
			if activeEntry >= 0 {
				meta := &entries[activeEntry]
				switch localName {
				case "title":
					meta.titleSeen = true
				case "content", "encoded":
					meta.contentBase = base
				case "summary", "description":
					meta.descriptionBase = base
				case "link":
					href, rel := "", ""
					for _, attr := range value.Attr {
						if attr.Name.Local == "href" {
							href = strings.TrimSpace(attr.Value)
						}
						if attr.Name.Local == "rel" {
							rel = strings.ToLower(strings.TrimSpace(attr.Value))
						}
					}
					if href != "" && (rel == "" || rel == "alternate") {
						resolved := resolveRSSURI(base, href)
						meta.linkCount++
						meta.singleLink = resolved
						meta.links[href] = resolved
						meta.links[resolved] = resolved
					}
				}
			}
			stack = append(stack, rssXMLFrame{name: value.Name.Local, base: base})
		case xml.EndElement:
			if len(stack) == 0 || stack[len(stack)-1].name != value.Name.Local {
				return nil, ErrInvalidRSSFeed
			}
			localName := strings.ToLower(value.Name.Local)
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				rootClosed = true
			}
			if activeEntry >= 0 && (localName == "entry" || localName == "item") {
				activeEntry = -1
			}
		case xml.CharData:
			if len(stack) == 0 && strings.TrimSpace(string(value)) != "" {
				return nil, ErrInvalidRSSFeed
			}
		}
	}
}

func resolveRSSURI(base, reference string) string {
	if reference == "" {
		return ""
	}
	rel, err := url.Parse(reference)
	if err != nil {
		return ""
	}
	if base == "" {
		return rel.String()
	}
	parsedBase, err := url.Parse(base)
	if err != nil {
		return rel.String()
	}
	return parsedBase.ResolveReference(rel).String()
}

// Network decoding produces UTF-8 bytes. Update a bounded XML declaration so
// both XML parsers interpret those bytes as UTF-8 instead of decoding again.
func normalizeRSSXMLDeclaration(xmlText string) string {
	start := 0
	if strings.HasPrefix(xmlText, "\xef\xbb\xbf") {
		start = len("\xef\xbb\xbf")
	}
	end := strings.Index(xmlText[start:], "?>")
	if end < 0 || end > 256 {
		return xmlText
	}
	end += start + 2
	declaration := xmlText[start:end]
	if !strings.HasPrefix(strings.ToLower(declaration), "<?xml") {
		return xmlText
	}
	if len(declaration) <= len("<?xml") || (!isXMLSpace(declaration[len("<?xml")]) && declaration[len("<?xml")] != '?') {
		return xmlText
	}
	lower := strings.ToLower(declaration)
	index := strings.Index(lower, "encoding")
	if index < 0 || (index > 0 && !isXMLSpace(declaration[index-1])) {
		return xmlText
	}
	index += len("encoding")
	if index >= len(declaration) || (!isXMLSpace(declaration[index]) && declaration[index] != '=') {
		return xmlText
	}
	for index < len(declaration) && isXMLSpace(declaration[index]) {
		index++
	}
	if index >= len(declaration) || declaration[index] != '=' {
		return xmlText
	}
	index++
	for index < len(declaration) && isXMLSpace(declaration[index]) {
		index++
	}
	if index >= len(declaration) || (declaration[index] != '\'' && declaration[index] != '"') {
		return xmlText
	}
	quote := declaration[index]
	valueStart := index + 1
	valueEnd := strings.IndexByte(declaration[valueStart:], quote)
	if valueEnd < 0 || valueEnd > 40 {
		return xmlText
	}
	valueEnd += valueStart
	return xmlText[:start+valueStart] + "UTF-8" + xmlText[start+valueEnd:]
}

func isXMLSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

var rssHTMLAllowedTags = map[string]bool{
	"a": true, "abbr": true, "acronym": true, "address": true, "article": true, "aside": true,
	"b": true, "big": true, "blockquote": true, "br": true, "caption": true, "center": true,
	"cite": true, "code": true, "dd": true, "del": true, "details": true, "dfn": true, "div": true,
	"dl": true, "dt": true, "em": true, "figcaption": true, "figure": true, "footer": true,
	"h1": true, "h2": true, "h3": true, "h4": true, "h5": true, "h6": true, "header": true,
	"hr": true, "i": true, "img": true, "ins": true, "kbd": true, "li": true, "nav": true,
	"ol": true, "p": true, "pre": true, "q": true, "s": true, "samp": true, "section": true,
	"small": true, "source": true, "span": true, "strong": true, "sub": true, "sup": true,
	"table": true, "tbody": true, "td": true, "tfoot": true, "th": true, "thead": true,
	"time": true, "tr": true, "tt": true, "u": true, "ul": true, "var": true,
}

var rssHTMLVoidTags = map[string]bool{"area": true, "base": true, "br": true, "col": true, "embed": true, "hr": true, "img": true, "input": true, "link": true, "meta": true, "param": true, "source": true, "track": true, "wbr": true}
var rssHTMLDropTags = map[string]bool{"script": true, "style": true, "iframe": true, "object": true, "embed": true, "applet": true}

func sanitizeRSSHTML(value, base string) string {
	contextNode := &nethtml.Node{Type: nethtml.ElementNode, Data: "div", DataAtom: atom.Div}
	fragment, err := nethtml.ParseFragment(strings.NewReader(value), contextNode)
	if err != nil {
		return strings.TrimSpace(value)
	}
	var out strings.Builder
	for _, node := range fragment {
		renderRSSHTML(&out, node, base)
	}
	return strings.TrimSpace(out.String())
}

func renderRSSHTML(out *strings.Builder, node *nethtml.Node, base string) {
	switch node.Type {
	case nethtml.TextNode:
		out.WriteString(stdhtml.EscapeString(node.Data))
	case nethtml.ElementNode:
		tag := strings.ToLower(node.Data)
		if rssHTMLDropTags[tag] {
			return
		}
		if !rssHTMLAllowedTags[tag] {
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				renderRSSHTML(out, child, base)
			}
			return
		}
		out.WriteByte('<')
		out.WriteString(tag)
		attrs := append([]nethtml.Attribute(nil), node.Attr...)
		sort.SliceStable(attrs, func(i, j int) bool { return attrs[i].Key < attrs[j].Key })
		for _, attr := range attrs {
			name := strings.ToLower(attr.Key)
			value := attr.Val
			if !rssHTMLAttributeAllowed(name) {
				continue
			}
			if name == "href" {
				value = safeRSSHTMLURI(resolveRSSURI(base, strings.TrimSpace(value)))
			} else if name == "src" {
				value = safeRSSHTTPURL(value, base)
				if value == "" {
					continue
				}
			}
			out.WriteByte(' ')
			out.WriteString(name)
			out.WriteString(`="`)
			out.WriteString(stdhtml.EscapeString(value))
			out.WriteByte('"')
		}
		if rssHTMLVoidTags[tag] {
			out.WriteString(" />")
			return
		}
		out.WriteByte('>')
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			renderRSSHTML(out, child, base)
		}
		out.WriteString("</")
		out.WriteString(tag)
		out.WriteByte('>')
	case nethtml.DocumentNode:
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			renderRSSHTML(out, child, base)
		}
	}
}

func rssHTMLAttributeAllowed(name string) bool {
	switch name {
	case "abbr", "align", "alt", "axis", "border", "cellpadding", "cellspacing", "char", "charoff", "cite", "class", "colspan", "datetime", "dir", "height", "href", "hreflang", "id", "lang", "longdesc", "name", "rel", "rowspan", "scope", "src", "start", "summary", "title", "type", "valign", "value", "width":
		return true
	default:
		return false
	}
}

func safeRSSHTMLURI(value string) string {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil {
		return ""
	}
	if parsed.Scheme == "" || parsed.Scheme == "http" || parsed.Scheme == "https" || parsed.Scheme == "mailto" || parsed.Scheme == "ftp" {
		return value
	}
	return ""
}

// safeRSSHTTPURL intentionally applies stricter filtering than feedparser's
// parity model: downstream templates emit article and image URLs into href/src
// attributes, so unsafe or ambiguous values are skipped instead of preserved.
func safeRSSHTTPURL(raw, base string) string {
	if raw == "" || len(raw) > 8192 {
		return ""
	}
	if hasUnsafeRSSURLChar(raw) {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if !parsed.IsAbs() {
		if base == "" {
			return ""
		}
		parsedBase, err := url.Parse(base)
		if err != nil || !parsedBase.IsAbs() {
			return ""
		}
		parsed = parsedBase.ResolveReference(parsed)
	}
	parsed.Scheme = strings.ToLower(parsed.Scheme)
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.Opaque != "" {
		return ""
	}
	if strings.HasSuffix(parsed.Host, ":") {
		return ""
	}
	if port := parsed.Port(); port != "" {
		portNumber, err := strconv.Atoi(port)
		if err != nil || portNumber < 0 || portNumber > 65535 {
			return ""
		}
	}
	resolved := parsed.String()
	if len(resolved) == 0 || len(resolved) > 8192 {
		return ""
	}
	return resolved
}

func hasUnsafeRSSURLChar(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) || r == '"' || r == '\'' || r == '<' || r == '>' || r == '`' {
			return true
		}
	}
	return false
}

func stringPointer(value string) *string { return &value }

func rssGUID(entry *gofeed.Item) string {
	if entry.GUID != "" {
		return entry.GUID
	}
	return entry.Link
}

func rssAuthor(entry *gofeed.Item) string {
	if len(entry.Authors) > 0 && entry.Authors[0] != nil {
		if entry.Authors[0].Name != "" {
			return entry.Authors[0].Name
		}
		if entry.Authors[0].Email != "" {
			return entry.Authors[0].Email
		}
	}
	if entry.Author != nil {
		if entry.Author.Name != "" {
			return entry.Author.Name
		}
		return entry.Author.Email
	}
	return ""
}

func rssPublishedAt(entry *gofeed.Item, now time.Time) string {
	// Parse raw fields in Python's priority order. In particular, a malformed
	// published value must not hide a valid updated value.
	for _, value := range []string{entry.Published, entry.Updated} {
		if parsed, ok := parseRSSDate(value); ok {
			return parsed.UTC().Format(time.RFC3339)
		}
	}
	if entry.DublinCoreExt != nil {
		for _, value := range entry.DublinCoreExt.Date {
			if parsed, ok := parseRSSDate(value); ok {
				return parsed.UTC().Format(time.RFC3339)
			}
		}
	}
	namespaces := make([]string, 0, len(entry.Extensions))
	for namespace := range entry.Extensions {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)
	for _, namespace := range namespaces {
		for _, created := range entry.Extensions[namespace]["created"] {
			if parsed, ok := parseRSSDate(created.Value); ok {
				return parsed.UTC().Format(time.RFC3339)
			}
		}
	}
	for _, parsed := range []*time.Time{entry.PublishedParsed, entry.UpdatedParsed} {
		if parsed != nil {
			return parsed.UTC().Format(time.RFC3339)
		}
	}
	return now.UTC().Format(time.RFC3339)
}

func parseRSSDate(value string) (time.Time, bool) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, false
	}
	parsed, err := dateparse.ParseAny(value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func rssImage(entry *gofeed.Item, meta rssEntryMetadata, feedURL string) string {
	entryBase := meta.entryBase
	if entryBase == "" {
		entryBase = feedURL
	}
	for _, enclosure := range entry.Enclosures {
		if enclosure != nil && strings.HasPrefix(strings.ToLower(enclosure.Type), "image/") && enclosure.URL != "" {
			if imageURL := safeRSSHTTPURL(enclosure.URL, entryBase); imageURL != "" {
				return imageURL
			}
		}
	}
	for _, extName := range []string{"media", "media:content", "media_content"} {
		for _, media := range entry.Extensions[extName]["content"] {
			imageURL := media.Attrs["url"]
			if imageURL != "" && (media.Attrs["medium"] == "image" || strings.HasPrefix(strings.ToLower(media.Attrs["type"]), "image/") || looksImage(imageURL)) {
				if imageURL = safeRSSHTTPURL(imageURL, entryBase); imageURL != "" {
					return imageURL
				}
			}
		}
	}
	for _, extName := range []string{"media", "media:thumbnail", "media_thumbnail"} {
		for _, thumbnail := range entry.Extensions[extName]["thumbnail"] {
			if imageURL := safeRSSHTTPURL(thumbnail.Attrs["url"], entryBase); imageURL != "" {
				return imageURL
			}
		}
	}
	for _, candidate := range []struct{ html, base string }{{entry.Content, meta.contentBase}, {entry.Description, meta.descriptionBase}} {
		htmlBlob := candidate.html
		if htmlBlob == "" || !strings.Contains(strings.ToLower(htmlBlob), "<img") {
			continue
		}
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlBlob))
		if err != nil {
			continue
		}
		imageURL, exists := doc.Find("img[src]").First().Attr("src")
		if !exists || !looksImage(imageURL) {
			continue
		}
		imageBase := candidate.base
		if imageBase == "" {
			imageBase = entryBase
		}
		if safeURL := safeRSSHTTPURL(imageURL, imageBase); safeURL != "" {
			return safeURL
		}
	}
	return ""
}
