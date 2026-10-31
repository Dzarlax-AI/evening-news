package editorial

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf16"
)

const (
	ViberRenderVersion     = "1"
	ViberSoftUTF16Limit    = 6500
	ViberHardUTF16Limit    = 7000
	ViberSoftJSONByteLimit = 28000
	ViberHardJSONByteLimit = 30720
	maxViberAuthTokenBytes = 4096
	maxViberSenderBytes    = 255
)

// ViberRenderOptions contains transport data used only to measure full payloads.
// These values are never included in the returned rendering or any error.
type ViberRenderOptions struct {
	AuthToken string
	Sender    string
}

type ViberPartMetrics struct {
	UTF16Units   int  `json:"utf16_units"`
	JSONBytes    int  `json:"json_bytes"`
	SoftUTF16    bool `json:"soft_utf16"`
	SoftJSONByte bool `json:"soft_json_bytes"`
}

type ViberRendered struct {
	Parts    []string           `json:"parts"`
	StoryIDs []string           `json:"story_ids"`
	Version  string             `json:"version"`
	Metrics  []ViberPartMetrics `json:"metrics"`
}

type viberPayload struct {
	AuthToken string `json:"auth_token"`
	From      string `json:"from"`
	Type      string `json:"type"`
	Text      string `json:"text"`
}

// ViberPayloadSize returns the text's UTF-16 length and compact ASCII JSON byte
// length for the complete send_message payload, including auth_token and from.
func ViberPayloadSize(text, sender, token string) (utf16Units, compactJSONBytes int) {
	utf16Units = len(utf16.Encode([]rune(text)))
	encoded, err := json.Marshal(viberPayload{AuthToken: token, From: sender, Type: "text", Text: text})
	if err != nil {
		return utf16Units, 0
	}
	return utf16Units, len(asciiJSON(encoded))
}

// RenderViber creates deterministic plain-text Viber messages. It validates the
// source edition before rendering and rejects a complete story that cannot fit.
func RenderViber(s Snapshot, locale string, e Edition, opts ViberRenderOptions) (ViberRendered, error) {
	if err := ValidateEdition(s, locale, e); err != nil {
		return ViberRendered{}, err
	}
	if len(opts.AuthToken) > maxViberAuthTokenBytes || len(opts.Sender) > maxViberSenderBytes {
		return ViberRendered{}, fmt.Errorf("viber_payload_too_large")
	}
	articles := make(map[string]Article, len(s.Articles))
	for _, article := range s.Articles {
		articles[article.ID] = article
	}
	mains, groups := orderStories(e.Stories, articles)
	labels := renderLabels(locale)
	dateLabel, err := localizedDateLabel(e.Date, locale)
	if err != nil {
		return ViberRendered{}, fmt.Errorf("invalid_edition_date")
	}
	blocks := make([]viberBlock, 0, len(e.Stories)+3)
	metadata := fmt.Sprintf(labels.materials, len(s.Articles))
	if len(e.Excluded) > 0 {
		metadata += fmt.Sprintf(labels.excluded, len(e.Excluded))
	}
	blocks = append(blocks,
		viberBlock{text: viberBold("Evening News · "+dateLabel) + "\n" + metadata},
		viberBlock{text: viberBold(labels.main)},
	)
	if len(mains) == 0 {
		blocks = append(blocks, viberBlock{text: labels.noMain})
	}
	storyIDs := make([]string, 0, len(e.Stories))
	for _, story := range mains {
		blocks = append(blocks, viberStoryBlock(story, articles, e.SiteBaseURL, locale, false))
		storyIDs = append(storyIDs, story.ID)
	}
	if len(groups) > 0 {
		blocks = append(blocks, viberBlock{text: viberBold(labels.briefs)})
		for _, group := range groups {
			for _, story := range group.stories {
				blocks = append(blocks, viberBlock{text: viberBold(group.categoryName) + "\n" + viberStoryBlock(story, articles, e.SiteBaseURL, locale, true).text, storyID: story.ID})
				storyIDs = append(storyIDs, story.ID)
			}
		}
	}

	parts, metrics, err := packViberBlocks(blocks, e.EditionURL, opts)
	if err != nil {
		return ViberRendered{}, err
	}
	return ViberRendered{Parts: parts, StoryIDs: storyIDs, Version: ViberRenderVersion, Metrics: metrics}, nil
}

type viberBlock struct {
	text    string
	storyID string
}

type viberPackedPart struct{ body string }

func viberStoryBlock(story Story, articles map[string]Article, siteBaseURL, locale string, brief bool) viberBlock {
	primary := articles[story.PrimaryArticleID]
	var b strings.Builder
	if brief {
		b.WriteString("• ")
		b.WriteString(story.Summary)
	} else {
		b.WriteString(viberBold(story.Title))
		b.WriteByte('\n')
		b.WriteString(story.Summary)
	}
	b.WriteByte('\n')
	b.WriteString(articleURL(siteBaseURL, primary.ID, locale))
	return viberBlock{text: b.String(), storyID: story.ID}
}

func viberBold(value string) string {
	// Viber's lightweight formatting has no reliable escape for its marker. Keep
	// editorial text byte-for-byte intact and use plain text when it contains one.
	if strings.Contains(value, "*") || strings.ContainsAny(value, "\r\n") {
		return value
	}
	return "*" + value + "*"
}

func packViberBlocks(blocks []viberBlock, editionURL string, opts ViberRenderOptions) ([]string, []ViberPartMetrics, error) {
	// The part count changes footer size. Repack from original block boundaries
	// until the total used for budgeting matches the actual number of parts.
	estimatedTotal := 1
	for attempt := 0; attempt < 8; attempt++ {
		packed, err := packViberBlocksForTotal(blocks, editionURL, opts, estimatedTotal)
		if err != nil {
			return nil, nil, err
		}
		if len(packed) > estimatedTotal {
			estimatedTotal = len(packed)
			continue
		}
		parts := make([]string, len(packed))
		metrics := make([]ViberPartMetrics, len(packed))
		for i, p := range packed {
			parts[i] = viberFinalizePart(p.body, i+1, len(packed), editionURL)
			units, bytes := ViberPayloadSize(parts[i], opts.Sender, opts.AuthToken)
			metrics[i] = ViberPartMetrics{UTF16Units: units, JSONBytes: bytes, SoftUTF16: units <= ViberSoftUTF16Limit, SoftJSONByte: bytes <= ViberSoftJSONByteLimit}
			if units > ViberHardUTF16Limit || bytes > ViberHardJSONByteLimit {
				return nil, nil, fmt.Errorf("viber_payload_too_large")
			}
		}
		return parts, metrics, nil
	}
	return nil, nil, fmt.Errorf("viber_payload_too_large")
}

func packViberBlocksForTotal(blocks []viberBlock, editionURL string, opts ViberRenderOptions, total int) ([]viberPackedPart, error) {
	var packed []viberPackedPart
	current := ""
	for _, block := range blocks {
		candidate := block.text
		if current != "" {
			candidate = current + "\n\n" + block.text
		}
		index := len(packed) + 1
		if viberFits(candidate, editionURL, index, total, opts, ViberSoftUTF16Limit, ViberSoftJSONByteLimit) {
			current = candidate
			continue
		}
		if current != "" {
			packed = append(packed, viberPackedPart{body: current})
			current = block.text
			index = len(packed) + 1
		}
		if !viberFits(current, editionURL, index, total, opts, ViberSoftUTF16Limit, ViberSoftJSONByteLimit) {
			return nil, fmt.Errorf("viber_story_too_large")
		}
	}
	if current != "" {
		packed = append(packed, viberPackedPart{body: current})
	}
	return packed, nil
}

func viberFinalizePart(body string, index, total int, editionURL string) string {
	return fmt.Sprintf("%s\n\nPart %d/%d\n*Ceo pregled:* %s", body, index, total, editionURL)
}

func viberFits(body, editionURL string, index, total int, opts ViberRenderOptions, maxUTF16, maxJSON int) bool {
	text := viberFinalizePart(body, index, total, editionURL)
	units, bytes := ViberPayloadSize(text, opts.Sender, opts.AuthToken)
	return units <= maxUTF16 && bytes <= maxJSON
}

func asciiJSON(encoded []byte) []byte {
	var out bytes.Buffer
	for _, r := range string(encoded) {
		if r <= 0x7f {
			out.WriteByte(byte(r))
			continue
		}
		if r <= 0xffff {
			fmt.Fprintf(&out, "\\u%04x", r)
			continue
		}
		pair := utf16.Encode([]rune{r})
		fmt.Fprintf(&out, "\\u%04x\\u%04x", pair[0], pair[1])
	}
	return out.Bytes()
}
