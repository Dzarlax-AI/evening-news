package extractor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRSSImagesCollectAllFeedImagesInDocumentOrder(t *testing.T) {
	feed := `<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/" xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel><item>
<title>Fixture</title><link>https://publisher.example/story</link>
<description><![CDATA[<img src="/one.jpg" alt="First"><img src="/opaque?id=2&amp;size=large">]]></description>
<enclosure url="/three.webp" type="image/webp"/><enclosure url="/movie.mp4" type="video/mp4"/>
<media:group><media:content url="/four.png" medium="image"/><media:thumbnail url="/thumb.png"/></media:group>
<content:encoded><![CDATA[<img src="/one.jpg"><img src="javascript:alert(1)"><img src="data:image/png;base64,AA"><img src="/five.gif">]]></content:encoded>
</item><item><title>Empty</title><link>https://publisher.example/empty</link></item></channel></rss>`
	got := rssImageWire(t, feed)
	want := []string{"https://publisher.example/one.jpg", "https://publisher.example/opaque?id=2&size=large", "https://publisher.example/three.webp", "https://publisher.example/four.png", "https://publisher.example/thumb.png", "https://publisher.example/five.gif"}
	var urls []string
	for _, m := range got[0].Media {
		urls = append(urls, m.URL)
		if m.Source != "rss_feed" || m.Type != "image" {
			t.Fatalf("feed image lost provenance/type: %#v", m)
		}
	}
	if !reflect.DeepEqual(urls, want) {
		t.Fatalf("RSS images = %v, want %v", urls, want)
	}
	if got[1].Media == nil || len(got[1].Media) != 0 {
		t.Fatalf("empty RSS images must be an explicit empty list: %#v", got[1].Media)
	}
}

func TestAtomImagesIncludeHTMLXHTMLAndImageAttachments(t *testing.T) {
	feed := `<feed xmlns="http://www.w3.org/2005/Atom" xml:base="https://publisher.example/feeds/"><entry xml:base="../stories/">
<title>Fixture</title><link href="one"/>
<summary type="html" xml:base="../images/">&lt;img src="one.jpg"&gt;</summary>
<content type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml"><p><img src="two.png" alt="Second"/></p></div></content>
<link rel="enclosure" type="image/jpeg" href="three.jpg"/>
</entry></feed>`
	got := rssImageWire(t, feed)
	var urls []string
	for _, m := range got[0].Media {
		urls = append(urls, m.URL)
	}
	want := []string{"https://publisher.example/images/one.jpg", "https://publisher.example/stories/two.png", "https://publisher.example/stories/three.jpg"}
	if !reflect.DeepEqual(urls, want) {
		t.Fatalf("Atom images = %v, want %v", urls, want)
	}
}

func TestRSSImagesAreBounded(t *testing.T) {
	feed := `<rss version="2.0"><channel><item><link>https://publisher.example/story</link><description><![CDATA[` + strings.Repeat(`<img src="/one.jpg">`, 1000) + `]]></description></item></channel></rss>`
	got := rssImageWire(t, feed)
	if len(got[0].Media) != 1 {
		t.Fatalf("repeated images must be deduplicated: %d", len(got[0].Media))
	}
}

func rssImageWire(t *testing.T, feed string) []struct {
	Media []Media `json:"media_files"`
} {
	t.Helper()
	articles, err := ParseRSSSource(context.Background(), feed, "https://publisher.example/feed.xml", "Fixture", 0, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(articles)
	if err != nil {
		t.Fatal(err)
	}
	var result []struct {
		Media []Media `json:"media_files"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestRSSImagesRejectOverflowInsteadOfTruncating(t *testing.T) {
	var html strings.Builder
	for i := 0; i <= maxArticleMediaCount; i++ {
		fmt.Fprintf(&html, `<img src="/image-%d.jpg">`, i)
	}
	feed := `<rss version="2.0"><channel><item><link>https://publisher.example/story</link><description><![CDATA[` + html.String() + `]]></description></item></channel></rss>`
	_, err := ParseRSSSource(context.Background(), feed, "https://publisher.example/feed", "Fixture", 0, time.Now())
	if !errors.Is(err, ErrRSSResourceLimit) {
		t.Fatalf("overflow must be explicit: %v", err)
	}
}
