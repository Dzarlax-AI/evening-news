package extractor

import (
	"strings"
	"testing"
)

func TestTelegramVideoPlayerPrefersNestedVideoAndPoster(t *testing.T) {
	doc := parse(`<div class="tgme_widget_message">
		<a class="tgme_widget_message_video_player" href="https://t.me/sample/42">
			<video src="https://cdn.test/file/clip.mp4" poster="https://cdn.test/file/poster.jpg"></video>
			<img src="https://cdn.test/file/preview.jpg">
		</a>
		<i class="tgme_widget_message_video_thumb" style="background-image:url('https://cdn.test/file/thumb-preview.jpg')"></i>
		<a class="tgme_widget_message_photo_wrap" style="background-image:url('https://cdn.test/file/gallery.jpg')"></a>
	</div>`)
	media := telegramMedia(doc.Selection.Find(".tgme_widget_message"))

	var videos []Media
	for _, item := range media {
		if item.Type == "video" {
			videos = append(videos, item)
		}
	}
	if len(videos) != 1 {
		t.Fatalf("got %d video items, want one: %+v", len(videos), videos)
	}
	if videos[0].URL != "https://cdn.test/file/clip.mp4" {
		t.Errorf("video URL = %q, want nested MP4 source", videos[0].URL)
	}
	if videos[0].Thumbnail != "https://cdn.test/file/poster.jpg" {
		t.Errorf("video thumbnail = %q, want video poster", videos[0].Thumbnail)
	}
	for _, item := range media {
		if item.Type == "image" && strings.Contains(item.URL, "thumb") {
			t.Errorf("video thumbnail was emitted as independent media: %+v", item)
		}
	}
	if len(media) < 2 || media[1].Type != "image" || media[1].URL != "https://cdn.test/file/gallery.jpg" {
		t.Errorf("gallery image should follow the video and remain the first image: %+v", media)
	} else if media[1].Thumbnail != media[1].URL {
		t.Errorf("gallery thumbnail = %q, want its image URL %q", media[1].Thumbnail, media[1].URL)
	}
}
