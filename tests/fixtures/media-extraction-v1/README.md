# Inline media parity corpus

Four synthetic outputs were captured from the retained Python `processing/media_extractor.py` helper. They cover HTML image attributes, video/source/embed records, linked media, relative URL resolution, duplicate ordering and URL filtering. No media is downloaded.

`go-processing/internal/processing/media_test.go` compares the Go helper with these frozen outputs. Processor tests separately verify the caller enforces text limits before parsing and preserves media on filtering/retry without replacing already-stored media. The retained Python helper still serves the historical Telegram media-refresh utility; it is not an article-processing fallback.
