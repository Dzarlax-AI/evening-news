# Russian editorial edition engine

This module accepts one bounded JSON request on stdin and writes one JSON response on stdout. It is intended for invocation by the existing private domain executor. It does not run a server, persist editions, publish images, send Telegram messages, or call external sites. Only the `generate` operation calls Gemini; `validate` and `render` are local operations.

The request and representative response are in `testdata/generate-request.json` and `testdata/generate-response.json`. The top-level request fields are `operation`, `locale`, and `snapshot`; `edition` is required for `validate` and `render`. `locale` must explicitly be `ru`. Serbian generation and website localization are not implemented.

`snapshot` contains `date` (`YYYY-MM-DD`), `cutoff_utc` (RFC3339 with a UTC offset), `site_base_url`, `edition_url`, and the immutable `articles` array. Each article carries its positive numeric database ID as a decimal string, title, optional summary/content, source name, original HTTP(S) URL, opaque positive numeric category ID, and supplied category display name. The source language is not assumed to be Russian. Category IDs are preserved as supplied and are never translated or inferred by this module.

The `generate` operation requires `GEMINI_API_KEY` and `GEMINI_MODEL`; `GEMINI_ENDPOINT` is optional for a compatible endpoint. It returns an edition with deterministic story IDs, snapshot/content SHA-256 hashes, source evidence, and rich HTML plus Telegram `sendMessage`-compatible fallback parts. Provider usage/model and result hash are returned without credentials. Provider redirects are not followed. Request/response sizes and deadlines are bounded, and error responses contain finite codes only.

Rich HTML, fallback parts, and local preview text link each headline to `/article/{article_id}` under `site_base_url`. Telegram renders publisher names as plain text, deduplicated within a story; original URLs remain available from the web article and digest pages and are never included in the Telegram output. Fallback parts use escaped inline `<b>` and `<a>` markup, keep the supplied immutable `edition_url` linked as “Полный выпуск”, and stay within 4096 UTF-16 code units of visible text. The length check strips only generated inline tags and decodes escaped entities once, so literal text such as `&amp;` counts as five visible characters. Parts split only between complete stories. If one complete story cannot fit, rendering returns `render_story_too_large`; it does not cut or drop it. `preview_text` is unformatted local preview text.

The model draft requires `main_stories`, `brief_stories`, and an `article_stories` object with one record for every snapshot article. Main and brief groups may combine reports only when they describe the same concrete event. The application validates that group IDs are known and assigned once, creates a singleton brief for each ungrouped article, and attaches exact source evidence from the required per-article records. This enforces input coverage and evidence references; it does not prove semantic grouping or summary accuracy.

The provider receives deterministic, application-generated source spans from each article's content (or its summary/title only when content is empty). It returns an `article_stories` object with a required key for every supplied article ID; the engine rejects missing, extra, or cross-article span references and resolves IDs into exact quotes. The app creates one brief per article not assigned to an explicit main or brief group, so missing group entries cannot silently omit eligible materials. Explicit brief groups permit repeated reports about the same concrete event without promoting them to the main section. These checks do not establish that a generated summary is semantically correct, that sources are reliable, or that two reports describe the same event. Human editorial review remains necessary before persistence or publication.

Run the focused module checks with:

```sh
go test ./...
go vet ./...
```
