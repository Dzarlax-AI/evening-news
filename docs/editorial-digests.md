# Editorial digests

The Russian editorial format is **Главное + ещё коротко**: up to five main stories, followed by short stories grouped by category. Related reports may form one story; unrelated reports must not be joined merely because they share a category. Generation must assign every supplied eligible article to exactly one story and rejects model-proposed exclusions. The snapshot already comes from completed, non-advertisement, categorized articles; editorial importance is not permission to drop a material. Offline validation of an explicitly curated edition retains support for recorded exclusion reasons. Brief stories each contain exactly one article; same-event grouping is currently limited to main stories to prevent broad category roundups from hiding individual news.

## Responsibility boundaries

The `go-editorial` module owns provider-backed editorial generation, validation and rendering. Its bounded JSON CLI runs inside the existing private domain image. It does not serve HTTP, access the database or send messages. Provider credentials remain in the private domain runtime; they do not move into the web or background processes. This adds no container.

The existing domain application supplies a fenced snapshot and stores a completed immutable edition. The Go web reader displays that edition and serves individual article pages. Existing article analysis, categorization and delivery operations remain separate migration work; this feature does not claim to remove Python.

## Edition and links

An edition records its UTC date and cutoff, explicit locale, policy version, article snapshot, deterministic snapshot/content hashes, stories, evidence references, exclusions, rendered representations and provider usage. New source collection must not change an already stored edition. Revisions are separate records. The current renderer supports only `ru` and rejects unsupported locales.

- A story headline links to `/article/{primary_article_id}` on the configured site.
- A source label links to the original URL from the snapshot. Multiple sources remain individually accessible.
- “Полный выпуск” links to `/digest/YYYY-MM-DD?edition=UUID`, selecting the stored edition.
- Both Rich Message HTML and ordinary `sendMessage` HTML preserve the same link destinations. Parts split only between complete stories; text is never silently truncated.
- GET requests only read existing data. They do not call a provider or publish a message.

The public article page uses the existing published-article visibility boundary. Historical dates may use the legacy daily-summary reader when no editorial edition exists. An explicitly requested missing edition must return not found rather than silently selecting another one.

## Validation and limits

Article and category IDs are canonical positive signed-32-bit identifiers matching the current database schema. Category IDs are stable classification identities; display labels are separate supplied metadata. The provider receives deterministic, article-bound source-span IDs and exact source text, preferring content with summary/title fallback only when content is empty. Its internal draft has separate main-story and brief-story arrays; a brief has one scalar article ID and span ID. Story kinds and public evidence fields are constructed by the application. The provider returns span references rather than copied quotations. Go resolves each reference back to an exact source substring and rejects unknown, cross-article, duplicate, or missing references. The persisted evidence format remains quote text. Source IDs, evidence snippets and coverage are checked against the fixed input. The application builds links; the model does not choose URLs or HTML.

Generation uses the configured Gemini digest model. Request, response, deadline and subprocess output bounds apply. Errors use finite codes and must not reveal provider payloads or credentials. Rendering is available offline. Provider tests use local fake endpoints.

Evidence matching establishes traceability, not factual truth or correct event clustering. Editorial quality still needs human review on a representative sample. A structurally valid synthetic fixture is not proof of real-provider quality.

## Future Serbian support

Serbian digests will use a separate Telegram channel. The current implementation and acceptance focus on Russian. Future work includes Serbian article titles/summaries, localized category labels and website interface strings. Classification IDs, source links and story membership remain language-independent. Serbian readiness and retries must not invalidate an already completed Russian result or consume article extraction attempts. No Serbian output, fake translations or bulk historical backfill is enabled by this feature.

## Release boundary

Local generation and preview do not authorize live delivery. This change requires PR review before any separately approved release. Production, shared browser, Readeck, live provider evaluation, image publication and Telegram sends remain outside the current implementation acceptance.
