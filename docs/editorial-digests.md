# Editorial digests

The Russian editorial format is **Главное + ещё коротко**: up to five main stories, followed by short stories grouped by category. Related reports may form one coherent umbrella story; verbatim duplicates are not required. For example, same-day coverage of one product refresh or reports about an election result and its immediate reactions may share a story when the sources support that connection. A shared publisher, category, company, country, topic or product family alone is insufficient. Grouping is presentation, not deduplication: every eligible article remains assigned exactly once with its own evidence. Do not invent a common place, date or causal link, or change the source's scope, attribution or uncertainty. The snapshot already comes from completed, non-advertisement, categorized articles; editorial importance is not permission to drop a material. Offline validation of an explicitly curated edition retains support for recorded exclusion reasons. Ungrouped articles become singleton briefs, and main-story editorial ranking is retained.

## Responsibility boundaries

The `go-editorial` module owns provider-backed editorial generation, validation and rendering. Its bounded JSON CLI runs inside the existing private domain image. It does not serve HTTP, access the database or send messages. Provider credentials remain in the private domain runtime; they do not move into the web or background processes. This adds no container.

The existing domain application supplies a fenced snapshot and stores a completed immutable edition. The Go web reader displays that edition and serves individual article pages. Existing article analysis, categorization and delivery operations remain separate migration work; this feature does not claim to remove Python.

## Edition and links

An edition records its UTC date and cutoff, explicit locale, policy version, article snapshot, deterministic snapshot/content hashes, stories, evidence references, exclusions, rendered representations and provider usage. New source collection must not change an already stored edition. Revisions are separate records. The current renderer supports only `ru` and rejects unsupported locales.

- Main-story headlines and brief summary links lead to `/article/{primary_article_id}` on the configured site.
- Main stories retain the usual headline, summary, and plain source names. Each brief renders its `summary` as one standalone linked sentence, then publisher name(s) on a separate italic line; the brief `title` is not repeated in Telegram. Rich HTML separates brief items within each category with horizontal rules; ordinary HTML uses text dividers and repeats category headings safely when parts split. Keep the summary self-contained with the actor or product and material figures when supported, while retaining title and summary separately on the web edition.
- In Telegram, publisher names are plain text and repeated names within a story appear once. There are no per-story “Подробнее” links. Original URLs remain individually accessible on the web article and digest pages.
- “Полный выпуск” links to `/digest/YYYY-MM-DD?edition=UUID`, selecting the stored edition.
- Both Rich Message HTML and ordinary `sendMessage` HTML preserve the same link destinations. Parts split only between complete stories; text is never silently truncated.
- GET requests only read existing data. They do not call a provider or publish a message.

The public article page uses the existing published-article visibility boundary. Historical dates may use the legacy daily-summary reader when no editorial edition exists. An explicitly requested missing edition must return not found rather than silently selecting another one.

## Validation and limits

Article and category IDs are canonical positive signed-32-bit identifiers matching the current database schema. Category IDs are stable classification identities; display labels are separate supplied metadata. Editorial generation sends saved titles, source/category metadata and deterministic article-bound spans of the saved summary only. Raw content and original URL fields remain in the immutable snapshot for storage/web use and are not serialized into the provider prompt. Empty summaries produce `provider_summary_missing` before HTTP; the domain wrapper maps this to `editorial_not_ready`. No article is silently skipped and no raw-content fallback is performed.

The internal draft contains required `main_stories` and `brief_stories` group arrays and an `article_stories` object with a required `brief_summary` and `span_id` for every supplied article ID. Each compact sentence is based on the ready summary, avoiding a second full article analysis. Singleton titles are copied from the snapshot; original article titles/summaries are not overwritten. Unknown or missing article keys, overlapping group members and cross-article span references are rejected. Main stories retain model order; briefs follow the earliest member in snapshot order. Go builds story kinds, IDs, links, quote text and provenance.

New evidence carries optional `basis: "summary"` and must match an exact bounded substring of that article's saved summary. Legacy evidence with no basis retains its original content-first validation and serialized hashes; unknown basis values are rejected. Edition version 1 and the database schema stay unchanged; new domain writes use policy `ru-v2-summaries`. Evidence traces the supplied summary rather than proving a claim against the original publication.

Generation uses the configured Gemini digest model. Request, response, deadline and subprocess output bounds apply. Errors use finite codes and must not reveal provider payloads or credentials. Rendering is available offline. Provider tests use local fake endpoints.

Evidence matching establishes traceability, not factual truth or correct event clustering. Editorial quality still needs human review on a representative sample. A structurally valid synthetic fixture is not proof of real-provider quality.

## Future Serbian support

Serbian digests will use a separate Telegram channel. The current implementation and acceptance focus on Russian. Future work includes Serbian article titles/summaries, localized category labels and website interface strings. Classification IDs, source links and story membership remain language-independent. Serbian readiness and retries must not invalidate an already completed Russian result or consume article extraction attempts. No Serbian output, fake translations or bulk historical backfill is enabled by this feature.

## Release boundary

Local generation and preview do not authorize live delivery. This change requires PR review before any separately approved release. Production, shared browser, Readeck, live provider evaluation, image publication and Telegram sends remain outside the current implementation acceptance.
