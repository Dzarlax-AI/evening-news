# Go source collection

The private Go collector owns RSS/Atom fetching, Telegram capture and parsing, Page Monitor capture and parsing, and article extraction. The database-aware Go ingestion service calls the collector for every configured-source collection run, then persists articles and Page Monitor progress in News PostgreSQL. Python source management remains responsible for source CRUD and one-source connection probes; a probe does not update collection state or apply returned articles.

The collector itself remains database-free. It receives bounded source descriptors through `POST /v1/sources/collect`, using bearer authentication and contract version `1`. Requests carry unique positive source IDs and up to eight sources. Results preserve each ID and report `success`, `partial`, `deferred`, `failed`, or `rejected` with finite error codes and retry metadata.

The Go service runs up to five source operations concurrently. Native browser rendering uses the shared admission gate only for actual renders. Request and response bodies are limited to 8 MiB; RSS wire and decoded bodies are limited to 5 MiB. Guarded transport validates public destinations, redirects, DNS resolution, and a 60-second RSS transport timeout. A feed without a caller limit may return at most 1,000 entries.

The collector validates and parses input without access to News tables. RSS parity is characterized against a frozen synthetic RSS/Atom corpus covering dates, content and summary, author/GUID/tags, image priorities, relative links, HTML sanitization, malformed input, cancellation, and size limits. Go package tests cover Telegram and Page Monitor parsing. The isolated Go ingestion fixture runs RSS, Telegram, and Page Monitor sources through the real collector and ingestion service against synthetic origins and PostgreSQL. These checks do not establish compatibility with every real feed.

For local service configuration and isolated acceptance, see the [Go article ingestion guide](go-article-ingestion.md). Run the Go collector checks from `go/`:

```sh
go test -race -count=1 ./...
go vet ./...
go mod verify
```
