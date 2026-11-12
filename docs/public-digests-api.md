# Public digest API

These read-only endpoints expose saved editorial editions to native clients. They do not create jobs, call AI providers, or trigger delivery. Responses use `Cache-Control: no-store`.

## Catalog

```http
GET /api/public/digests?locale=ru[&month=2026-11]
```

`locale` is required and must be `ru` or `sr-Latn`. `month` is optional and must be a real `YYYY-MM` month. Without it, the server selects the month containing the latest saved edition for that locale, or the current UTC month when the archive is empty. Explicit months are returned even when they contain no editions.

```json
{
  "version": "1",
  "locale": "ru",
  "timezone": "UTC",
  "month": "2026-11",
  "earliest_date": "2026-11-02",
  "latest": {"date": "2026-11-10", "edition_id": "11111111-1111-4111-8111-111111111111"},
  "issues": [{"date": "2026-11-10", "edition_id": "11111111-1111-4111-8111-111111111111"}]
}
```

`earliest_date` and `latest` describe the whole locale archive. They are `null` for an empty archive. `issues` contains at most one saved edition per date in descending date order. For a date with multiple editions, the latest `created_at`, then greatest UUID, wins. Catalog metadata and issues are read in one repeatable-read, read-only PostgreSQL transaction. These are saved edition records, not delivery receipts.

## Edition detail

```http
GET /api/public/digests/2026-11-10?locale=ru[&edition=11111111-1111-4111-8111-111111111111]
```

`locale` is required. `edition` is optional; when supplied it must be a UUID that identifies an edition for the exact date and locale. Without it, the latest edition for that date and locale is selected. A pinned edition is never replaced by another edition.

The response contains `version`, `date`, `locale`, `edition_id`, `issue_url`, `categories`, and `stories`. Stories are ordered with main stories first, followed by brief stories in the same order used by the website's shared projection. Category IDs and names come from each story's primary article in the saved snapshot; a missing category ID is represented as `other`. Category counts reflect the returned stories. `sources` includes only named sources with safe HTTP(S) URLs.

The response does not include the source snapshot, source article body, evidence or quotes, provider usage, rendered delivery content, job or destination fields, or internal hashes.

## Errors

| Status | Code | Meaning |
| --- | --- | --- |
| 400 | `invalid_request` | Invalid or repeated date, locale, month, UUID, query value, or query encoding. |
| 404 | `digest_not_found` | No saved edition matches the requested date, locale, and optional pinned ID. |
| 503 | `digest_unavailable` | Reader capability, database, permissions, or saved edition data is unavailable or corrupt. |

An absent editorial edition table is treated as an empty catalog and as no matching detail. Invalid saved content is an availability error; it is not converted to an empty digest or replaced with another saved edition.
