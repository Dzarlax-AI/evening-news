# Evening News — project instructions

## Scope and safety

- Confirm the repository, branch and dirty state before editing. Preserve unrelated work.
- Commit, push, PR creation, merge and production deployment require explicit authorization.
- Browser and Readeck changes require separate authorization; application work does not authorize them.
- Never print env files, runtime manifests, API keys, passwords, tokens, article bodies or raw SQL diagnostics.
- Remove task-owned temporary files and fixtures after verification. Preserve installation volumes, encryption keys, user changes and the required rollback material.

## Commands

Run from the repository root:

```sh
make managed-up       # Prepare and start a fresh managed local installation
make managed-token    # Issue a one-time first-administrator token
make managed-logs
make managed-stop     # Preserve persistent installation volumes
make test             # All News Go modules
make vet
```

For focused checks, run `go test ./...`, `go test -race ./...`, `go vet ./...` or `go mod verify` inside the affected module. Database integration tests require an isolated loopback `TEST_DATABASE_URL`; never use production or a shared database. Real AI calls incur cost, and Telegram test/digest actions send messages; use synthetic fixtures unless the real operation is authorized.

`make dev`, `make logs`, `make stop` and `make migrate` operate on the separate manual-configuration development profile. See `deploy/news/README.md`; do not mix its env inputs with managed installation state.

## Architecture

The News application is Go. Logical components remain independently replaceable without requiring one container per component or tying contracts to an implementation language.

| Module | Responsibility |
| --- | --- |
| `go-web` | Public reader, admin UI, auth, HTTP/API boundary and typed private-domain client |
| `go-background` | One process hosting the background components |
| `go-worker` | Durable jobs, leases, heartbeat and generation fencing |
| `go-scheduler` | Persisted schedules and dispatch/outbox in a separate scheduler database |
| `go-execution` | Execution receipts and orchestration |
| `go-ingestion` | Collection and transactional article/source persistence |
| `go-processing` | Article processing admission, attempts and retry policy |
| `go-domain` | AI callbacks, categorization, editorial workflow, Telegram delivery and maintenance |
| `go-editorial` | Editorial contracts, validation and rendering |
| `go` | Reusable extraction service, source adapters and native CDP transport |
| `go-tools` | Installer/runtime configuration, CI fixtures and operator tools |

The independent `github.com/Dzarlax-AI/go-ai` module provides Gemini/OpenAI adapters; News owns prompts, retries, validation and accounting. One installation selects its active provider and article/digest/text models. Do not add automatic cross-provider fallback.

Source types are `rss`, `telegram` and `custom` (CSS Page Monitor). The optional shared native Cloak browser and protective egress are separate from News business logic. Four Python launcher/policy/relay files remain under `deploy/news-extractor/browser/` and `scripts/`; they are browser runtime dependencies, not unported application code.

## Data and jobs

PostgreSQL stores articles, source receipts, durable jobs, editions and settings. Scheduler state uses a separate database/schema. Preserve database identities, restricted privileges and ownership boundaries.

- Read inputs, perform network/AI work outside write transactions, then persist results in a short transaction.
- Respect generation/claim fences, idempotency keys and delivery receipts. A timeout does not prove delivery failed; do not automatically resend an unknown outcome.
- Extractor admission waits/busy must not consume article processing attempts.
- News collection/extraction code constructs links; AI generates content and references, never authoritative URLs.
- Editorial digests use saved article summaries, with main stories followed by short items grouped by category. Headlines link to this site's article; source labels link to the original.
- Operational settings are snapshotted for each new job. Changing settings must not mutate an already running job's snapshot.
- Preserve existing database names such as `fetched_at`, `summary_text` and `articles_count`; inspect current SQL/contracts rather than guessing names from the removed Python models.

## Environment variables and configuration

Use `.env.example` for the default managed profile: its only optional input is `SITE_BASE_URL`, defaulting to `http://localhost:8000`. The local HTTP exception is explicit and loopback-published. Public installation requires HTTPS and specific trusted proxy CIDRs.

The installer provisions local databases, restricted roles, passwords, service tokens and signing/encryption state automatically. Managed services read private role files via `NEWS_RUNTIME_FILE`; background may also retain Go process settings such as `GOMAXPROCS` and `GOMEMLIMIT`. Never expose these manifests or manually flatten them into a public env file.

AI/provider keys and Telegram credentials are encrypted in PostgreSQL and configured through `/setup` and admin settings. Sources, categories, schedules and operational limits are also configured through the UI. Networks, database connectivity, browser integration, proxy trust, resource limits and technical timeouts remain operator settings.

`.env.full.example` is the central advanced name inventory; assignments are commented and are not a recommended configuration. `go-tools/configcatalog/catalog.json` defines ownership, storage, precedence, reload behavior and role allowlists. Keep its source locations valid. `.env.legacy.example` remains the manual development-profile input; optional standalone Compose overlays under `deploy/` are not the default managed topology.

## Authentication and acceptance

The transport boundary accepts HTTPS only from actual TLS or an explicitly trusted proxy. Never infer a secure request from `SITE_BASE_URL` alone, enable local HTTP on a public installation, or trust all proxy addresses.

The production admin/API route additionally uses Cloudflare Access. Its TLS-only proxy middleware must replace `X-Forwarded-Proto` with `https`, not remove it. Keep identity-header cleanup and Access verification. Native `/auth/login` acceptance alone does not validate the `/admin` and `/api/v1` middleware path. Use a real authorized session for browser acceptance; never forge Access assertions. Settings reads must load successfully, not merely render the page shell.

After an authorized deployment, verify health, the original user path, normal scheduled receipts, heartbeat, restart/OOM counters and unchanged protected neighbors. Separate deployment evidence from long-term reliability claims; a successful run does not resolve the historical PostgreSQL COMMIT-delay cause.

## Frontend

The UI uses the Book visual system: ivory/graphite, serif headings and sans-serif body, no gradients. Inspect `web/static/css/` and `go-web/templates/` before editing; public and admin templates are distinct. Preserve dark mode, mobile behavior, keyboard access and error/loading states. Use TypeScript for new frontend code; follow existing JS for localized maintenance.

## Recovery and documentation

Managed HTTP-only local recovery differs from adopted/native/external-database recovery. The latter requires a reviewed operator procedure; do not claim universal `newsctl backup/restore/resume` support. A DB dump alone cannot recover encrypted credentials without the matching private keys/manifests. Never remove persistent state volumes during cleanup.

Keep README focused on the current product, architecture and setup, with English technical links and no deployment diary or neighbor-service details. `go-tools/COVERAGE.md` maps current checks and explicit limits; `go-tools/RETIREMENT.md` is historical evidence. Do not delete golden fixtures just because their names contain `legacy`.
