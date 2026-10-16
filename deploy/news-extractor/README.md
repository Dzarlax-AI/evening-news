# HTTP-only extractor pilot draft

These overlays are review artifacts. They have not been copied to the canonical
`personal_ai_stack/deploy/news/` or deployed. Existing service `app`, network
`infra`, and the `ghcr.io/dzarlax-ai` namespace follow the inspected deployment.
`rss-extractor` is the proposed new independently released image package.

The Go service has no published port, Traefik route, Chrome endpoint or application
env file. It cannot access production CDP via configuration. Both service startup
and Python request routing enforce HTTP-only behavior. Python keeps its existing
native CloakBrowser transport for approved fallback, including its normal limits.
Shared `infra-chrome` is not changed or restarted.

The 256-MiB/one-CPU/two-request settings are conservative pilot starting limits,
not measured final sizing or a memory-saving claim. Resolve load measurements
before activation. The total budget includes Go, sequential Python fallback and
learning; the outer processing attempt budget still applies.

## Release and standby

After explicit commit/push/PR authorization and review, run the extractor workflow
on the default branch with `publish=true`. Other events verify only. It publishes
`ghcr.io/dzarlax-ai/rss-extractor:sha-<commit>` and records its digest as an artifact.
Deploy using that digest, not a mutable tag. No images are built on the VPS.

Prepare a root-only release env file outside git with:

- `NEWS_APP_IMAGE`: verified Python client image ending in `@sha256:<64 hex>`.
- `NEWS_EXTRACTOR_IMAGE`: verified extractor image in the same digest form.
- `NEWS_EXTRACTOR_TOKEN`: a new random token of at least 16 bytes, used by both
  containers. Never reuse the application's full `.env` as Go `env_file`.

Render the canonical base plus `compose.standby.yml` in a trusted shell. The
canonical base still reads its existing application env file; do not overwrite it.
Do not paste rendered Compose output: interpolation contains the service token.
Inspect only a sanitized projection of images, env *names*, modes, networks,
ports and limits. Confirm immutable image refs, HTTP-only mode, no public port and
no change to existing Python browser configuration.

Start standby only after deployment authorization. Python remains in `python`
mode, then independently check extractor authentication, health/version, overload
and private supplied-HTML smoke. Record the actual previous app digest for rollback.

## Canary and rollback

Once source quality, fallback classifications and capacity checks pass, the
`compose.canary.yml` last overlay changes routing to `go`. This is a small-deployment
pilot, **not percentage routing**: all compatible extraction calls in this app
instance use the opt-in path. Do not describe it as a 1% canary.

`compose.rollback.yml` as the final overlay restores Python mode. Compose changes
take effect by recreating only `app`; no PostgreSQL restore, browser restart,
scheduler replay or resend is needed. Returning to a previous app digest remains
a separate rollback option if client startup itself fails. Bound drain before
retiring an old extractor. Authorize this exact rollback with production activation.

Never enable inline `shadow` in production. Use the out-of-band comparison runner
against fixed article fixtures on an isolated service. Full Python+Go+Cloak memory,
latency and soak measurements remain required before activation; existing CDP
failures block Go browser cutover regardless of HTTP-only readiness.
