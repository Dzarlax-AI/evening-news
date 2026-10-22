# Extractor deployment profiles (draft)

The HTTP-only overlays described in the following sections are review artifacts.
They have not been copied to the canonical
`personal_ai_stack/deploy/news/` or deployed. Existing service `app`, network
`infra`, and the `ghcr.io/dzarlax-ai` namespace follow the inspected deployment.
`rss-extractor` is the proposed new independently released image package.

For the HTTP-only profile, the Go service has no published port, Traefik route,
Chrome endpoint or application env file. It cannot access production CDP via
configuration. Both service startup and Python request routing enforce HTTP-only
behavior. Python keeps its existing native CloakBrowser transport for approved
fallback, including its normal limits. Shared `infra-chrome` is not changed or
restarted by that profile.

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

## Native browser standby profile (blocked)

`compose.native-standby.yml` changes only the `extractor` worker environment. To
render it, merge in this order: the canonical `personal_ai_stack/deploy/news`
base, this repository's `compose.standby.yml`, then
`compose.native-standby.yml`. Python stays routed in `python` mode because no
application overlay is included. The worker selects the native backend, uses
one worker/browser slot, and has a 55-second stop grace period (36-second drain,
6-second HTTP shutdown, and 6-second browser close, with margin). The existing
256-MiB memory limit is retained as an unmeasured starting limit; it is not a
sizing result.

Rendering requires explicit values for `NEWS_NATIVE_CHROME_ENDPOINT`,
`NEWS_NATIVE_PROXY_URL`, `NEWS_NATIVE_PROXY_TOKEN`, and
`NEWS_NATIVE_NETWORK_ISOLATION_ASSERTION`. Supply these through a protected
operator environment, never in command-line output or a committed env file.
The Compose-required assertion only prevents an accidental omission. The Go
runtime treats the value `true` as an assertion; that setting creates no network
boundary and does not verify one. Do not set it to `true` until a separate,
independently verified network design exists. This overlay does not configure or
verify a boundary around the shared Cloak browser. See
[`shared-browser-isolation.md`](shared-browser-isolation.md) for the separate
browser network design work. No browser service, network policy, or public port
is provisioned by this overlay.

This worker profile is configuration preparation only. Required release and
integration prerequisites remain blocked, including a reviewed isolated browser
network design, release image/digest, measured resource sizing, and real
authenticated browser-path acceptance. It does not make the native backend
production-ready or authorize starting/restarting any service. It adds no
synthetic readiness selector or diagnostics mode.

With Docker Compose available, validate the merged configuration using dummy
values and do not print the rendered configuration, because it contains
interpolated tokens. This example creates a minimal temporary base declaring
only `app` and the external `infra` network, so it does not read the canonical
base or its application env file. It renders the exact order: temporary base,
standby overlay, native overlay.

```sh
base=$(mktemp)
trap 'rm -f "$base"' EXIT
cat > "$base" <<'YAML'
services:
  app: {}
networks:
  infra:
    external: true
YAML

NEWS_APP_IMAGE=example.invalid/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
NEWS_EXTRACTOR_IMAGE=example.invalid/extractor@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
NEWS_EXTRACTOR_TOKEN=placeholder-service-token \
NEWS_NATIVE_CHROME_ENDPOINT=http://browser.invalid:9222 \
NEWS_NATIVE_PROXY_URL=http://proxy.invalid:3128 \
NEWS_NATIVE_PROXY_TOKEN=placeholder-proxy-token \
NEWS_NATIVE_NETWORK_ISOLATION_ASSERTION=false \
docker compose -f "$base" \
  -f deploy/news-extractor/compose.standby.yml \
  -f deploy/news-extractor/compose.native-standby.yml config --quiet
```

Also confirm that rendering fails when any one of the four `NEWS_NATIVE_*`
values is absent. Do not use real secrets for either render, and do not paste
the rendered configuration into logs or chat.

## Python compatibility with the future shared-browser boundary

`compose.python-proxy.yml` is an optional application-only overlay for the new
Python proxy-context adapter. It uses the same gateway URL/token as the native
worker, while the service-to-service extraction token remains separate. It does
not change `EXTRACTION_SERVICE_MODE`. Do not include it in today's production
configuration. It neither creates nor verifies OS isolation.

In the proposed future merge order it follows the canonical base, standby and
native standby overlays; routing activation, if approved, is a separate final
overlay. The Python defaults remain `BROWSER_PROXY_ENABLED=false` with no proxy
settings. Merely removing this compatibility overlay is not a valid rollback
once the shared browser has been isolated: the restored direct tabs would be
blocked. Use the separate boundary rollback described in
[shared-browser-isolation.md](shared-browser-isolation.md).

The new mode deliberately uses disposable contexts: cookies/storage last only
for one `browser_tab` scope and are not a persistent logged-in browser profile.
Only public HTTPS on port 443 is supported by the gateway. Existing HTTP-only
monitored sources must be identified before activation; do not silently rewrite
them, route around isolation or report an empty page as successful extraction.

The application validates the proxy settings at startup. Set all three together:
`BROWSER_PROXY_ENABLED=true`, an `http://<private-gateway>:<explicit-port>` URL
without embedded credentials/path/query/fragment, and a dedicated
`BROWSER_PROXY_TOKEN` of 16–4096 characters. Partial settings, invalid URLs and
proxy values supplied while the mode is disabled are rejected. The token is a
secret field excluded from the settings representation. Gateway authentication
uses username `extractor` and the token as password only for its matching Proxy
authentication challenge; publisher authentication is cancelled.

Unknown context ownership blocks new proxy allocations in this application
process. Restarting that application to clear the latch is a separate operational
action; the adapter does not request a restart of the shared browser. Successful
owned-context cleanup permits ordinary following jobs. Activation still needs
real Cloak acceptance and the Linux network boundary; unit and loopback CDP
checks alone do not establish either.


## Native article routing activation draft

`compose.native-canary.yml` is the application opt-in for native Go/Cloak article
extraction. Merge it last after the canonical base, `compose.standby.yml` and
`compose.native-standby.yml`, only after separately approved deployment and shared
browser isolation acceptance. It sets mode `go`,
`EXTRACTION_SERVICE_NATIVE_BROWSER=true`, and
`EXTRACTION_SERVICE_PYTHON_FALLBACK=false`. This routes all compatible article
requests from that app instance; it is not percentage routing. The older
`compose.canary.yml` remains explicitly HTTP-only.

The native flag defaults to false. When enabled it requires Go mode and disables
automatic Python fallback; invalid combinations fail client configuration. The
request uses `use_browser=true` and the smaller of the HTTP/browser byte caps.
The Go worker retains HTTP-first extraction and may use native Cloak within its
existing limits. Failures continue through existing durable processing state;
there is no second immediate Python browser attempt. Shadow mode remains the
HTTP-only diagnostic path. No API deadline or processing retry policy is changed.

Verify the exact client and worker image digests, authenticated readiness and
`capabilities.native_browser=true`, then a controlled article and queue state
before expanding any future activation. An HTTP-only worker is not a compatible
activation target. No production switch or image publication is performed by
these drafts.

Telegram and Page Monitor still use the Python browser pool. Their current paths
must remain functional before changing the shared browser network boundary; the
Go-only recovery receipt does not accept Python compatibility. Their migration is
a later scope. Applying `compose.rollback.yml` last disables the native request
flag and restores Python routing. This restores application routing only, not
the shared browser network boundary; retain the two-part rollback described in
[shared-browser-isolation.md](shared-browser-isolation.md).
