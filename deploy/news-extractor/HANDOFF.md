# Native Compose release and rollback

The current design is in `compose.shared-browser.yml` (merge into the existing
infra project) and `compose.native.yml` (merge into the existing news project).
Docker manages restart. There is no host supervisor, systemd unit, namespace
anchor, Docker socket mount, or separately running production relay container.

Before applying, record exact images/digests, canonical Compose hashes, app and
browser volumes/networks, scheduler state, and all neighboring container IDs,
start times, restart/OOM counts and health. Select non-overlapping private and
outbound subnets from current Docker/route inspection. Keep the existing shared
browser-control volume. Never overwrite either canonical `.env`.

Prepare a dedicated protected interpolation file using the `NEWS_*` variables
required by the two overlays, with exact image digests and two separate random
tokens (service and proxy). `NEWS_BROWSER_CONTROLLER_IPS` is a JSON object of
exact admitted client addresses. Add a further client only when actual CDP use
and compatibility have been verified; do not infer a Readeck connection from
its service name. Render privately: output includes secrets. Require exactly
one browser interface, no published browser/extractor/proxy ports, app's existing
infra/traefik attachments, and no infra/DB network for worker or proxy.

The derived browser image starts from pinned Cloak and installs its browser-only
namespace policy before starting children. Bootstrap has NET_ADMIN; runtime
parent drops it, and both browser and relay drop all capabilities. Docker's
separate healthcheck reads nft policy and checks real child identity, listener
and loopback CDP. Worker/proxy retain their Go URL, DNS and numeric-dial guards;
they do not use the removed host role policies.

The runtime supervisor tolerates one or two consecutive loopback HTTP/CDP
probe failures and resets the count after a successful probe; the third
consecutive failure exits with `browser_cdp_unavailable`. Identity, child
command/exit, FD threshold, listener, heartbeat, and nft policy failures remain
immediate. Probe logs use finite reasons: `http_timeout`, `invalid_json`,
`invalid_payload`, `connection_error`, `http_error`, `subprocess_timeout`, or
`subprocess_error`, followed by one recovery event when a probe succeeds.
Historical `browser_cdp_unavailable` logs combined listener and probe failures,
so they do not identify the actual cause of the earlier browser restarts.

## Acceptance before production

Use CI-built images in a fresh isolated Compose project with bounded memory,
private fixture TLS and test-only gateway configuration. Verify browser socket
denials, Python shared-browser/parser fixtures, CoreExtractor-to-Go and auth
rejection. Exercise a browser restart and worker restart, then a next article.
Confirm exact cleanup, no new OOM events, and unchanged protected containers.
A successful old host-anchor campaign does not accept this container entrypoint.

Require final PR review and CI for the actual release revision. Promote and
record image digests; never build images on the VPS. Mac tests cover portable
Python/Go behavior; the namespace and capability acceptance runs on Linux/amd64.

## Production cutover

1. Verify Assistant remains stopped with restart policy `no`. Confirm active
   Python browser sources use HTTPS and their proxy adapter has passed the
   isolated fixtures. Preserve Readeck, bot and MCP configurations/networks and
   record a real read-only Readeck API/article baseline.
2. Drain the active News work and pause scheduled work through the existing
   app controls. Save the exact prior state for restoration; do not trigger
   digest delivery or replay completed articles.
3. Install pinned image references and accepted service definitions into the
   canonical infra/news files, retaining original env-file references, mounts,
   unrelated services and routing labels. Keep new tokens in dedicated 0600
   env files; remove their interpolated values from any committed/rendered
   non-secret configuration. Persist the new Chrome definition so an ordinary
   future infra update cannot restore the former unguarded launch.
4. Recreate only `chrome` with `--no-deps`, then start only the news `egress`
   and `extractor` services. Docker stops the old browser before replacement;
   verify only one browser runs and its health/capability/network checks pass.
   Do not run a combined stack `up` or `down` that restarts DB, Redis or Traefik.
5. After worker/proxy readiness and authenticated smoke, recreate only `app`
   with native routing and proxy settings together. Verify public home/feed,
   actual article extraction, durable failed-job/next-job behavior, Python
   browser paths and resource limits before restoring the saved schedule state.
6. Repeat read-only Readeck API/article checks and compare all neighboring
   fingerprints. Report deployment only after those checks pass.

## Rollback

Stop the new browser before returning the former browser definition. Restore
saved canonical News/infra files and exact previous app/browser image digests;
recreate only app/chrome in the appropriate order. Preserve the shared marker
volume and other persistent data. Stop only the new extractor/proxy services
when retiring the native route. Verify health and real News/Readeck read paths,
then restore the saved scheduler state. No database restore, category replay,
Telegram resend, Redis or Traefik restart is part of rollback.

Compose may replace container IDs. Rollback depends on saved immutable images,
configuration and volumes, not retention of the old container ID.
