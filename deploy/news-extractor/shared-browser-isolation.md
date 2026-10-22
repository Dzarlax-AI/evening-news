# Shared Cloak isolation: reviewed preparation, activation blocked

## Decision and evidence

On 2026-10-22 the user selected one shared Cloak instance. This is preparation
for a future approved rollout; no running browser, application, network or
canonical `personal_ai_stack` deployment file was changed.

The current local canonical infra Compose declares one `infra-chrome`, pinned
to Cloak 0.5.2 digest `2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4`,
on network `infra`, using `cloakserve --port=9222`. Read-only VPS inventory found
`news_web` referring to it through `BROWSER_WS_ENDPOINT`; both were healthy.
The inventory covered running containers and the two known endpoint variables
only. It does not prove there are no other CDP clients or hardcoded endpoints.
Evidence: `artifacts/shared-cloak-profile-2026-10-22/inventory.json`.

## Required boundary

The intended path is application → private Go worker → shared Cloak CDP. Browser
page traffic must leave through the existing authenticated Go CONNECT gateway,
which admits public HTTPS destinations and pins the resolved address. The
worker and gateway are services, not additional browser instances. Worker
restart must not terminate the browser or another client's contexts.

A browser attached to an internal Docker network alone is insufficient: access
to peer services, host gateways, IPv6 and loopback can remain. A future Linux
namespace policy must default-deny new browser connections except the gateway;
CDP responses must go only to admitted controllers. Block raw DNS, direct
TCP/UDP, local/private/metadata targets and unwanted loopback listeners. The
browser must lack NET_ADMIN and Docker socket access. Control transport must
be separated from browser page traffic: the existing synthetic stand uses a
loopback CDP listener and a relay with a distinct UID. Reuse that tested boundary
where applicable; its test-only fixtures, namespace anchors and launch script
are not a production supervisor.

The gateway needs its own independently constrained namespace: controlled
resolver traffic and admitted TCP 443 only, with no production DB/service access.
The current `docker/Dockerfile.extractor` packages only `/extract-service`; a
reviewed gateway release artifact is still required. Do not assume that image
contains `/extract-egress`. Its configured private bind address must be stable and explicit. Do not select
subnets, IPs or routes until the host's current assignments have been checked.
Do not publish CDP, the gateway or the extractor through host ports or Traefik.
Native worker network assertions may be enabled only after actual socket/drop
counter tests pass. Compose interpolation is not proof of isolation.

## Compatibility blocker before network changes

The deployed legacy `core/browser_pool.py` creates ordinary tabs with
`browser.get(url, new_tab=True)`. It does not configure the per-context proxy
used by Go's `Target.createBrowserContext`. Restricting this shared browser now
would break those Python tabs. A global proxy argument alone is insufficient:
the gateway requires proxy authentication, while request interception and
ownership must remain bounded.

Article extraction, Telegram and Page Monitor use the Python pool. Before
activation, accept the opt-in Python owned-context/proxy path for every
remaining browser caller, preserving parsing, cookies where required, cancellation
and cleanup. The benchmark's `_disposable_context_tab` demonstrates a tested
mechanism; it must not be imported into production wholesale. Check supported
URL schemes explicitly: the current egress gateway accepts CONNECT to HTTPS
443 only. Do not silently reroute HTTP-only monitored sources or turn policy
failures into empty successes.

Python's semaphore is process-local; Go concurrency one does not create a shared
browser-wide limit. Validate aggregate load and ensure other clients cannot
close Go targets. Do not invent a global scheduler or increase memory limits
without evidence. The Python adapter is now implemented locally, disabled by default, and checked
with unit and real-nodriver loopback tests. The optional application overlay is
`compose.python-proxy.yml`. Real shared-Cloak compatibility and Linux boundary
bootstrap remain unaccepted; no production application or network was changed.

## Validation and future activation order

1. Complete compatible Python context/proxy handling and enumerate other CDP
   callers. Verify all affected paths on isolated fixtures, including cancel,
   owned crash, auth failures and next-job success with one shared browser.
2. Materialize the Linux namespace policy using inspected addresses and immutable
   release artifacts. Verify direct TCP/UDP/DNS/private/loopback denial and allowed
   authenticated CDP/proxy paths. Inspect both IPv4 and IPv6; include browser UID,
   capabilities and namespace identity checks. Mac Compose rendering is not this
   Linux acceptance.
3. The 2026-10-22 explicit Go-only recovery profile passed controlled page crash
   and fatal worker exit/restart with the same browser generation. Evidence:
   `artifacts/go-worker-only-2026-10-22/result.json`. This does not accept Python
   shared-browser compatibility or production sizing; complete those separately.
4. Only with separate deployment approval, drain browser users and recreate the
   single shared browser with the accepted boundary. A browser recreation causes
   a CDP generation change; reconnect clients and verify their real paths before
   enabling Go routing. Starting a native standby also creates probe contexts and
   requires approval, even while application routing stays `python`.
5. Apply the native standby overlay after the existing standby overlay. Verify
   auth denial, readiness, build identity and controlled extraction. Applying the
   `compose.native-canary.yml` application overlay later opts compatible article
   calls into native Go mode with Python fallback disabled. The existing
   `compose.canary.yml` stays HTTP-only. Neither is percentage routing.

## Rollback is two separate operations

The existing `compose.rollback.yml` restores Python routing only. It cannot undo
a shared browser network change. Before any future activation retain the exact
previous infra Compose/image, network assignments and browser healthcheck mounts,
plus the application image/config. A separately authorized boundary rollback
must drain affected users, restore that browser configuration, reconnect both
clients and verify article/Telegram/Page Monitor paths. No DB restore, replay or
resend is part of routing or browser rollback. Existing durable failed-article
retry state remains intact.

## Current deliverable

`compose.native-standby.yml` is a worker-only configuration draft: native mode,
one admission slot, inherited private service token/security/resource settings,
explicit browser/proxy parameters and 55-second termination grace. It provisions
neither gateway nor browser isolation. Release publication, integration,
capacity acceptance and deployment remain open; no production-readiness claim
follows from a successful Compose render.


## Python adapter implementation evidence — 2026-10-22

The approved local adapter is implemented in `core/browser_proxy.py` and the
shared pool with validated opt-in settings. It installs request/auth handlers
before navigation, checks CDP navigation errors, and disposes only its owned
context. It caps active callback tasks at 32 and authenticated request IDs at
128. Proxy operations do not invalidate the shared browser or request its
restart through the existing marker mechanism. Unknown context allocation or
failed owned cleanup conservatively blocks new proxy allocations in this
application process; known successful cleanup allows a following job.

Root independently ran the affected browser, config, extraction, resource,
cleanup and Telegram suites: 137 passed, 10 warnings. Seven tests use installed
nodriver over loopback WebSocket CDP, covering initial/deferred navigation,
proxy auth, publisher-auth rejection, errorText rejection, actual Page Monitor
connection results and a following job with a fresh context. No browser engine
or publisher was contacted. Compile and whitespace checks passed; the optional
Compose overlay rendered with dummy inputs and preserved Python routing.
Evidence and file hashes: `artifacts/python-proxy-context-2026-10-22/checks.json`.
This does not close the real Cloak, Linux boundary or capacity gates above.


### Shared native Linux gate — 2026-10-22

The approved opt-in worker-recovery profile now mounts the current Python
config/pool/proxy files read-only and checks production browser_tab navigation,
proxy/publisher auth separation, exact context-baseline cleanup and real browser
discovery generation before running the Go cases. Local harness verification:
207 passed, 22 subtests. The versioned payload includes 73 source files and the
same previously verified Linux binaries; no new image was pulled or published.

The single strict VPS run passed 14 raw namespace-denial probes with zero trap
connections, then stopped during Cloak startup after 26.2 seconds on host memory
PSI full avg10=0.55. Neither the Python shared-browser checks nor worker recovery
was reached. Last MemAvailable was 4,370,767,872 bytes; sampled partial runtime
peak was 183,533,568 bytes. These numbers are not a production sizing result or
proof of pressure attribution. There was no guard bypass or automatic retry.

Independent production fingerprints/health, memory max/OOM deltas0 and home/feed
HTTP200 passed. All exact-owned test resources and the stage were removed.
Read-only post-run container pressure/events were collected; historical counters
cannot establish the cause of this run. Linux production isolation, actual shared
Python/Go compatibility and fatal recovery remain open.
Evidence: `artifacts/shared-native-acceptance-2026-10-22/result.json`,
`pressure-after.json`, `root-postflight-verification.json`.
