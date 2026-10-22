# Persistent native extraction runtime (review artifact)

This directory contains a root-owned Linux supervisor, default-deny role
policies, a systemd unit, and a deliberately invalid production configuration
template. Nothing here starts containers until an operator installs the files,
fills a root-only config with verified production values, and explicitly enables
the unit. The repository template is not a deployment configuration.

The supervisor creates three labelled network-namespace anchors and installs
read-back-checked nftables policies before it starts any service. The browser
namespace has no public route; the unprivileged relay is the sole listener on
port 9222 and forwards only exact configured client IPv4 addresses to Chrome's
loopback port 5100. Gateway and worker namespaces get an explicit root-owned
`resolv.conf` containing only the configured public IPv4 resolver; only that
resolver's UDP/TCP port 53 is allowed. The gateway may open public HTTPS, while
the worker may open public HTTP/HTTPS. Their private peers and return traffic
are explicit; all other traffic is dropped. The shared browser remains one
root-owned Cloak process using the existing 768 MiB, 1 GiB shared-memory, 0.5 CPU
and 256-PID limits.

The supervisor finds each active application-side CDP client by one exact
Docker label/value pair, then checks its immutable image digest, running state,
and assigned private control IP. Configure every active real client (maximum
eight), including Readeck only if current inspection confirms it uses this
browser. Every active client must have an explicit `cdp_proxy_accepted: true`
decision before production mode validates. The `worker` entry describes its
runtime-owned static control IP; it is created by this supervisor. Never use a
CIDR or wildcard as a client identity.

`personal_assistant-agent-1` must be recorded in `disabled_clients` with its
exact stopped container ID, image and label identity. Preflight and every
supervisor iteration require that exact container to remain stopped with
restart policy `no`. The supervisor does not stop, start, or modify the
Personal Assistant. Its exclusion prevents it from becoming a default-browser
CDP controller again. Since this inventory record is never launched or pulled,
its `image` may be the exact local `sha256:<image-id>` reported by Docker; the
active production clients and runtime images still require registry digest
references. The template's placeholder ID must be replaced with the current
exact values before production validation can pass.

## Staging and production gates

`supervise-test` and `stop-owned-test` accept only root-owned mode
`isolated-test` configs. Run them only on a disposable Linux Docker host using
fresh, non-overlapping role subnets, an independently built app stub with the
configured exact label and image, and pinned images. The app stub must be able
to perform the same admitted browser, gateway, and worker readiness probes. The
test mode skips the legacy production browser check; it does not weaken the
production `supervise` command. Verify DNS from each service namespace and test
denial of direct browser HTTP/HTTPS/DNS/UDP, private and metadata destinations,
unlisted CDP clients, and replacement/stopped role containers before proposing
production activation. An isolated candidate may use Docker's immutable
`sha256:<image-id>` references for images loaded from a saved CI artifact; the
supervisor checks the resolved `Image` ID exactly. Production mode rejects
these local-only references and still requires registry `name@sha256:...`
references.

Production validation refuses to run until the exact existing shared-browser
container ID and disabled Personal Assistant container ID are stopped with
restart policy `no`, all active application-side client images and control IPs
are inventoried, and each active caller's proxy-auth compatibility is
accepted. It never starts or manages the legacy browser or Personal Assistant.
The canonical Compose handoff must keep the application on its existing infra
and Traefik networks and add only the verified private control network. This
runtime does not edit Compose, `.env`, DNS, or any unrelated service.

Install the reviewed code under `/opt/news-extractor-runtime`; place a completed
root-owned config at `/etc/news-extractor-runtime.json` with mode `0600`. Keep
`gateway.env` and `worker.env` at mode `0600`, containing only their dedicated
tokens. Make the runtime scripts, single-resolver `resolv.conf`, and browser-info
directory root-owned and not group/world writable. Copy the systemd unit to
`/etc/systemd/system/news-extractor-runtime.service`. Inspect `plan --config` in
a controlled environment before authorizing service enable/start; the plan
prints paths and immutable image references, never env-file contents.

On shutdown or failed startup, `ExecStopPost` stops only containers with this
instance's exact owner/role labels; stopped anchors remain for safe ownership
reconciliation and are removed/recreated on the next start. A worker or gateway
process exit restarts that exact stopped role. Cloak/relay exit or an authorized
restart marker causes a bounded browser-generation restart, preserving the
gateway. Anchor, client identity, route, policy, or DNS-config drift fails
closed and stops the owned children so systemd retries from checked state. No
database/Redis/Traefik operation or app-container recreation is part of this
service. The unit is ordered after and bound to `docker.service`; `PartOf=`
propagates Docker stop/restart jobs to the runtime, while the `docker.service`
`WantedBy=` link pulls it back in when Docker starts again. `Restart=on-failure`
then retries supervisor startup after an unexpected runtime failure.

Do not activate this artifact on production yet. It still needs the isolated
Linux acceptance above, current client-inventory and proxy-compatibility
evidence, and a separately reviewed canonical Compose handoff. In particular,
the actual browser/role DNS behavior and Chrome proxy authentication must pass
on the target pinned images before the deployment is considered ready.
