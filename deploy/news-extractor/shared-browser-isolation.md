# Shared Cloak boundary under Docker Compose

The user selected one shared Cloak and then explicitly requested removal of the
host-supervisor design. The current artifacts are `compose.shared-browser.yml`,
`compose.native.yml`, `browser/`, and `docker/Dockerfile.browser`. The deployment
procedure is in `HANDOFF.md`; production acceptance is recorded separately.

The application and Go worker use a restricted relay on browser port 9222.
Chrome itself listens on loopback 5100. Only the relay UID can initiate that
loopback connection. Browser page traffic can reach only the exact authenticated
CONNECT proxy address. Browser nft rules deny direct DNS, public/private TCP,
UDP and IPv6; an internal Docker network alone is not the claimed boundary.

The container applies and reads back the policy before starting either child.
It drops NET_ADMIN from the runtime parent's bounding set; browser and relay
children have empty effective/permitted/bounding capability sets. No Docker
socket or host firewall access is mounted. Docker's trusted healthcheck checks
the installed policy separately. Browser child exit, restart marker or FD limit
stops the container; Docker restarts it through the same policy-first bootstrap.

Worker and proxy use private control plus ordinary outbound Compose networks;
they are not attached to the production infra/DB network. Their existing Go
URL validation, all-answer DNS admission and numeric-IP dial protection remain
in force. There is no new kernel isolation claim for these two Go services.

Python's opt-in proxy adapter owns its own browser contexts and preserves
Telegram/Page Monitor. Native article routing does not immediately fall back to
Python; existing durable retries remain. Publisher authentication and proxy
authentication remain separate and must not leak credentials upstream.

Fresh read-only inventory on 2026-10-22 found News as a CDP caller. Readeck,
its bot and MCP had no browser endpoint env, mounted-config CDP references or
established port-9222 connection at the snapshot. Toolkit source had no CDP
references. This is bounded evidence, not a universal absence claim. Their
networks/configuration are preserved and Readeck API/article reading is checked
before and after deployment. Assistant remains stopped with restart disabled.

The old namespace-anchor/source acceptance remains historical evidence for Go
and Python behavior. It does not accept the new container startup/lifecycle.
Use the final CI images in the isolated Compose acceptance before cutover.
