# Digest push notifications

Managed installations can publish opt-in digest availability notifications for
`ru` and `sr-Latn`. The server owns the `com.dzarlax.NewsFeed` APNs topic.
Sandbox and production routing are separate. Existing editions are not scanned
or backfilled when the feature is enabled.

## Installation API

All public operations require HTTPS in public deployments and a device-only
32-byte random capability, encoded as 43-character unpadded base64url, in
`Authorization: Bearer …`. The client persists its random UUID and capability
before its first request. There is no installation enumeration endpoint.

| Method and path | Strict JSON body | Success |
| --- | --- | --- |
| `POST /api/public/push/installations` | `installation_id`, `device_token`, `locale`, `environment` | 202 pending; 200 unchanged registration |
| `PUT /api/public/push/installations/{id}` | `expected_version`, `device_token`, `locale`, `environment` | 200 current state |
| `POST /api/public/push/installations/{id}/activate` | `subscription_version`, `nonce` | 200 active |
| `DELETE /api/public/push/installations/{id}` | Empty body | 204 |

Tokens use lowercase hexadecimal and may contain up to 512 decoded bytes.
Locales are `ru` and `sr-Latn`; environments are `sandbox` and `production`.
Requests reject unknown, duplicate and null fields and oversized bodies.
Responses expose only `installation_id`, `state`, `subscription_version` and,
for pending verification, the actual `expires_at`. An authenticated 409 includes
that safe state for reconciliation. Invalid capabilities do not expose state.

A registration becomes active only after the app returns the nonce sent to its
claimed token through an APNs background challenge. The app must match its own
pending installation and expected version. A valid issued nonce is proof even
if the provider acknowledgement is still in flight or was lost. Verification
expires after ten minutes. APNs acceptance alone never activates ownership.

Token or environment changes require another challenge. An active locale-only
change retains ownership, increments the subscription version, rebinds token
encryption to that version, and cancels unsent work for the old version. Disabling
erases routing data and cancels unsent work. In-flight requests cannot be recalled.
Durable IP, installation and token limits also count rejected attempts; stored
address buckets contain keyed hashes rather than raw addresses.

## Events and delivery

Both Russian and Serbian edition persistence insert a readiness event in the
same transaction as the new immutable edition. The first edition for each
date/locale is pinned. Activation, version changes and event insertion share
an ordering lock. Fanout uses a durable cursor and bounded subscriber batches;
only a matching active subscription version that predates readiness is eligible.

Normal delivery is deduplicated by installation/date/locale and by token,
environment/date/locale. A delivery has a fixed expiry derived from readiness;
retries do not extend its lifetime. Work is marked `sending` with a lease before
external I/O. Explicit 429/5xx responses have bounded retries and a stable APNs
request ID. A lost response or expired sending lease becomes `unknown` and is
not blindly replayed. HTTP 200 means `accepted`, not proof that a device displayed
the notification. A late response cannot invalidate a rotated token.

The existing guarded domain process owns the bounded delivery loop. APNs I/O
runs after commit; provider failures do not roll back editions or Telegram.
Provider JWTs and HTTP/2 connections are reused for their credential identity.
Apple endpoints are fixed; there is no configurable production destination URL.

## Configuration and acceptance

Migration 021 creates domain-only tables and extends the encrypted credential
provider allowlist with `apns`. Apply the matching managed role grants before
starting the new domain binary. The web role receives neither push-table access
nor the credential encryption/signing key.

The authenticated credential API accepts provider `apns`, with a strict JSON
credential containing `key_id`, `team_id` and the P-256 PKCS#8 `private_key`.
Credential metadata reads never return that value. The credential is stored
through the existing authenticated encryption mechanism, without environment
fallback. Routing tokens and challenge nonces use separate installation/version
bindings under the managed encryption key.

`push_cutover.enabled` and `push_cutover.sender_enabled` both default to false.
The first controls creation of ordinary readiness events; the second controls
external sending. Configure credentials and verify signed client entitlement and
environment before enabling a controlled canary. With readiness still disabled,
the administrator-only `POST /api/v1/push/canary` accepts one active installation,
one exact existing edition and a UUID `idempotency_key`. Its private domain route
is `/internal/web/v1/push/canary`. It creates no readiness event and no historical
fanout. Repeating the same key observes the same request and does not requeue it.

After checking one real device notification and exact-issue navigation, enable
ordinary readiness delivery. Simulator injection only checks routing. For rollback,
disable the sender and preserve subscriptions, outbox rows and outcome receipts.
No App Store submission is part of this backend workflow.

## Retention

- Pending proof data expires after ten minutes, including unknown challenge outcomes.
- Inactive subscriptions are disabled and their routing data erased after 180 days.
- Disabled tombstones and terminal delivery/deduplication receipts are retained for 30 days.
- Address rate buckets expire within two hours.

All cleanup passes are bounded. Unknown outcome records keep their outcome while
expired encrypted proof material is erased.

## Validation

Tests use isolated PostgreSQL and fake APNs. Coverage includes strict public/private
forwarding, safe 409 reconciliation, ownership/version/nonce limits, transaction
rollback, RU/SR independence, no backfill, cursor fanout, shared-token deduplication,
rotation, 200/400/410/429/5xx/timeout outcomes, retry exhaustion, expired leases,
stale work, private canary and retention. Real device receipt is a separate release
acceptance check.

Apple references: [registration](https://developer.apple.com/documentation/usernotifications/registering-your-app-with-apns),
[provider authentication](https://developer.apple.com/documentation/usernotifications/establishing-a-token-based-connection-to-apns),
[sending requests](https://developer.apple.com/documentation/usernotifications/sending-notification-requests-to-apns).
