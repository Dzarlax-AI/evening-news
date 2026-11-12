# Digest push verification

This change adds managed-installation APNs delivery with the readiness and sender
gates disabled by default. No live APNs transport or device receipt has been
verified by these local checks.

## Local checks

- Full `go-domain` race suite (`-count=1 -p=1 ./...`), `go vet ./...` and
  `go mod verify` passed with an isolated loopback PostgreSQL 17 fixture.
- Full `go-web` and `go-tools` race suites, vet and dependency verification passed.
- After the private canary was separated from ordinary readiness cutover, the
  complete `apnspush` race suite and vet passed again.
- Configuration catalog validation passed after refreshing source references.
- Independent focused review addressed the public safe-409 boundary, challenge
  attempt accounting, in-flight ownership proof, unknown-challenge resend,
  fixed expiration, token quota, encryption-version binding and lock ordering.

PostgreSQL tests cover ownership/capability rejection and durable quotas,
exact expiry, locale/token rotation, per-version encryption, bounded cursor
fanout, shared-token deduplication, 200/400/410/429/5xx and timeout outcomes,
retry exhaustion, expired sending leases, late old-version results, stale work,
private canary idempotency, retention, readiness rollback and RU/SR independence.
The web tests exercise the real private client through the public handler,
including strict safe-409 preservation and unauthorized state hiding.

## Release boundaries

PostgreSQL 16/17 CI and image acceptance are separate from these local checks.
Apply migration 021 and matching domain grants before starting the new binary.
Configure the encrypted APNs provider and verify the signed client's actual
environment before one controlled canary. Keep ordinary readiness cutover off
during that check. HTTP 200 is provider acceptance; user-device display and
exact-issue navigation require their own evidence.

The existing Telegram/Viber behavior, browser/egress/extractor configuration,
private manifests and unrelated application work are outside this patch.
