# Private Go domain runtime

The development candidate runs `news-domain` as PID 1 in the existing private
executor container. It provides the authenticated fixed domain operations,
article AI and selector callbacks, terminal events, and the private web
backend. The Go background execution component retains job orchestration and
its own execution identity; calls and database writes remain fenced to the
current worker, attempt, execution and domain ownership. The web process uses
its separate private backend token.

The domain process calls analysis, editorial, media, learning, backup and
notification components directly. It does not start a Python process or a Go
child and has no Unix-socket bridge. Provider calls and backup subprocesses do
not hold database transactions. The singleton guard occupies one pooled
connection, and the pool reserves one connection from the configured total for
`pg_dump`. Runtime startup does not apply DDL; `news-migrate` is the explicit
one-shot migration command.

Authenticated `/internal/v1/status` reports `domain_instance_id`,
`active_operations`, and `ready`. It returns unavailable when the singleton
guard, database, direct runtime, or drain state is not ready. On guard loss or
shutdown, the process stops admitting requests, cancels and drains active work,
then releases resources. See [Go digest delivery](go-digest-delivery.md) for
delivery receipts and ambiguous outcomes.

This describes an unmerged development candidate. The full Linux image,
PostgreSQL 16/17 backup/restore, fault and coverage gates are still in progress;
see [Go tooling coverage](../go-tools/COVERAGE.md). Production remains a
separate, explicitly authorized rollout.
