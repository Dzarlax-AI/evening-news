# Background worker

The existing Go background process owns durable queue claims, worker lease and recovery, scheduling, ingestion and processing. It calls the private Go domain for operations requiring domain logic or providers. Private tokens and independently scoped database roles separate these responsibilities without adding a steady-state process or container.

A worker uses job, attempt and domain fences for mutations. External calls run outside database transactions. A delivery that may have reached its provider is recorded as uncertain and is not automatically replayed. Shutdown stops admission and drains work; loss of the singleton guard makes the domain unavailable.

Run `go test -race ./...` and `go vet ./...` in `go-worker`, `go-background`, `go-execution` and `go-scheduler`. PostgreSQL integration tests need an isolated loopback `TEST_DATABASE_URL` or `SCHEDULER_TEST_DATABASE_URL`. The consolidated image gate runs synthetic jobs, source receipts, shutdown, lease loss, uncertain delivery and backup/restore against fake providers; see [Core image build and release](core-image-build-and-release.md). No test result implies a production rollout.
