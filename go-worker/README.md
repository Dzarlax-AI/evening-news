# Go job worker

The Go worker is the sole durable-queue owner. It holds the shared PostgreSQL
worker lease while claiming and finishing jobs; the Python executor only
accepts fixed job attempts and runs existing News operations under that lease.

## Supported executor topology

Run exactly one Python executor process per database schema. The executor
holds a dedicated PostgreSQL session advisory lock, fails startup if another
instance owns it, and stops accepting work if that lock connection is lost.
`JOB_EXECUTOR_URL` must resolve directly to this singleton. Do not put a load
balancer in front of multiple executor instances or run overlapping rolling
replacements. Replace/restart the process in place so a changed instance ID
means the previous process has exited before Go recovers its receipts.

The singleton guard uses a separate advisory key from the Go worker lease and
the selected-retry coordination lock. It does not claim jobs or write worker
status. PostgreSQL remains the source of truth for both lock owners.

## Local validation

Build the image from the repository root so the Dockerfile can copy the Go
module paths:

```sh
docker build -f go-worker/Dockerfile -t evening-news-job-worker:local .
```

```sh
go test ./...
go test -race ./...
go vet ./...
```

The PostgreSQL store tests run only when `TEST_DATABASE_URL` points to a
loopback PostgreSQL instance. They create and remove a uniquely named schema.
