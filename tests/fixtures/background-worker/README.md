# Isolated Go worker and ingestion gates

The Go job worker gate exercises the durable News queue and private Python executor:

```sh
python3 scripts/run_go_job_worker_fixture_gate.py \
  --image evening-news-app:test \
  --worker-image evening-news-job-worker:test \
  --output-dir artifacts/go-job-worker
```

The ingestion gate runs Go article ingestion with the Go collector and synthetic
RSS, Telegram, and Page Monitor responses:

```sh
python3 scripts/run_go_ingestion_fixture_gate.py \
  --app-image evening-news-app:test \
  --worker-image evening-news-job-worker:test \
  --collector-image evening-news-collector:test \
  --ingestion-image evening-news-ingestion:test \
  --output-dir artifacts/go-ingestion-fixture
```

Both gates use disposable PostgreSQL and fake provider/receiver endpoints. The
Go job-worker gate uses a deterministic ingestion receipt stub and a pre-seeded
synthetic article to exercise executor/AI behavior; the Go ingestion gate tests
real Go collection and persistence. They do not publish ports or use real sites,
credentials, AI providers, Telegram sends, or production services. The shared `run_background_worker_fixture_gate.py`
module still provides Docker setup and cleanup helpers for these gates; its
retired Python worker CLI is not a supported test command.
