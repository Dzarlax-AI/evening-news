# Isolated Go worker and ingestion gates

The Go job worker gate exercises the durable News queue and private Python executor:

```sh
python3 scripts/run_go_job_worker_fixture_gate.py \
  --image evening-news-web-app:components-20261026 \
  --web-image evening-news-web:components-20261026 \
  --worker-image evening-news-job-worker:web-components-20261026 \
  --processing-image evening-news-processing:web-components-20261026 \
  --output-dir artifacts/go-job-worker
```

The ingestion gate runs Go article ingestion with the Go collector and synthetic
RSS, Telegram, and Page Monitor responses:

```sh
python3 scripts/run_go_ingestion_fixture_gate.py \
  --app-image evening-news-web-app:components-20261026 \
  --web-image evening-news-web:components-20261026 \
  --worker-image evening-news-job-worker:web-components-20261026 \
  --collector-image evening-news-collector:web-components-20261026 \
  --ingestion-image evening-news-ingestion:web-components-20261026 \
  --processing-image evening-news-processing:web-components-20261026 \
  --output-dir artifacts/go-ingestion-fixture
```

Both gates use disposable PostgreSQL and fake provider/receiver endpoints. The
Go job-worker gate uses a deterministic ingestion receipt stub and a pre-seeded
synthetic article to exercise Go processing, its private Python AI callback, and executor behavior; the Go ingestion gate tests
real Go collection and persistence. They do not publish ports or use real sites,
credentials, AI providers, Telegram sends, or production services. The shared `run_background_worker_fixture_gate.py`
module still provides Docker setup and cleanup helpers for these gates; its
older Python-public-web implementation is no longer run. Its CLI now forwards
to the Go web and Go job-worker acceptance gate; pass the same app, web,
worker, and processing image tags shown above.
