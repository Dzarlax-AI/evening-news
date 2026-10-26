# Article processing v1 golden corpus

These fixtures freeze the repository's active Python article-processing behavior for Go parity.
All articles, provider results, URLs, timestamps, and extracted text are synthetic. No network,
browser, database, or real provider is used.

Each JSON file has `version`, `case`, `article`, `scenario`, and `expected` members. `scenario.pipeline`
is one of `analyze` (the active `process_article_outcome` path with the source-summary stage stubbed),
`summary_by_source_type` (the active source-type method with fake AI/filter dependencies), or
`force_refresh` (the active forced extraction path with fake extractor/AI dependencies). `expected`
records the typed outcome, persistence translation when applicable, call order, and relevant stats.
The Go parity tests load these files directly. `test_article_processing_golden.py` validates schema,
synthetic-only constraints, and coverage after the Python implementation is removed.

Reference outputs were captured from the Python runtime in `/private/tmp/news-ingestion-venv-20261024/bin/python`.
