# Go article ingestion

The ingestion component runs inside the existing Go background process. It accepts authenticated private requests, reads configured sources, calls the extractor and persists fenced run and source receipts. Page Monitor state is acknowledged with article persistence. It does not hold a database transaction across an external HTTP call.

Its runtime role is provisioned by `deploy/news-ingestion/provision-runtime-role.sql`. Keep its database URL and private token separate from the worker, processing and web credentials. The domain handles selector analysis through the private callback with the current job and attempt fence.

The consolidated synthetic image gate uses four RSS sources, one Telegram source and three Page Monitor sources on an isolated network. It exercises the real background, domain, web, collector and PostgreSQL roles with fake providers. Unit and integration tests live in `go-ingestion` and `go-domain/selectors`; PostgreSQL tests require an isolated loopback test database. See [Core image build and release](core-image-build-and-release.md) for CI and acceptance status.
