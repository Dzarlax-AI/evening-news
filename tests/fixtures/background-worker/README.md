# Background worker Docker gate

Run `python3 scripts/run_background_worker_fixture_gate.py --image evening-news-background-worker:test-20261023 --output-dir artifacts/background-worker-gate` after building the local app image from the current checkout.

The gate creates uniquely named and labeled Docker resources on an internal-only network. It uses a new disposable PostgreSQL container, a fake Go/Gemini/Telegram endpoint, and separate web and worker containers. It does not publish ports or mount the repository, `.env`, or application data into the app containers. The only bind mount is this fixture directory, read-only. Cleanup removes only resources whose names and ownership labels were created by this run.

The fake collection endpoint implements the Go SourceManager v1 wire contract; it does not run the Go service or prove browser/site throughput. The fake Gemini endpoint accepts the configured API request shape and returns deterministic structured output. A test-only bootstrap routes TelegramService to the fake receiver in both roles and makes web-side NewsOrchestrator and AIClient construction fail. No real sites, browsers, provider credentials, Telegram sends, or production services are used. Containers have bounded memory, CPU, and process limits, and the report explicitly marks that no Go runtime, browser, or throughput claim was tested.
