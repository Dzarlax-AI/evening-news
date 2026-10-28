# Local quick start

The public web is served by `go-web`. The development candidate uses the private Go domain executor and an explicit one-shot Go migration command. Its full image acceptance remains in progress.

Follow the [README local development instructions](../README.md#local-development) for the complete required environment and local service stack:

```bash
docker compose -f docker-compose.example.yml -f docker-compose.dev.yml up --build
```

Compose runs `python -m news_aggregator.migrate` once before schema consumers start. The web service exposes port 8000. The private executor listens on port 8100 inside the service network and requires its service credentials; do not expose it publicly.

For the component boundaries, private backend configuration, and local verification commands, see [Web/API components](web-api-components.md). This local setup does not deploy production or configure browser rendering.
