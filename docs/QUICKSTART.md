# Local quick start

The default installation uses the managed profile. Run these commands from the repository root:

```sh
cp .env.example .env
make managed-up
make managed-token
```

Open `http://localhost:8000/setup`, claim the first administrator with the one-time token, and configure AI, Telegram, sources and schedules in the interface. Databases and private service configuration are provisioned automatically. See the [README quick start](../README.md#quick-start) and [configuration guide](configuration.md) for HTTPS, browser attachment and recovery requirements.

## Manual development profile

For a deliberately hand-configured local stack, follow the database/role preparation in the [local deployment guide](../deploy/news/README.md), using `deploy/config/development.env.example`. The complete advanced name inventory is `deploy/config/full.env.example`. These are separate from the minimal managed `.env`.

```sh
docker compose --project-directory . -f deploy/compose/development.yml -f deploy/compose/development.override.yml up --build
```

The explicit project directory preserves build contexts and bind mounts at the repository root. Run the command there; if invoking from another directory, use absolute paths for the project directory, Compose files and env input. This command starts local services and is not a production deployment.
