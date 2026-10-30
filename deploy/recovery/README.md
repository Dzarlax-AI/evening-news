# Operator assets

Follow [managed settings recovery](../../docs/managed-settings-recovery.md). These SQL recipes are **not** a full database backup or a replacement for infrastructure recovery.

- `export-settings.sql`: one JSON document containing explicitly selected configuration.
- `export-schedules.sql`: logical scheduler definitions, without outbox, leases or due instants.
- `import-settings.sql`: transactional import into a new, migrated `news_restore_*` database with no runtime clients. It requires `target_database` and `installation_id`, reads the authenticated `settings.json` and compact `background-input.json` derived from `background.json` in its private working directory, refuses populated targets and leaves maintenance on and delivery off.

Use the exact matching application revision, protected PostgreSQL service/pass files, and `psql -XqAt -v ON_ERROR_STOP=1`. Never pass passwords as command arguments or print the resulting JSON. Schema creation, role grants, network mapping, encrypted packaging and explicit activation are operator steps in the guide. Existing `newsctl backup/restore/resume` guards remain unchanged.
