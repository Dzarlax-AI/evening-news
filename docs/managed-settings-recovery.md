# Managed settings backup and recovery

This is an **operator procedure** for an adopted managed installation using native Cloak and an external PostgreSQL server. It restores a configured installation with an empty article history. It does not recreate the shared browser or restore an entire PostgreSQL cluster. The current `newsctl backup`, `restore` and `resume` commands do not support this profile; do not bypass their guards.

A usable copy combines current configuration records, encrypted credentials, matching private keys and role files, deployment metadata, and logical schedule definitions. A web API database export alone is insufficient. Recovery is performed offline; the managed admin page provides guidance rather than upload/restore controls.

## What is and is not recovered

The SQL allowlist in [export-settings.sql](../deploy/recovery/export-settings.sql) is authoritative:

- Sources: IDs, name/type/URL, enabled flag, interval and supported configuration fields. Collection offsets/hashes/acknowledgements, last fetch/success/error and error counts are reset. `source_collection_state` is not copied.
- Categories and category mappings; extraction patterns explicitly marked `manual` or `user`, including their success/failure counters needed by the existing eligibility/rate filter. Other automatic learning caches/statistics are disposable.
- AI provider/model settings, operational settings and Telegram destination settings, with their revisions. Job-specific setting snapshots and unrelated keys are excluded.
- Installation identity, administrator hash/revision, integration credential envelopes (including null-envelope deleted-key revisions), and the matching domain encryption key/key ID. One-time setup tokens are never restored.
- Logical schedules, including calendars/timezones, keys, operations, targets and payloads. Live dispatch/outbox/occurrences, old due instants and leases are excluded.
- Exact image digests/source revision, schema revision, current Compose, role/grant/search-path metadata, private role manifests and the adopted setup completion marker.

Articles, editions, prior Telegram delivery receipts, jobs, execution runs, logs, caches, PostgreSQL data directories and browser cookies/profile are not included. RSS/channel history may no longer be available. The aim is to resume collection, not reproduce every past article. Previously sent Telegram messages remain outside this archive; delivery therefore stays disabled until an operator deliberately enables it.

## Preconditions and trust boundary

Use the exact reviewed application revision that produced the copy. The recipe currently supports schema revision 016 with schema `news`; the scheduler uses `scheduler_service`. An upgrade is a separate step after recovery acceptance.

Tools: Bash, Docker Compose v2 or newer, `psql` matching the server major (16 or 17), `jq`, SHA-256 tooling, tar and [age](https://github.com/FiloSottile/age). Verify tool versions and available space before maintenance. Do not install tools or restart infrastructure as an implicit part of backup. The recipe has no dependency on a new always-running service.

Keep the age private recovery identity **off the VPS**, separately from the archive. The backup host needs only the public recipient. Protected PostgreSQL service/pass files supply database credentials; never put passwords/DSNs in command arguments, documentation, terminal output or Git.

The operator must inventory and confirm:

1. Compose project, stack directory and the exact service/container names. Examples below use services `app`, `executor`, `background`, `extractor`, `egress` and containers `news_web`, `news_executor`, `news_background`, `news-extractor-1`, `news-egress-1`; adapt only after inspection.
2. Actual source database/schema and scheduler database/schema. Never infer them from fresh-install defaults. Configure `PGSERVICEFILE` and `PGPASSFILE` (0600) with `news_source`, `scheduler_source` and, later, `news_target` service entries. Use an authorized operator login with the required reads; web/domain runtime roles deliberately cannot read all recovery state.
3. All five active runtime files and the setup volume's `adoption-committed.json`. These files contain secrets. They are not the old `.env` or stale `adoptions/*/import.json` staging files.
4. Exactly the News runtime identities, the schema owner and their privileges/search paths. Do not export cluster-wide globals, superuser passwords or other applications' roles. Target provisioning uses this reviewed scope and the checked-in grant policy; no broadening of shared privileges is part of recovery.
5. Fixed native CDP/egress endpoint/network expectations and dependencies. Reusing an existing browser requires matching credentials; moving to another host requires an independently provided browser endpoint. No News recovery command restarts or replaces that shared dependency.

## Backup window

A backup is a consistent configuration checkpoint, not two unrelated online reads. No automatic scheduled backup or retention job is installed by this procedure.

Record the original control revision/paused flag and which News processes are running. Pause the scheduler with its existing authenticated CAS control API. Use a 0600 curl config for the bearer header, not `curl -H` with a secret in argv. Save responses privately; do not print tokens. Stop only the News web ingress, wait for accepted jobs/callbacks/deliveries to finish, stop background gracefully, then stop the private domain. If a delivery is `unknown`, investigate it; never retry it as part of backup.

Keep PostgreSQL, the browser, Readeck, proxy, extractor and egress running. Do not use project-wide `down`, stop the shared database, or force-kill a stuck job to obtain a backup. If drain cannot finish, abort the copy and follow the original-state recovery steps below.

With those three writers stopped, configuration changes and key rotation cannot race the SQL snapshots. Verify there are no remaining runtime database sessions. PostgreSQL snapshots cover an individual database, not a cross-database transaction; quiescence supplies the cross-database consistency boundary.

### Capture a private staging directory

Assemble the capture and encryption blocks into **one reviewed Bash script**, not separate commands in a login shell. `NEWS_REPO` is the matching checkout; `NEWS_STACK` is the deployment directory. Define and test a stack-specific `restore_original_availability` function before any quiescence: it must restore exactly the recorded running processes/control state and verify health as described below. The function is required, not an optional empty callback; its commands depend on the inspected Compose project and authenticated CAS controls. Install the exit trap before performing the backup-window steps, then continue with capture/encryption in the same script. Do not exit between these blocks.

```bash
set -euo pipefail
umask 077
# Refuse to proceed unless the reviewed original-state recovery is installed.
declare -F restore_original_availability >/dev/null || { echo 'Missing availability recovery function' >&2; exit 1; }
NEWS_STAGE=''
NEWS_PARTIAL=''
news_backup_exit() {
  NEWS_EXIT_STATUS=$?
  trap - EXIT HUP INT TERM
  set +e
  restore_original_availability
  NEWS_RESTORE_STATUS=$?
  if [ -n "$NEWS_PARTIAL" ]; then rm -f -- "$NEWS_PARTIAL"; fi
  if [ -n "$NEWS_STAGE" ]; then rm -rf -- "$NEWS_STAGE"; fi
  if [ "$NEWS_RESTORE_STATUS" -ne 0 ]; then
    echo 'Original availability recovery failed; follow the recorded recovery procedure' >&2
    exit 1
  fi
  exit "$NEWS_EXIT_STATUS"
}
trap news_backup_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
NEWS_STAGE=$(mktemp -d)
chmod 700 "$NEWS_STAGE"
# Perform the reviewed quiescence/drain steps here, then capture below.

PGSERVICE=news_source psql -XqAt -v ON_ERROR_STOP=1 \
  -f "$NEWS_REPO/deploy/recovery/export-settings.sql" > "$NEWS_STAGE/settings.json" 2>/dev/null || {
  NEWS_SQL_STATUS=$?; echo 'Settings export failed' >&2; exit "$NEWS_SQL_STATUS"
}
PGSERVICE=scheduler_source psql -XqAt -v ON_ERROR_STOP=1 \
  -f "$NEWS_REPO/deploy/recovery/export-schedules.sql" > "$NEWS_STAGE/schedules.json" 2>/dev/null || {
  NEWS_SQL_STATUS=$?; echo 'Schedule export failed' >&2; exit "$NEWS_SQL_STATUS"
}
# Reject missing identity, empty admin, unknown format or oversized output.
test "$(wc -c < "$NEWS_STAGE/settings.json")" -le 16777216
jq -e '.format == "news-settings-v1" and .schema_version == 16 and
  (.identity.installation_id | test("^[a-f0-9]{32}$")) and
  (.managed_admin | length == 1)' "$NEWS_STAGE/settings.json" >/dev/null

docker cp news_web:/run/news/runtime.json "$NEWS_STAGE/web.json"
docker cp news_executor:/run/news/runtime.json "$NEWS_STAGE/domain.json"
docker cp news_background:/run/news/runtime.json "$NEWS_STAGE/background.json"
docker cp news-extractor-1:/run/news/runtime.json "$NEWS_STAGE/extractor.json"
docker cp news-egress-1:/run/news/runtime.json "$NEWS_STAGE/egress.json"
# Preserve the reviewed source Compose as data, not an evaluated command.
jq -n --rawfile compose "$NEWS_STACK/docker-compose.yml" \
  '{compose_yaml: $compose}' > "$NEWS_STAGE/deployment.json"
chmod 600 "$NEWS_STAGE"/*
```

These commands read files from stopped containers; `docker exec` cannot be used after stopping them. Export the current adopted setup completion marker using a read-only mount of the inventoried setup volume and a reviewed pinned utility image, with `--network none`. Name the result `adoption-committed.json`. Copy only that marker, not all historical adoption staging. Store the scoped role/grant/search-path inventory as `roles.json` and the matching operator procedure as `RESTORE.md`.

Check that **every** role manifest, the marker and the SQL identity agree. Verify that the domain file contains the encryption key and key ID used by every credential envelope. Fail if any required role or mapping is missing; do not publish an incomplete copy. Record the exact application revision and every immutable image digest.

### Encrypt and publish

The operator archive is a separate `settings-recovery-v1` format. It is not accepted by `newsctl restore`.

Use these exact flat regular-file names (no directory, symlink, device or hardlink entries):

```text
manifest.json
settings.json
schedules.json
web.json
domain.json
background.json
extractor.json
egress.json
adoption-committed.json
deployment.json
roles.json
RESTORE.md
```

Create `manifest.json` with format `settings-recovery-v1`, the source installation ID, profile `adopted-native-external`, schema revision 16, UTC time, source revision/image digests, and the exact byte size/SHA-256 of every other entry. The manifest itself must not contain the archive's own digest. Include it inside the encrypted payload. Read checksums locally into the manifest; never print the files to create a report. Refuse unknown/missing entries and an unexpected identity before encryption.

```bash
# Use an explicit allowlist, not tar of an entire deployment directory.
COPYFILE_DISABLE=1 tar -C "$NEWS_STAGE" -cf "$NEWS_STAGE/payload.tar" \
  manifest.json settings.json schedules.json web.json domain.json \
  background.json extractor.json egress.json adoption-committed.json \
  deployment.json roles.json RESTORE.md
# NEWS_RECIPIENT is the public age recipient; NEWS_ARCHIVE is a NEW output path.
NEWS_PARTIAL=$(mktemp "${NEWS_ARCHIVE}.partial.XXXXXX")
age -r "$NEWS_RECIPIENT" "$NEWS_STAGE/payload.tar" > "$NEWS_PARTIAL"
# An atomic hard link refuses overwrite. Partial and final must share a filesystem.
ln "$NEWS_PARTIAL" "$NEWS_ARCHIVE"
rm "$NEWS_PARTIAL"
NEWS_PARTIAL=''
# After off-host verification, exit this script; its trap restores availability.
```

On encryption failure, remove the owned partial file and publish nothing. Capture the encrypted file checksum, copy it to the approved off-host destination, and compare that checksum. Keep the current verified copy and one previous copy; do not prune the previous copy until a new restoration test succeeds. Decryption and restore validation, not just a checksum or tar listing, establish recoverability. Do not promise secure erasure of staging on SSD; use protected/encrypted temporary storage and remove the owned files afterward.

### Restore original availability after backup

On success **or failure**, start only the News processes that were running originally. Verify web/domain/background health and the authenticated admin/API path. Restore the prior paused state using the current CAS revision; if originally paused, leave it paused. Do not reset schedules, replay deliveries or turn on a disabled delivery flag. Remove staging and report whether the archive was published/verified. A backup error must not silently leave the application stopped.

## Restore into a clean target

Restoration requires separate authorization for the target. Keep the original installation and its data untouched. On a shared PostgreSQL server, create **new** News/scheduler databases; never run `--clean`, cluster restore, or `TRUNCATE CASCADE` against an existing application database.

### Authenticate before touching the target

Decrypt the complete age stream into a new private file and require success before tar extraction. Apply an encrypted-input size budget (64 MiB for this settings format) and an unpacked budget (32 MiB, 16 MiB per entry). Verify the exact flat entry-name set above, regular-file types, no duplicates, safe paths and entry sizes before extracting into an empty private directory. A tar listing alone is not the integrity check: after extraction verify every manifest size/hash, profile/version, identity, image/schema compatibility and required private role files. Never extract directly into `/root/news`, a DB volume or a runtime mount. A malformed or unsupported bundle is not partially restored.

Use only a trusted operator-produced archive. This is a reviewed manual procedure, not an endpoint for accepting arbitrary uploaded archives. The current application's encrypted recovery parser deliberately rejects this different format.

### Prepare schemas and scoped roles

Provision empty databases using the reviewed infrastructure procedure, with the exact News-only role/grant/search-path mapping. A new News target name must match `news_restore_[a-z0-9_]+`; the import recipe additionally compares it to the explicitly supplied target name. Provisioning does not change or reuse a superuser identity for a runtime. Conflicting existing role credentials or access are a stop condition, not permission to alter neighboring identities.

Create schema `news` using the one-shot `news-migrate` from the matching pinned release; create the scheduler schema with the matching scheduler migrations, initially paused. A fresh SQL schema is required, **not** an already claimed managed installation. Do not run `setup prepare`, `provision` or `adopt` over recovered role files: they can generate or validate a different identity. The procedure restores the original installation ID/key pair instead.

No web/domain/background clients may connect during import. Confirm the new target has no articles, jobs, sources, administrator, credentials or setup tokens. Bootstrap category/settings seeds may be replaced only inside that new target. Preserve a control sentinel in another schema during a rehearsal to verify scope.

```bash
# NEWS_CLEAR is the verified empty-private-directory extraction from the previous step.
# news_target uses the NEW target's protected PostgreSQL service/pass files.
cd "$NEWS_CLEAR"
# Docker role files can be pretty-printed; COPY expects one compact JSON record.
jq -c . background.json > background-input.json
chmod 600 background-input.json
PGSERVICE=news_target psql -XqAt -v ON_ERROR_STOP=1 \
  -v target_database="$NEWS_TARGET_DB" \
  -v installation_id="$(jq -r .identity.installation_id settings.json)" \
  -f "$NEWS_REPO/deploy/recovery/import-settings.sql" 2>/dev/null || {
  NEWS_SQL_STATUS=$?; echo 'Settings import failed; target remains inactive' >&2
  exit "$NEWS_SQL_STATUS"
}
```

The recipe materializes all seven operational settings before assigning a revision. Saved fields win; missing fields use the matching background manifest’s NEWS_LIMIT_* values, then the application/adoption defaults (limits off, 50 total, 50 per source, one day, no date cutoff, processing on). A partial old set is repaired only in the fresh target, never written back to the source. The complete set is range/date/type validated; invalid input rolls back the transaction. Delivery is always forced off and the operational revision advances.

The recipe imports explicit columns in dependency order, resets source cursors/error state, restores sequences, excludes setup tokens and histories, and commits with `maintenance_mode=true` and delivery disabled. It rejects populated targets, unknown format/version, mismatched identity or another active target connection. A repeat is refused rather than merged into live configuration. Do not remove a guard to import over an existing installation. If import fails, keep the new target inactive; investigate and rebuild a fresh target instead of applying ad hoc partial SQL.

### Reattach private state and validate

Restore the five role files and adopted marker into **new**, empty per-role volumes with their correct owners/modes (web/background/extractor/egress UID 65532, domain UID 10001; verify the pinned images). Check installation ID/key ID parity against the imported DB again. Rebind only the specifically reviewed database names/endpoints and infrastructure network mappings. Keep native browser/egress credentials matching the external stack; do not rotate them implicitly.

Generate a new web JWT signing secret privately to invalidate old sessions; keep the restored administrator password hash. Other service tokens must remain consistent across their paired roles. Do not expose a private role file to another runtime. Record the rebind and new web signing key in the new protected installation state, not in Git or console output.

Use the existing native browser/egress only after explicit activation approval. For a rehearsal use synthetic tokens/endpoints and deny all external egress. Do not start a cloned stack with production provider credentials, Telegram destinations or an internet route. No real provider request or Telegram test is needed: domain-local credential decryption and authenticated configuration reads are sufficient.

Verify source/category/mapping counts and values, administrator state, AI/provider configuration, Telegram settings, source configuration reset and role-denial boundaries. Articles, editions, queues and receipts must be empty. Wrong key/installation ID must fail credential decryption. A database dump without the matching private state is not a valid copy.

### Recreate schedules without replay

Keep the scheduler's global control **paused**. Start background only while the restored installation remains in maintenance and the delivery flag remains false; verify no work is executing. Recreate the logical resources in `schedules.json` through the existing authenticated scheduler `PUT /v1/schedules/{schedule_key}` API, with `expected_revision: 0` on each new resource. Send `enabled: false` for every recovered definition and only `calendar`, `target_alias`, `operation` and `payload` from the saved definition. Preserve the original enabled flags in the authenticated archive for activation. Do not send historical revision/due/occurrence fields.

The scheduler validates each resource. All recovered definitions must remain disabled with no due instant throughout acceptance; global pause alone does not prevent a due instant from becoming overdue. Do not calculate DST calendars in shell or import old occurrences. A conflicting existing schedule is a failed clean-target preflight, not an invitation to overwrite it.

### Explicit acceptance and release

Before releasing maintenance, verify the real admin route and settings API through the intended HTTPS/Access chain; native `/auth/login` alone is insufficient. Verify healthy services, zero newly executing jobs, empty scheduler outbox and paused control. On the original host, verify unchanged protected browser/Readeck/PostgreSQL/proxy fingerprints.

The operator explicitly releases maintenance and authorizes the first collection with delivery still disabled. Confirm fresh source receipts and content. Only then deliberately enable delivery. Immediately before resuming schedules, enable only the originally enabled definitions through their existing CAS PUT API with current revisions. This recomputes due times from activation time, even after a long acceptance window. Verify every enabled definition is still strictly in the future before releasing global pause; if a due instant passes during preparation, disable/re-enable it and recheck before release. Originally disabled definitions remain disabled. There is no automatic resend of an old digest or replay of missed days. If any acceptance step fails, keep the target paused/inactive and the original installation unchanged.

## Verification and limits

The isolated recipe gate is `TestSettingsRecoveryRecipes` in `go-domain/recovery/settings_recipe_test.go`. It requires `psql` and an explicitly named loopback `settings_recovery_fixture` database via `NEWS_SETTINGS_RECOVERY_TEST_URL`. It creates/drops only uniquely named test databases, runs the actual published SQL files, and uses synthetic source/admin/provider settings. Never use a shared or production URL.

Run it against PostgreSQL 16 and 17 before accepting changes. Unit tests for credentialstore/recovery cover ciphertext/identity and encrypted bundle failures separately. Full native-browser/external-server recovery is still an operator workflow; this procedure does not add unsupported functionality to `newsctl`. Actual production backup and restoration, off-host destination setup, automatic scheduling/retention, and browser infrastructure recovery require separate authorization.
