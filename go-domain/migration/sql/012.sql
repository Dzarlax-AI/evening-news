CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.article_ingestion_runs (
                id UUID PRIMARY KEY,
                job_id UUID NOT NULL REFERENCES __SCHEMA_Q__.background_jobs(id) ON DELETE CASCADE,
                attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
                envelope_sha256 VARCHAR(64) NOT NULL CHECK (length(envelope_sha256)=64),
                owner_id UUID NOT NULL,
                worker_generation INTEGER NOT NULL CHECK (worker_generation > 0),
                backend_pid INTEGER NOT NULL CHECK (backend_pid > 0),
                executor_instance_id UUID NOT NULL,
                deadline_at TIMESTAMP NOT NULL,
                state VARCHAR(20) NOT NULL DEFAULT 'running'
                    CHECK (state IN ('running','succeeded','failed','cancelled','interrupted')),
                started_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                updated_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                finished_at TIMESTAMP,
                result JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(result)='object'),
                error_code VARCHAR(40),
                UNIQUE (job_id, attempt_generation)
            );

CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.article_ingestion_source_runs (
                run_id UUID NOT NULL REFERENCES __SCHEMA_Q__.article_ingestion_runs(id) ON DELETE CASCADE,
                source_id INTEGER NOT NULL,
                source_type VARCHAR(20) NOT NULL CHECK (source_type IN ('rss','telegram','custom')),
                config_sha256 VARCHAR(64) NOT NULL CHECK (length(config_sha256)=64),
                state VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending','running','succeeded','partial','failed','skipped',
                                     'busy','deferred','resource_limit','unavailable')),
                candidates INTEGER NOT NULL DEFAULT 0 CHECK (candidates >= 0),
                saved INTEGER NOT NULL DEFAULT 0 CHECK (saved >= 0),
                updated INTEGER NOT NULL DEFAULT 0 CHECK (updated >= 0),
                skipped INTEGER NOT NULL DEFAULT 0 CHECK (skipped >= 0),
                fetched_at TIMESTAMP,
                error_code VARCHAR(40),
                selector_study_sha256 VARCHAR(64),
                PRIMARY KEY (run_id, source_id)
            );

CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.source_collection_state (
                source_id INTEGER PRIMARY KEY REFERENCES __SCHEMA_Q__.sources(id) ON DELETE CASCADE,
                config_sha256 VARCHAR(64) NOT NULL CHECK (length(config_sha256)=64),
                scan_limit INTEGER NOT NULL DEFAULT 20 CHECK (scan_limit BETWEEN 1 AND 100),
                cursor_offset INTEGER NOT NULL DEFAULT 0 CHECK (cursor_offset BETWEEN 0 AND 100),
                scan_active BOOLEAN NOT NULL DEFAULT FALSE,
                previous_hashes JSONB NOT NULL DEFAULT '{}'::jsonb
                    CHECK (jsonb_typeof(previous_hashes)='object'),
                scan_hashes JSONB NOT NULL DEFAULT '{}'::jsonb
                    CHECK (jsonb_typeof(scan_hashes)='object'),
                acknowledged_hashes JSONB NOT NULL DEFAULT '{}'::jsonb
                    CHECK (jsonb_typeof(acknowledged_hashes)='object'),
                snapshot_at TIMESTAMP,
                content_sha256 VARCHAR(64),
                updated_at TIMESTAMP NOT NULL DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                CHECK (cursor_offset <= scan_limit)
            );

CREATE OR REPLACE FUNCTION __SCHEMA_Q__.fence_article_ingestion_attempt(
                p_run_id UUID,
                p_job_id UUID,
                p_attempt_generation INTEGER,
                p_envelope_sha256 TEXT,
                p_owner_id UUID,
                p_worker_generation INTEGER,
                p_backend_pid INTEGER,
                p_executor_instance_id UUID,
                p_deadline_at TIMESTAMP
            ) RETURNS TABLE(run_id UUID, run_state TEXT)
            LANGUAGE plpgsql SECURITY DEFINER
            SET search_path = pg_catalog, __SCHEMA_Q__
            AS $function$
            DECLARE
                v_now TIMESTAMP;
                v_owner UUID;
                v_generation INTEGER;
                v_pid INTEGER;
                v_heartbeat TIMESTAMP;
                v_worker_state TEXT;
                v_job_kind TEXT;
                v_job_state TEXT;
                v_job_owner INTEGER;
                v_job_attempt INTEGER;
                v_payload JSONB;
                v_receipt_executor UUID;
                v_receipt_owner UUID;
                v_receipt_generation INTEGER;
                v_receipt_deadline TIMESTAMP;
                v_receipt_state TEXT;
                v_existing __SCHEMA_Q__.article_ingestion_runs%ROWTYPE;
                v_held BOOLEAN;
            BEGIN
                v_now := clock_timestamp() AT TIME ZONE 'UTC';
                SELECT owner_id, generation, lock_backend_pid, heartbeat_at, status
                  INTO v_owner, v_generation, v_pid, v_heartbeat, v_worker_state
                  FROM __SCHEMA_Q__.background_worker_status WHERE id=1 FOR UPDATE;
                IF NOT FOUND OR v_worker_state <> 'running' OR v_owner IS DISTINCT FROM p_owner_id
                   OR v_generation IS DISTINCT FROM p_worker_generation
                   OR v_pid IS DISTINCT FROM p_backend_pid OR v_heartbeat IS NULL
                   OR v_heartbeat > v_now OR v_now-v_heartbeat > INTERVAL '20 seconds' THEN
                    RAISE EXCEPTION 'stale_ingestion_fence' USING ERRCODE='P0001';
                END IF;
                SELECT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_locks l
                    WHERE l.locktype='advisory' AND l.granted AND l.pid=p_backend_pid AND l.objsubid=2
                      AND l.classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid)
                      AND l.objid=(((hashtext(__SCHEMA_LITERAL__)::bigint+4294967296)%4294967296)::oid)
                      AND l.database=(SELECT oid FROM pg_catalog.pg_database
                                      WHERE datname=current_database())
                ) INTO v_held;
                IF NOT v_held THEN
                    RAISE EXCEPTION 'stale_ingestion_fence' USING ERRCODE='P0001';
                END IF;
                SELECT kind, state, owner_generation, attempt_generation, payload
                  INTO v_job_kind, v_job_state, v_job_owner, v_job_attempt, v_payload
                  FROM __SCHEMA_Q__.background_jobs WHERE id=p_job_id FOR UPDATE;
                IF NOT FOUND OR v_job_state <> 'running' OR v_job_owner IS DISTINCT FROM p_worker_generation
                   OR v_job_attempt IS DISTINCT FROM p_attempt_generation
                   OR (v_job_kind <> 'news_processing' AND NOT (
                       v_job_kind='news_digest' AND COALESCE(v_payload->'run_processing','true'::jsonb)='true'::jsonb)) THEN
                    RAISE EXCEPTION 'stale_ingestion_job' USING ERRCODE='P0001';
                END IF;
                SELECT * INTO v_existing FROM __SCHEMA_Q__.article_ingestion_runs
                  WHERE job_id=p_job_id AND attempt_generation=p_attempt_generation FOR UPDATE;
                IF FOUND THEN
                    IF v_existing.envelope_sha256 <> p_envelope_sha256 OR v_existing.id <> p_run_id
                       OR v_existing.owner_id <> p_owner_id OR v_existing.worker_generation <> p_worker_generation
                       OR v_existing.backend_pid <> p_backend_pid
                       OR v_existing.executor_instance_id <> p_executor_instance_id
                       OR v_existing.deadline_at <> p_deadline_at THEN
                        RAISE EXCEPTION 'ingestion_envelope_conflict' USING ERRCODE='P0001';
                    END IF;
                ELSE
                    IF p_deadline_at <= v_now THEN
                        RAISE EXCEPTION 'ingestion_deadline_expired' USING ERRCODE='P0001';
                    END IF;
                    INSERT INTO __SCHEMA_Q__.article_ingestion_runs
                        (id,job_id,attempt_generation,envelope_sha256,owner_id,worker_generation,
                         backend_pid,executor_instance_id,deadline_at,state)
                    VALUES (p_run_id,p_job_id,p_attempt_generation,p_envelope_sha256,p_owner_id,
                            p_worker_generation,p_backend_pid,p_executor_instance_id,p_deadline_at,'running')
                    ON CONFLICT (job_id,attempt_generation) DO NOTHING;
                    SELECT * INTO v_existing FROM __SCHEMA_Q__.article_ingestion_runs
                      WHERE job_id=p_job_id AND attempt_generation=p_attempt_generation FOR UPDATE;
                    IF v_existing.envelope_sha256 <> p_envelope_sha256 OR v_existing.id <> p_run_id THEN
                        RAISE EXCEPTION 'ingestion_envelope_conflict' USING ERRCODE='P0001';
                    END IF;
                END IF;
                SELECT executor_instance_id, owner_id, worker_generation,
                       deadline_at, state
                  INTO v_receipt_executor, v_receipt_owner, v_receipt_generation,
                       v_receipt_deadline, v_receipt_state
                  FROM __SCHEMA_Q__.background_job_executions
                  WHERE job_id=p_job_id AND attempt_generation=p_attempt_generation FOR UPDATE;
                v_now := clock_timestamp() AT TIME ZONE 'UTC';
                IF NOT FOUND OR v_receipt_executor IS DISTINCT FROM p_executor_instance_id
                   OR v_receipt_owner IS DISTINCT FROM p_owner_id
                   OR v_receipt_generation IS DISTINCT FROM p_worker_generation
                   OR v_receipt_deadline IS DISTINCT FROM p_deadline_at OR v_receipt_state <> 'running'
                   OR p_deadline_at <= v_now OR v_heartbeat IS NULL OR v_heartbeat > v_now
                   OR v_now-v_heartbeat > INTERVAL '20 seconds' THEN
                    RAISE EXCEPTION 'inactive_ingestion_executor' USING ERRCODE='P0001';
                END IF;
                RETURN QUERY SELECT v_existing.id, v_existing.state::TEXT;
            END;
            $function$;

REVOKE ALL ON FUNCTION __SCHEMA_Q__.fence_article_ingestion_attempt(UUID,UUID,INTEGER,TEXT,UUID,INTEGER,INTEGER,UUID,TIMESTAMP) FROM PUBLIC;
