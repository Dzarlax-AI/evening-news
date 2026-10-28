CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.digest_delivery_intents (
                id UUID PRIMARY KEY,
                edition_id UUID NOT NULL REFERENCES __SCHEMA_Q__.editorial_digest_editions(id),
                destination_hash CHAR(64) NOT NULL CHECK (destination_hash ~ '^[0-9a-f]{64}$'),
                purpose VARCHAR(32) NOT NULL CHECK (purpose = 'ru_digest'),
                render_hash CHAR(64) NOT NULL CHECK (render_hash ~ '^[0-9a-f]{64}$'),
                job_id UUID NOT NULL,
                attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
                domain_instance_id UUID NOT NULL,
                state VARCHAR(16) NOT NULL CHECK (state IN ('active','completed')),
                parts_sent INTEGER CHECK (parts_sent > 0),
                rich_message BOOLEAN,
                CHECK ((state = 'active' AND parts_sent IS NULL AND rich_message IS NULL)
                    OR (state = 'completed' AND parts_sent IS NOT NULL AND rich_message IS NOT NULL)),
                created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
                UNIQUE (edition_id, destination_hash, purpose)
            );

CREATE TABLE IF NOT EXISTS __SCHEMA_Q__.digest_delivery_parts (
                intent_id UUID NOT NULL REFERENCES __SCHEMA_Q__.digest_delivery_intents(id),
                method VARCHAR(16) NOT NULL CHECK (method IN ('sendMessage', 'sendRichMessage')),
                part_index INTEGER NOT NULL CHECK (part_index >= 0),
                payload_hash CHAR(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
                job_id UUID NOT NULL,
                attempt_generation INTEGER NOT NULL CHECK (attempt_generation > 0),
                domain_instance_id UUID NOT NULL,
                state VARCHAR(16) NOT NULL CHECK (state IN ('sending', 'sent', 'rejected', 'unknown')),
                message_id BIGINT,
                status_code INTEGER CHECK (status_code BETWEEN 100 AND 599),
                retry_after_seconds DOUBLE PRECISION CHECK (retry_after_seconds >= 0),
                created_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
                updated_at TIMESTAMP NOT NULL DEFAULT timezone('UTC', now()),
                PRIMARY KEY (intent_id, method, part_index),
                CHECK (state = 'sent' OR message_id IS NULL)
            );
