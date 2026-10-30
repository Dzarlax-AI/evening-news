-- Logical definitions only. Replay, control revision and due instants are excluded.
\set ON_ERROR_STOP on
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout = '30s';
SELECT jsonb_build_object('format','news-schedules-v1','schedules',COALESCE((SELECT jsonb_agg(jsonb_build_object('schedule_key',schedule_key,'enabled',enabled,'calendar',calendar,'target_alias',target_alias,'operation',operation,'payload',payload) ORDER BY schedule_key) FROM scheduler_service.schedules),'[]'::jsonb));
COMMIT;
