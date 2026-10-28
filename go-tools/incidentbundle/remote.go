package incidentbundle

const remoteScriptPrefix = `set +e
set -o pipefail
DOMAIN='@DOMAIN@'; BACKGROUND='@BACKGROUND@'; EXTRACTOR='@EXTRACTOR@'; DB='@DB@'; DBUSER='@DBUSER@'; DBNAME='@DBNAME@'; SCHEMA='@SCHEMA@'
LOG_CURSOR_BACKGROUND='@LOG_CURSOR_BACKGROUND@'; LOG_CURSOR_EXTRACTOR='@LOG_CURSOR_EXTRACTOR@'
POLL_START_UTC=$(date -u +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null)
case "$POLL_START_UTC" in ????-??-??T??:??:??Z) printf 'log_cursor_seed=%s\n' "$POLL_START_UTC" ;; *) POLL_START_UTC= ;; esac
awk '/^(MemTotal|MemAvailable|SwapTotal|SwapFree):/ {gsub(":", "", $1); print tolower($1) "=" $2}' /proc/meminfo 2>/dev/null
awk '/^(some|full) / {for(i=1;i<=NF;i++) if($i ~ /^avg10=/) {split($i,a,"="); print "psi_" $1 "=" a[2]}}' /proc/pressure/memory 2>/dev/null
container_snapshot() {
  role="$1"; name="$2"; [ -n "$name" ] || return
  meta=$(docker inspect --format '{{.Id}} {{.State.StartedAt}} {{.RestartCount}} {{.State.OOMKilled}} {{.State.Pid}}' "$name" 2>/dev/null) || { echo "${role}_error=container_unavailable"; return; }
  set -- $meta; cid="$1"; started="$2"; restarts="$3"; oom="$4"; hostpid="$5"
  case "$cid" in *[!a-fA-F0-9]*|'') return;; esac
  printf '%s\n' "${role}_container=$cid" "${role}_started=$started" "${role}_restarts=$restarts" "${role}_oom=$oom"
  [ "$hostpid" -gt 1 ] 2>/dev/null || return
  cgpath=$(awk -F: '$1 == "0" {print $3; exit}' "/proc/$hostpid/cgroup" 2>/dev/null)
  [ -n "$cgpath" ] || { echo "${role}_error=cgroup_unavailable"; return; }
  base="/sys/fs/cgroup$cgpath"
  current=$(cat "$base/memory.current" 2>/dev/null); file=$(awk '$1 == "file" {print $2}' "$base/memory.stat" 2>/dev/null); inactive=$(awk '$1 == "inactive_file" {print $2}' "$base/memory.stat" 2>/dev/null)
  periods=$(awk '$1 == "nr_periods" {print $2}' "$base/cpu.stat" 2>/dev/null); throttled=$(awk '$1 == "nr_throttled" {print $2}' "$base/cpu.stat" 2>/dev/null); usec=$(awk '$1 == "throttled_usec" {print $2}' "$base/cpu.stat" 2>/dev/null); oomkills=$(awk '$1 == "oom_kill" {print $2}' "$base/memory.events" 2>/dev/null)
  case "$current:$file:$inactive:$periods:$throttled:$usec:$oomkills" in *[!0-9:]*|'') echo "${role}_error=cgroup_unavailable"; return;; esac
  printf '%s\n' "${role}_memory_current=$current" "${role}_file=$file" "${role}_inactive_file=$inactive" "${role}_periods=$periods" "${role}_throttled_periods=$throttled" "${role}_throttled_usec=$usec" "${role}_oomkills=$oomkills"
}
container_snapshot background "$BACKGROUND"; container_snapshot extractor "$EXTRACTOR"; container_snapshot postgres "$DB"
sql="BEGIN READ ONLY; SET LOCAL statement_timeout='500ms'; SELECT jsonb_build_object('captured_at',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z','worker',(SELECT jsonb_build_object('generation',generation,'pid',lock_backend_pid,'heartbeat_at',to_char(heartbeat_at,'YYYY-MM-DD\"T\"HH24:MI:SS.US') || 'Z') FROM \"$SCHEMA\".background_worker_status WHERE id=1),'pg_stat_activity',(SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'state',state,'wait_type',wait_event_type,'wait_event',wait_event,'transaction_age_seconds',extract(epoch FROM (clock_timestamp()-xact_start)),'blocker_pids',pg_blocking_pids(pid))),'[]'::jsonb) FROM (SELECT pid,state,wait_event_type,wait_event,xact_start FROM pg_stat_activity WHERE pid<>pg_backend_pid() ORDER BY pid LIMIT 64) a),'pg_metrics',(SELECT jsonb_build_object('wal_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0'),'checkpoints_timed',nullif(to_jsonb(b)->>'checkpoints_timed','')::numeric,'checkpoints_requested',nullif(to_jsonb(b)->>'checkpoints_req','')::numeric,'checkpoint_write_time_ms',nullif(to_jsonb(b)->>'checkpoint_write_time','')::numeric,'checkpoint_sync_time_ms',nullif(to_jsonb(b)->>'checkpoint_sync_time','')::numeric,'buffers_checkpoint',nullif(to_jsonb(b)->>'buffers_checkpoint','')::numeric,'buffers_clean',nullif(to_jsonb(b)->>'buffers_clean','')::numeric) FROM pg_stat_bgwriter b)); COMMIT;"
dbresult=$(docker exec "$DB" psql -XAtq -v ON_ERROR_STOP=1 -U "$DBUSER" -d "$DBNAME" -c "$sql" 2>/dev/null)
if [ -n "$dbresult" ]; then echo "db_json=$dbresult"; else echo "database_error=database_unavailable"; fi
`

const remoteLogScript = `
capture_logs() {
 role="$1"; name="$2"; cursor="$3"; [ -n "$name" ] || return
 case "$cursor" in ????-??-??T??:??:??Z) since="$cursor" ;; *) since="$POLL_START_UTC" ;; esac
 case "$since" in ????-??-??T??:??:??Z) ;; *) echo "${role}_log_error=log_unavailable"; return ;; esac
 boundary=$(date -u +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null)
 case "$boundary" in ????-??-??T??:??:??Z) ;; *) echo "${role}_log_error=log_unavailable"; return ;; esac
 timeout 3 docker logs --since "$since" --tail 65 "$name" 2>&1 | timeout 4 docker exec -i "$DOMAIN" /usr/local/bin/news-domain incident-log-filter --role "$role"
 statuses=("${PIPESTATUS[@]}")
 if [ "${statuses[0]}" -eq 0 ] && [ "${statuses[1]}" -eq 0 ]; then
   printf 'log_cursor_%s=%s\n' "$role" "$boundary"
 elif [ "${statuses[0]}" -eq 124 ] || [ "${statuses[1]}" -eq 124 ]; then
   printf '%s_log_error=timeout\n' "$role"
 elif [ "${statuses[1]}" -eq 3 ]; then
   printf '%s_log_error=log_overflow\n' "$role"
 else
   printf '%s_log_error=log_unavailable\n' "$role"
 fi
}
capture_logs background "$BACKGROUND" "$LOG_CURSOR_BACKGROUND"
capture_logs extractor "$EXTRACTOR" "$LOG_CURSOR_EXTRACTOR"
`
