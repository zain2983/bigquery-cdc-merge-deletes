#!/usr/bin/env bash
# What does a Postgres DELETE carry under each replica identity, and what does each identity cost in WAL?
#
# Needs a Postgres 14+ server with wal_level=logical that psql can reach (PGHOST, PGPORT, PGUSER, PGDATABASE).
# With Docker:
#   docker run -d --name ri-pg -e POSTGRES_PASSWORD=pg -p 55432:5432 postgres:17 -c wal_level=logical
#   PGHOST=localhost PGPORT=55432 PGUSER=postgres PGPASSWORD=pg ./pg_identity.sh
#
# For each identity (DEFAULT, USING INDEX on a unique (uuid, created_at) index, FULL) it builds the same table, then
# measures the WAL written by inserts, by updates that leave the key alone, and by deletes, and decodes one delete
# and one partition-moving update with test_decoding to show which old values reach a CDC reader.
set -euo pipefail
ROWS="${ROWS:-100000}"          # seeded rows per table
UPDATES="${UPDATES:-20000}"     # non-key updates (status and payload)
DELETES="${DELETES:-5000}"
PAYLOAD="${PAYLOAD:-400}"       # payload bytes per row; above ~2,000 the payload is TOASTed
OUT="${OUT:-results/pg_identity.csv}"
mkdir -p "$(dirname "$OUT")"

p() { psql -X -q -v ON_ERROR_STOP=1 -At "$@"; }

wal() { p -c "SELECT pg_current_wal_lsn()"; }
diff_bytes() { p -c "SELECT pg_wal_lsn_diff('$2', '$1')::bigint"; }

echo "identity,payload_bytes,phase,rows,wal_bytes,wal_bytes_per_row" >"$OUT"
for mode in default index full; do
  t="ri_$mode"
  p <<SQL
DROP TABLE IF EXISTS $t;
CREATE TABLE $t (uuid text PRIMARY KEY, created_at timestamptz NOT NULL, status text, payload jsonb);
SQL
  case $mode in
    index) p -c "CREATE UNIQUE INDEX ${t}_ident ON $t (uuid, created_at); ALTER TABLE $t REPLICA IDENTITY USING INDEX ${t}_ident" ;;
    full) p -c "ALTER TABLE $t REPLICA IDENTITY FULL" ;;
  esac

  p -c "CHECKPOINT"
  a=$(wal)
  # Distinct hex per 32 bytes, so the payload doesn't compress away.
  p -c "INSERT INTO $t SELECT md5('r-' || n), TIMESTAMPTZ '2026-10-01' + n * INTERVAL '1 second', 'created',
    jsonb_build_object('seq', n, 'note', left((SELECT string_agg(md5(n::text || '-' || i), '')
      FROM generate_series(1, ($PAYLOAD / 32) + 1) i), $PAYLOAD))
    FROM generate_series(1, $ROWS) n"
  b=$(wal)
  echo "$mode,$PAYLOAD,insert,$ROWS,$(diff_bytes "$a" "$b"),$(( $(diff_bytes "$a" "$b") / ROWS ))" >>"$OUT"

  p -c "CHECKPOINT"
  a=$(wal)
  p -c "UPDATE $t SET status = 'updated', payload = payload || '{\"v\": 2}'
    WHERE uuid IN (SELECT md5('r-' || n) FROM generate_series(1, $UPDATES) n)"
  b=$(wal)
  echo "$mode,$PAYLOAD,update,$UPDATES,$(diff_bytes "$a" "$b"),$(( $(diff_bytes "$a" "$b") / UPDATES ))" >>"$OUT"

  p -c "CHECKPOINT"
  a=$(wal)
  p -c "DELETE FROM $t WHERE uuid IN (SELECT md5('r-' || n) FROM generate_series($ROWS - $DELETES + 1, $ROWS) n)"
  b=$(wal)
  echo "$mode,$PAYLOAD,delete,$DELETES,$(diff_bytes "$a" "$b"),$(( $(diff_bytes "$a" "$b") / DELETES ))" >>"$OUT"

  # Decode one delete and one update that moves created_at (as a CDC reader would see them).
  p -c "SELECT 'init' FROM pg_create_logical_replication_slot('ri_peek', 'test_decoding')" >/dev/null
  p -c "DELETE FROM $t WHERE uuid = md5('r-' || ($UPDATES + 1));
        UPDATE $t SET created_at = created_at + INTERVAL '1 day' WHERE uuid = md5('r-' || ($UPDATES + 2))"
  echo "--- $mode: what the CDC reader receives"
  p -c "SELECT regexp_replace(data, '''\{[^'']*\}''', '''{...}''', 'g') FROM pg_logical_slot_get_changes('ri_peek', NULL, NULL)
        WHERE data LIKE 'table%'"
  p -c "SELECT pg_drop_replication_slot('ri_peek')" >/dev/null
done

echo "--- WAL per row ($OUT)"
column -s, -t <"$OUT" 2>/dev/null || cat "$OUT"
