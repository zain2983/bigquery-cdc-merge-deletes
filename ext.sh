#!/usr/bin/env bash
# Extension tests for Artie's BigQuery MERGE partition-pruning harness.
#
# Builds on run.sh from https://github.com/artie-labs/benchmarks/tree/master/bigquery-merge-partition (MIT license,
# copyright Artie). Copy run.sh next to this file; ext.sh sources it for its helpers (q, rows, day, clone, stage, ...).
#
#   PROJECT=my-project ./ext.sh                    # every step below, in order
#   PROJECT=my-project ./ext.sh xsetup h2h report  # the reproduction only (run.sh's h2h step)
#   PROJECT=my-project ./ext.sh cleanup            # drop the benchmark dataset
#
# Steps:
#   xsetup     generate only the two 3-year tables the tests need (run.sh's setup makes nine)
#   xkeymap    build the uuid -> created_at lookup table, partitioned by a hash bucket of uuid
#   xdeletes   one batch whose 50 deletes carry only the primary key, as Postgres sends them under the default
#              replica identity. Arms: a = no predicate (what a merge must fall back to), s = split, l = lookup.
#              Run twice: deletes of yesterday's rows, and deletes spread over 50 days of history.
#   xsequence  XSEQ consecutive batches per arm on persistent tables, adding d = deferred deletes; then xverify
#   xverify    all arms end with the same rows (cheap checks; see the function)
#   xmoves     correctness: updates that move a row to another partition, with and without the old partition value
#   xspread    filtered vs unfiltered on the SAME spread batch (30, 120, 365 partitions); run.sh only ran filtered
#   xadvisor   dry runs only (free): filtered vs unfiltered estimates for batches spread over 1..365 partitions
#   xreport    write results/xops.csv and results/xdryrun.csv; summarize.py turns them into results/xsummary.csv
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$HERE/run.sh" ] || { echo "copy Artie's run.sh next to ext.sh first" >&2; exit 1; }
# shellcheck source=run.sh
source "$HERE/run.sh"

BUCKETS="${BUCKETS:-3000}"     # keymap hash buckets; one CTAS may write at most 4,000 partitions
XREPS="${XREPS:-3}"            # reps per arm in xdeletes
XSEQ="${XSEQ:-12}"             # batches in xsequence (one hour of five-minute flushes)
FLUSH_EVERY="${FLUSH_EVERY:-6}" # arm d applies queued deletes every this many batches
XDEL="${XDEL:-recent}"         # which deletes xsequence uses: recent | spread
DRYRUN_CSV="$OUT/xdryrun.csv"
[ -f "$DRYRUN_CSV" ] || echo "scenario,arm,rep,part,batch,dryrun_bytes" >"$DRYRUN_CSV"

# ---------- helpers ----------

# Dry-runs a statement and records BigQuery's byte estimate. Dry runs are free and create no job.
dryrun() { # scenario arm rep part batch sql
  local est
  est="$(bq --project_id="$PROJECT" --location="$LOCATION" --format=json query --use_legacy_sql=false --dry_run "$6" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("statistics",{}).get("totalBytesProcessed",""))')"
  echo "$1,$2,$3,$4,$5,$est" >>"$DRYRUN_CSV"
}

# Runs one measured operation: dry run first, then the real job, labelled so xreport can sum a merge's parts.
xq() { # scenario arm rep part batch sql
  dryrun "$@"
  q --label=kind:xop --label="scenario:$1" --label="arm:$2" --label="rep:$3" --label="part:$4" --label="batch:$5" "$6" >/dev/null
}

# Artie's partition predicate for the rows of a table: empty when any row lacks a partition value, or there are none.
# Like Transfer, this is computed outside the measured MERGE (Transfer does it in memory).
pred_for() { # table where
  local raw sep="','"
  raw="$(q --label=kind:predicate --format=csv "SELECT IF(COUNTIF(created_at IS NULL) > 0, 'NULL',
  IFNULL(STRING_AGG(DISTINCT FORMAT_TIMESTAMP('%F', created_at, 'UTC'), '|'), 'EMPTY')) FROM \`$DS.$1\` WHERE $2" | tail -n1)"
  case "$raw" in
    NULL | EMPTY) echo "" ;;
    *) echo " AND DATE_TRUNC(tgt.\`created_at\`, DAY) IN ('${raw//|/$sep}')" ;;
  esac
}

# The MERGE run.sh uses (Artie Transfer's statement), from any source table.
merge_sql() { # target source predicate
  echo "MERGE INTO \`$DS.$1\` tgt USING \`$DS.$2\` AS stg ON tgt.\`uuid\` = stg.\`uuid\`$3
WHEN MATCHED AND stg.\`__artie_delete\` THEN DELETE
WHEN MATCHED AND IFNULL(stg.\`__artie_delete\`, false) = false THEN UPDATE SET \`uuid\`=stg.\`uuid\`,\`created_at\`=stg.\`created_at\`,\`status\`=stg.\`status\`,\`payload\`=stg.\`payload\`
WHEN NOT MATCHED AND IFNULL(stg.\`__artie_delete\`, false) = false THEN INSERT (\`uuid\`,\`created_at\`,\`status\`,\`payload\`) VALUES (stg.\`uuid\`,stg.\`created_at\`,stg.\`status\`,stg.\`payload\`);"
}

bucket_expr() { echo "ABS(MOD(FARM_FINGERPRINT($1), $BUCKETS))"; }

# Batch k as Postgres sends it under the default replica identity: 300 inserts and 150 updates as in run.sh's
# recent_batch, and 50 deletes that carry only uuid. recent = rows created yesterday at 23:00 (as in run.sh);
# spread = one row from each of 50 days across the 3 years (e.g. account-erasure requests).
pk_batch() { # k recent|spread
  local k=$1 ins=$((RPD * ($1 + 2) + RPD / 24 + 34)) del
  if [ "$2" = spread ]; then
    del="$(rows "ARRAY(SELECT DATE_SUB(DATE '$ANCHOR', INTERVAL 2 + i * 21 DAY) FROM UNNEST(GENERATE_ARRAY(0, 49)) i)" $((k + 1)) $((k + 1)) x)"
  else
    del="$(rows "$(day 1)" $((RPD - (k + 1) * 50 + 1)) $((RPD - k * 50)) x)"
  fi
  echo "SELECT *, false AS __artie_delete FROM ($(rows "$(day 0)" "$ins" $((ins + 299)) "inserted_$k"))
UNION ALL SELECT *, false FROM ($(rows "$(day 0)" $((k * 150 + 1)) $((k * 150 + 150)) "updated_$k"))
UNION ALL SELECT uuid, CAST(NULL AS TIMESTAMP), CAST(NULL AS STRING), CAST(NULL AS JSON), true FROM ($del)"
}

# ---------- arms ----------

# a: no predicate. This is what any correct merge must do when a delete has no partition value.
arm_a() { # scenario target rep batch
  xq "$1" a "$3" merge "$4" "$(merge_sql "$2" stg "")"
}

# s: split. The rows that carry a partition value get Artie's filtered MERGE; primary-key-only deletes get a DELETE.
arm_s() { # scenario target rep batch
  q --label=kind:setup "CREATE OR REPLACE TABLE \`$DS.stg_up\` AS SELECT * FROM \`$DS.stg\` WHERE created_at IS NOT NULL;
CREATE OR REPLACE TABLE \`$DS.stg_del\` AS SELECT uuid FROM \`$DS.stg\` WHERE __artie_delete AND created_at IS NULL" >/dev/null
  xq "$1" s "$3" merge "$4" "$(merge_sql "$2" stg_up "$(pred_for stg_up true)")"
  xq "$1" s "$3" delete "$4" "DELETE FROM \`$DS.$2\` WHERE uuid IN (SELECT uuid FROM \`$DS.stg_del\`)"
}

# l: lookup. Fill in each primary-key-only delete's created_at from the keymap, reading only the hash buckets the
# deletes fall in, then run Artie's filtered MERGE. A key missing from the keymap stays NULL, so the predicate is
# dropped and the merge is still correct, just unfiltered. Overwrites stg, so stage again before the next arm.
arm_l() { # scenario target keymap rep batch
  local raw
  raw="$(q --label=kind:predicate --format=csv "SELECT IFNULL(STRING_AGG(DISTINCT CAST($(bucket_expr uuid) AS STRING), '|'), 'EMPTY')
FROM \`$DS.stg\` WHERE __artie_delete AND created_at IS NULL" | tail -n1)"
  if [ -n "$raw" ] && [ "$raw" != EMPTY ]; then
    xq "$1" l "$4" resolve "$5" "CREATE OR REPLACE TABLE \`$DS.stg\` AS
SELECT s.uuid, COALESCE(s.created_at, k.created_at) AS created_at, s.status, s.payload, s.__artie_delete
FROM \`$DS.stg\` s LEFT JOIN (SELECT uuid, created_at FROM \`$DS.$3\` WHERE bucket IN (${raw//|/,})) k
  ON s.__artie_delete AND s.created_at IS NULL AND k.uuid = s.uuid"
    xq "$1" l "$4" keymap_del "$5" "DELETE FROM \`$DS.$3\` WHERE bucket IN (${raw//|/,})
  AND uuid IN (SELECT uuid FROM \`$DS.stg\` WHERE __artie_delete)"
  fi
  xq "$1" l "$4" merge "$5" "$(merge_sql "$2" stg "$(pred_for stg true)")"
  # New keys only. A real pipeline would use the change's operation type; the harness marks inserts in status.
  xq "$1" l "$4" keymap_ins "$5" "INSERT INTO \`$DS.$3\` (uuid, created_at, bucket)
SELECT uuid, created_at, $(bucket_expr uuid) FROM \`$DS.stg\` WHERE STARTS_WITH(status, 'inserted')"
}

# d: deferred deletes. Filtered MERGE every batch; primary-key-only deletes wait in a queue and are applied together.
# Readers see a deleted row until its queue is flushed (use a view that anti-joins the queue if that matters), and a
# key that is deleted and re-inserted inside one window needs ordering (compare change positions) before this is safe.
arm_d() { # scenario target rep batch flush(0|1)
  q --label=kind:setup "CREATE OR REPLACE TABLE \`$DS.stg_up\` AS SELECT * FROM \`$DS.stg\` WHERE created_at IS NOT NULL" >/dev/null
  xq "$1" d "$3" queue "$4" "INSERT INTO \`$DS.del_queue\` (uuid) SELECT uuid FROM \`$DS.stg\` WHERE __artie_delete AND created_at IS NULL"
  xq "$1" d "$3" merge "$4" "$(merge_sql "$2" stg_up "$(pred_for stg_up true)")"
  if [ "$5" = 1 ]; then
    xq "$1" d "$3" delete "$4" "DELETE FROM \`$DS.$2\` WHERE uuid IN (SELECT uuid FROM \`$DS.del_queue\`)"
    q --label=kind:setup "TRUNCATE TABLE \`$DS.del_queue\`" >/dev/null
  fi
}

# ---------- steps ----------

step_xsetup() {
  ensure_results
  bq --project_id="$PROJECT" show "$PROJECT:$DATASET" >/dev/null 2>&1 ||
    bq --project_id="$PROJECT" --location="$LOCATION" mk --dataset "$PROJECT:$DATASET"
  generate daily_1095 1095 daily &
  local p1=$!
  generate daily_1095_clustered 1095 daily cluster &
  local p2=$!
  wait "$p1"
  wait "$p2"
}

step_xkeymap() {
  q --label=kind:xbuild "CREATE OR REPLACE TABLE \`$DS.keymap_1095\` (uuid STRING NOT NULL, created_at TIMESTAMP, bucket INT64)
PARTITION BY RANGE_BUCKET(bucket, GENERATE_ARRAY(0, $BUCKETS, 1)) CLUSTER BY uuid
AS SELECT uuid, created_at, $(bucket_expr uuid) AS bucket FROM \`$DS.daily_1095\`" >/dev/null
  log "built keymap_1095 ($BUCKETS buckets)"
}

step_xdeletes() {
  local mode i
  for mode in recent spread; do
    for i in $(seq "$XREPS"); do
      stage "$(pk_batch 0 "$mode")"
      clone daily_1095 work
      arm_a "del_$mode" work "$i" 0
      clone daily_1095 work
      arm_s "del_$mode" work "$i" 0
      clone daily_1095 work
      clone keymap_1095 km_work
      arm_l "del_$mode" work km_work "$i" 0
      log "deletes $mode rep $i"
    done
  done
}

step_xsequence() {
  local k flush
  for t in a s l d; do clone daily_1095 "xseq_$t"; done
  clone keymap_1095 km_seq
  q --label=kind:setup "CREATE OR REPLACE TABLE \`$DS.del_queue\` (uuid STRING)" >/dev/null
  for k in $(seq 0 $((XSEQ - 1))); do
    flush=0
    { [ $(((k + 1) % FLUSH_EVERY)) -eq 0 ] || [ "$k" -eq $((XSEQ - 1)) ]; } && flush=1
    stage "$(pk_batch "$k" "$XDEL")"
    arm_a "seq_$XDEL" xseq_a 1 "$k"
    arm_s "seq_$XDEL" xseq_s 1 "$k"
    arm_d "seq_$XDEL" xseq_d 1 "$k" "$flush"
    arm_l "seq_$XDEL" xseq_l km_seq 1 "$k" # last: it overwrites stg
    log "sequence batch $k"
  done
}

# Cheaper than a full-row fingerprint of every table (each would read all 51.5 GB): per-partition row counts from
# metadata, a fingerprint of the uuid column (about 3.7 GB per table), and a full-row fingerprint of the partitions
# the batches touched. Untouched partitions hold cloned data that no statement matched.
step_xverify() {
  local touched="DATE(created_at) >= DATE_SUB(DATE '$ANCHOR', INTERVAL 1 DAY)"
  [ "$XDEL" = spread ] && touched="TRUE"
  q --label=kind:verify --format=csv "
WITH parts AS (
  SELECT table_name, TO_JSON_STRING(ARRAY_AGG(STRUCT(partition_id, total_rows) ORDER BY partition_id)) AS layout
  FROM \`$DS\`.INFORMATION_SCHEMA.PARTITIONS WHERE table_name LIKE 'xseq_%' GROUP BY table_name)
SELECT t.tbl, t.row_count, t.uuid_fp, t.touched_fp, FARM_FINGERPRINT(p.layout) AS partition_fp FROM (
  SELECT 'xseq_a' AS tbl, COUNT(*) AS row_count, BIT_XOR(FARM_FINGERPRINT(uuid)) AS uuid_fp,
    (SELECT BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(x))) FROM \`$DS.xseq_a\` x WHERE $touched) AS touched_fp FROM \`$DS.xseq_a\`
  UNION ALL SELECT 'xseq_s', COUNT(*), BIT_XOR(FARM_FINGERPRINT(uuid)),
    (SELECT BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(x))) FROM \`$DS.xseq_s\` x WHERE $touched) FROM \`$DS.xseq_s\`
  UNION ALL SELECT 'xseq_l', COUNT(*), BIT_XOR(FARM_FINGERPRINT(uuid)),
    (SELECT BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(x))) FROM \`$DS.xseq_l\` x WHERE $touched) FROM \`$DS.xseq_l\`
  UNION ALL SELECT 'xseq_d', COUNT(*), BIT_XOR(FARM_FINGERPRINT(uuid)),
    (SELECT BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(x))) FROM \`$DS.xseq_d\` x WHERE $touched) FROM \`$DS.xseq_d\`
) t JOIN parts p ON p.table_name = t.tbl ORDER BY 1" | tee "$OUT/xverify.csv"
  local expected=$((1095 * RPD + XSEQ * 300 - XSEQ * 50))
  if [ "$(tail -n +2 "$OUT/xverify.csv" | cut -d, -f2- | sort -u | wc -l | tr -d ' ')" = 1 ] &&
    [ "$(tail -n +2 "$OUT/xverify.csv" | head -n1 | cut -d, -f2)" = "$expected" ]; then
    log "xverify ok: all four arms have $expected rows and identical fingerprints"
  else
    log "xverify FAILED: expected $expected rows and identical fingerprints in all arms"
    return 1
  fi
}

# 100 rows created yesterday get a corrected created_at one day later. Under the default replica identity the
# change carries only the new created_at; with REPLICA IDENTITY USING INDEX on a unique (uuid, created_at) index,
# Postgres also sends the old key, because an identity column changed.
step_xmoves() {
  local moved
  moved="SELECT uuid, TIMESTAMP_ADD(created_at, INTERVAL 1 DAY) AS created_at, 'moved' AS status, payload,
  false AS __artie_delete, created_at AS __old_created_at FROM ($(rows "$(day 1)" 1 100 x))"
  stage "$moved"
  q --label=kind:setup "CREATE OR REPLACE TABLE \`$DS.stg_parts\` AS
SELECT created_at FROM \`$DS.stg\` UNION ALL SELECT __old_created_at FROM \`$DS.stg\`" >/dev/null
  clone daily_1095 xmove_a
  xq moves a 1 merge 0 "$(merge_sql xmove_a stg "")"
  clone daily_1095 xmove_b
  xq moves b 1 merge 0 "$(merge_sql xmove_b stg "$(pred_for stg true)")"
  clone daily_1095 xmove_i
  xq moves i 1 merge 0 "$(merge_sql xmove_i stg "$(pred_for stg_parts true)")"
  local w="DATE(created_at) >= DATE_SUB(DATE '$ANCHOR', INTERVAL 1 DAY)"
  q --label=kind:verify --format=csv "
SELECT 'xmove_a' AS tbl, COUNT(*) AS rows_last_2_days, COUNT(*) - COUNT(DISTINCT uuid) AS duplicate_uuids,
  BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS fp FROM \`$DS.xmove_a\` t WHERE $w
UNION ALL SELECT 'xmove_b', COUNT(*), COUNT(*) - COUNT(DISTINCT uuid), BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) FROM \`$DS.xmove_b\` t WHERE $w
UNION ALL SELECT 'xmove_i', COUNT(*), COUNT(*) - COUNT(DISTINCT uuid), BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) FROM \`$DS.xmove_i\` t WHERE $w
ORDER BY 1" | tee "$OUT/xmoves.csv"
  log "expect: a and i identical with 0 duplicates; b keeps the old rows, so 100 duplicate uuids"
}

# Like-for-like break-even. run.sh's spread step runs only the filtered arm and compares it with the unfiltered merge
# of a different (recent) batch. BigQuery bills a MERGE as the referenced columns of every scanned partition plus the
# full size of every partition it changes, so the unfiltered merge of the same spread batch also pays for those
# changed partitions. This runs both arms on the same batch.
step_xspread() {
  local n i
  for n in 30 120 365; do
    stage "$(spread_batch "$n" 1095)"
    for i in $(seq "$XREPS"); do
      clone daily_1095 work
      xq "spread_n$n" a "$i" "merge" "n$n" "$(merge_sql work stg "")"
      clone daily_1095 work
      xq "spread_n$n" b "$i" "merge" "n$n" "$(merge_sql work stg "$(pred_for stg true)")"
    done
    log "spread $n partitions, both arms"
  done
}

# Free: dry runs only, on Artie's spread batches (about 500 updates over n partitions), against daily_1095.
step_xadvisor() {
  local n pred
  for n in 1 2 7 30 60 90 120 150 180 365; do
    stage "$(spread_batch "$n" 1095)"
    pred="$(pred_for stg true)"
    dryrun advisor a "n$n" merge 0 "$(merge_sql daily_1095 stg "")"
    dryrun advisor b "n$n" merge 0 "$(merge_sql daily_1095 stg "$pred")"
    log "advisor dry runs, $n partitions"
  done
}

step_xreport() {
  ensure_results
  q --label=kind:setup "CREATE TABLE IF NOT EXISTS \`$RDS.xops\` (run_id STRING, scenario STRING, arm STRING, rep STRING,
  part STRING, batch STRING, job_id STRING, creation_time TIMESTAMP, total_bytes_processed INT64, total_bytes_billed INT64,
  total_slot_ms INT64, duration_ms INT64, inserted_row_count INT64, updated_row_count INT64, deleted_row_count INT64)" >/dev/null
  q --label=kind:setup "MERGE \`$RDS.xops\` m USING (
SELECT '$RUN_ID' AS run_id,
  (SELECT value FROM UNNEST(labels) WHERE key = 'scenario') AS scenario,
  (SELECT value FROM UNNEST(labels) WHERE key = 'arm') AS arm,
  (SELECT value FROM UNNEST(labels) WHERE key = 'rep') AS rep,
  (SELECT value FROM UNNEST(labels) WHERE key = 'part') AS part,
  (SELECT value FROM UNNEST(labels) WHERE key = 'batch') AS batch,
  job_id, creation_time, total_bytes_processed, total_bytes_billed, total_slot_ms,
  TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
  dml_statistics.inserted_row_count, dml_statistics.updated_row_count, dml_statistics.deleted_row_count
FROM \`$REGION\`.INFORMATION_SCHEMA.$JOBS_VIEW
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AND state = 'DONE' AND error_result IS NULL
  AND EXISTS(SELECT 1 FROM UNNEST(labels) WHERE key = 'run' AND value = '$RUN_ID')
  AND EXISTS(SELECT 1 FROM UNNEST(labels) WHERE key = 'kind' AND value IN ('xop', 'xbuild'))
) s ON m.job_id = s.job_id WHEN NOT MATCHED THEN INSERT ROW" >/dev/null
  q --format=csv --max_rows=100000 "SELECT * EXCEPT (run_id) FROM \`$RDS.xops\` WHERE run_id = '$RUN_ID' ORDER BY creation_time" >"$OUT/xops.csv"
  python3 "$HERE/summarize.py" "$OUT/xops.csv" "$DRYRUN_CSV" "$OUT/xsummary.csv"
}

steps=("$@")
[ ${#steps[@]} -gt 0 ] || steps=(xsetup xkeymap xdeletes xsequence xverify xmoves xspread xadvisor xreport)
log "run $RUN_ID, anchor $ANCHOR, dataset $DS, $RPD rows per day"
for s in "${steps[@]}"; do
  log "step $s"
  "step_$s"
done
