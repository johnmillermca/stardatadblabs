#!/usr/bin/env bash
# =============================================================================
# tableflow-streams.sh
# ====================
# Idempotent ksqlDB setup — creates the Kafka-native TableFlow derived streams
# for all 3 CDC sources (postgres, oracle, mongodb) × 6 tables each.
#
# For every source table this creates TWO derived topics:
#
#   <source>.cache_testing.<table>_sd
#       Soft-delete view.  Partitioned by PK.
#       INSERT / UPDATE → full row with IS_DELETED=false, DELETED_AT=null
#       DELETE          → row with IS_DELETED=true,  DELETED_AT=<event_ts>
#       The tableflow Python pod reads this topic and does UPSERT into Iceberg.
#
#   <source>.cache_testing.<table>_hist
#       History-tracking (append-only).
#       Every CDC event produces one row: BEFORE_* + AFTER_* + CDC_OP + CDC_TS.
#       The tableflow Python pod reads this topic and does APPEND into Iceberg.
#
# Usage:
#   ./tableflow-streams.sh           # create everything
#   ./tableflow-streams.sh --drop    # drop all derived streams + topics
#
# Requirements:
#   curl available, ksqlDB reachable at KSQL_URL (default http://192.168.1.50:30088)
# =============================================================================

set -euo pipefail

KSQL_URL="${KSQL_URL:-http://192.168.1.50:30088}"
DROP_MODE=false
[[ "${1:-}" == "--drop" ]] && DROP_MODE=true

# ── helpers ──────────────────────────────────────────────────────────────────
ksql() {
  local stmt="$1"
  local payload
  payload=$(printf '{"ksql":%s,"streamsProperties":{}}' "$(echo "$stmt" | python3 -c "import sys,json; print(json.dumps(sys.stdin.read()))")")
  local resp
  resp=$(curl -sf -X POST "${KSQL_URL}/ksql" \
    -H "Content-Type: application/vnd.ksql.v1+json" \
    -d "$payload" 2>&1) || { echo "  FAIL: $resp"; return 1; }
  local status
  status=$(echo "$resp" | python3 -c "
import sys,json
try:
  d=json.load(sys.stdin)
  if isinstance(d,list): print(d[0].get('commandStatus',{}).get('status','OK'))
  else: print(d.get('message','OK')[:80])
except: print('PARSE_ERR')
" 2>/dev/null)
  echo "  $status"
}

# ── sources: (prefix, stream_prefix, pk_field) ──────────────────────────────
# oracle uses uppercase field names in its Debezium JSON
declare -A SRC_STREAM_PREFIX=( [postgres]="POSTGRES" [oracle]="ORACLE" [mongodb]="MONGODB" )
declare -A SRC_TOPIC_PREFIX=(  [postgres]="postgres" [oracle]="oracle"  [mongodb]="mongodb"  )
declare -A SRC_NAMESPACE=(     [postgres]="cache_testing" [oracle]="CACHE_TESTING" [mongodb]="cache_testing" )
declare -A SRC_PK=(            [postgres]="id" [oracle]="ID" [mongodb]="_id" )

TABLES=( customers orders products order_items inventory_events product_reviews )

# =============================================================================
if [[ "$DROP_MODE" == "true" ]]; then
  echo "=== DROP MODE ==="
  for src in postgres oracle mongodb; do
    NS="${SRC_NAMESPACE[$src]}"
    for tbl in "${TABLES[@]}"; do
      TBL_UP="${tbl^^}"
      for suffix in SD HIST; do
        SNAME="${SRC_STREAM_PREFIX[$src]}_${TBL_UP}_${suffix}"
        echo -n "DROP STREAM ${SNAME} ..."
        ksql "DROP STREAM IF EXISTS ${SNAME} DELETE TOPIC;"
      done
    done
  done
  echo "=== Done dropping ==="
  exit 0
fi

# =============================================================================
echo "=== Creating TableFlow derived streams ==="

for src in postgres oracle mongodb; do
  SP="${SRC_STREAM_PREFIX[$src]}"
  TP="${SRC_TOPIC_PREFIX[$src]}"
  NS="${SRC_NAMESPACE[$src]}"
  PK="${SRC_PK[$src]}"

  for tbl in "${TABLES[@]}"; do
    TBL_UP="${tbl^^}"
    SRC_STREAM="${SP}_${NS^^}_${TBL_UP}"   # e.g. POSTGRES_CACHE_TESTING_CUSTOMERS
    # Normalise: oracle uses CACHE_TESTING in stream names
    if [[ "$src" == "oracle" ]]; then
      SRC_STREAM="${SP}_CACHE_TESTING_${TBL_UP}"
    fi

    SD_STREAM="${SP}_${TBL_UP}_SD"
    HIST_STREAM="${SP}_${TBL_UP}_HIST"
    SD_TOPIC="${TP}.cache_testing.${tbl}_sd"
    HIST_TOPIC="${TP}.cache_testing.${tbl}_hist"

    # ── For oracle uppercase fields we read $.after.ID, for others $.after.id
    if [[ "$src" == "oracle" ]]; then
      PK_AFTER="$.after.ID"
      PK_BEFORE="$.before.ID"
    elif [[ "$src" == "mongodb" ]]; then
      PK_AFTER='$.after._id'
      PK_BEFORE='$.before._id'
    else
      PK_AFTER="$.after.id"
      PK_BEFORE="$.before.id"
    fi

    echo ""
    echo "--- ${src} / ${tbl} ---"

    # ── SD stream (soft-delete upsert view) ──────────────────────────────────
    echo -n "  CREATE SD stream ${SD_STREAM} ..."
    ksql "CREATE STREAM IF NOT EXISTS ${SD_STREAM}
  WITH (
    KAFKA_TOPIC='${SD_TOPIC}',
    PARTITIONS=3,
    REPLICAS=1,
    VALUE_FORMAT='JSON',
    CLEANUP_POLICY='compact,delete',
    RETENTION_MS=604800000
  ) AS SELECT
    CASE
      WHEN EXTRACTJSONFIELD(PAYLOAD,'$.op') = 'd'
        THEN EXTRACTJSONFIELD(PAYLOAD,'${PK_BEFORE}')
      ELSE EXTRACTJSONFIELD(PAYLOAD,'${PK_AFTER}')
    END                                                       AS PK_KEY,
    EXTRACTJSONFIELD(PAYLOAD,'${PK_AFTER}')                   AS PK_ID,
    EXTRACTJSONFIELD(PAYLOAD,'$.after')                       AS AFTER_JSON,
    EXTRACTJSONFIELD(PAYLOAD,'$.before')                      AS BEFORE_JSON,
    CASE
      WHEN EXTRACTJSONFIELD(PAYLOAD,'$.op') = 'd' THEN 'true'
      ELSE 'false'
    END                                                       AS IS_DELETED,
    CASE
      WHEN EXTRACTJSONFIELD(PAYLOAD,'$.op') = 'd'
        THEN CAST(ROWTIME AS VARCHAR)
      ELSE NULL
    END                                                       AS DELETED_AT,
    EXTRACTJSONFIELD(PAYLOAD,'$.op')                          AS CDC_OP,
    CAST(ROWTIME AS BIGINT)                                   AS CDC_TS
  FROM ${SRC_STREAM}
  WHERE EXTRACTJSONFIELD(PAYLOAD,'$.op') IS NOT NULL
  PARTITION BY CASE
      WHEN EXTRACTJSONFIELD(PAYLOAD,'$.op') = 'd'
        THEN EXTRACTJSONFIELD(PAYLOAD,'${PK_BEFORE}')
      ELSE EXTRACTJSONFIELD(PAYLOAD,'${PK_AFTER}')
    END
  EMIT CHANGES;"

    # ── HIST stream (append-only history) ────────────────────────────────────
    echo -n "  CREATE HIST stream ${HIST_STREAM} ..."
    ksql "CREATE STREAM IF NOT EXISTS ${HIST_STREAM}
  WITH (
    KAFKA_TOPIC='${HIST_TOPIC}',
    PARTITIONS=3,
    REPLICAS=1,
    VALUE_FORMAT='JSON',
    CLEANUP_POLICY='delete',
    RETENTION_MS=2592000000
  ) AS SELECT
    EXTRACTJSONFIELD(PAYLOAD,'${PK_AFTER}')                   AS PK_ID,
    EXTRACTJSONFIELD(PAYLOAD,'$.after')                       AS AFTER_JSON,
    EXTRACTJSONFIELD(PAYLOAD,'$.before')                      AS BEFORE_JSON,
    CASE
      WHEN EXTRACTJSONFIELD(PAYLOAD,'$.op') = 'c' THEN 'INSERT'
      WHEN EXTRACTJSONFIELD(PAYLOAD,'$.op') = 'u' THEN 'UPDATE'
      WHEN EXTRACTJSONFIELD(PAYLOAD,'$.op') = 'd' THEN 'DELETE'
      WHEN EXTRACTJSONFIELD(PAYLOAD,'$.op') = 'r' THEN 'READ'
      ELSE EXTRACTJSONFIELD(PAYLOAD,'$.op')
    END                                                       AS CDC_OP,
    CAST(ROWTIME AS BIGINT)                                   AS CDC_TS
  FROM ${SRC_STREAM}
  WHERE EXTRACTJSONFIELD(PAYLOAD,'$.op') IS NOT NULL
  EMIT CHANGES;"

  done
done

echo ""
echo "=== All TableFlow streams created ==="
echo ""
echo "Derived topics produced:"
for src in postgres oracle mongodb; do
  TP="${SRC_TOPIC_PREFIX[$src]}"
  for tbl in "${TABLES[@]}"; do
    echo "  ${TP}.cache_testing.${tbl}_sd"
    echo "  ${TP}.cache_testing.${tbl}_hist"
  done
done
