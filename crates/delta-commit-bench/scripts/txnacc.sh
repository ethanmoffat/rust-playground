#!/usr/bin/env bash
# Follow-up Q3: cost of accumulated txn appIds.
# Usage: scripts/txnacc.sh <table-base-url> <work-dir> [counts...]
# For each count: create a table, seed that many distinct appIds, checkpoint, then run one commit
# loop at RATE files/s for DUR s and time fresh reader loads. scripts/txnacc_sum.py summarizes.
set -uo pipefail
BASE=${1:?table base url}
WORK=${2:?work dir}
shift 2
COUNTS=${*:-0 10 100 1000 10000}
RATE=${RATE:-10}
DUR=${DUR:-60}
DCB=${DCB:-$(dirname "$0")/../../../target/release/dcb}
RUN_ID=${RUN_ID:-$(date +%s)}
mkdir -p "$WORK"
for n in $COUNTS; do
  D=$WORK/n$n; T=$BASE/$RUN_ID-n$n; mkdir -p "$D"
  "$DCB" create --table-url "$T" >/dev/null
  [ "$n" -gt 0 ] && "$DCB" seed-txn --table-url "$T" --count "$n" --per-commit 100 >"$D/seed.json"
  "$DCB" checkpoint --table-url "$T" >"$D/ckpt0.json"
  "$DCB" read --table-url "$T" --repeat 5 >"$D/read0.jsonl"
  "$DCB" run --table-url "$T" --app-id inst-a --wal-dir "$D/wal" --rate "$RATE" --encoders 2 --duration "$DUR" \
    --sample-secs 2 --out "$D/run.jsonl" 2>"$D/run.err"
  "$DCB" checkpoint --table-url "$T" >"$D/ckpt1.json"
  "$DCB" read --table-url "$T" --repeat 5 >"$D/read1.jsonl"
  echo "n=$n done"
done
