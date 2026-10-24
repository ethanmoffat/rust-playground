#!/usr/bin/env bash
# Experiment 2: many tables on one committer core. Runs on a bench VM; results under ~/results/exp2/<account>.
# Usage: exp2.sh <storage-account> [table counts...]   (default: 1 100 1000 5000)
# Each table gets one encoder at RATE files/s. The commit runtime has one worker thread; its blocking
# pool (used by delta-rs for snapshot work) shares the "commit" thread name, so commit CPU can exceed 1 core.
set -uo pipefail

ACCT=$1; shift
COUNTS=${*:-1 100 1000 5000}
DUR=${DUR:-300}
RATE=${RATE:-0.2}
RUN=exp2${TAG:+-$TAG}
BASE=abfss://bench@$ACCT.dfs.core.windows.net/$RUN
OUT=$HOME/results/$RUN/$ACCT
DCB=$HOME/dcb
mkdir -p "$OUT"

for n in $COUNTS; do
  name=tables-$n; d=$OUT/$name
  [ -f "$d/check.json" ] && { echo "skip $name"; continue; }
  mkdir -p "$d"; rm -rf "$d/wal"
  echo "$(date -u +%FT%TZ) start $name" | tee -a "$OUT/progress.log"
  # object_store logs each internal retry (throttling included) at info.
  RUST_LOG=warn,object_store=info "$DCB" run --table-base "$BASE/$name" --tables "$n" --create --app-id "$RUN" \
    --wal-dir "$d/wal" --out "$d/run.jsonl" --duration "$DUR" --rate "$RATE" --encoders 1 --pacing group \
    --idempotency txn --commit-threads 1 --checkpoint-every 50 2> "$d/stderr.log"
  echo "exit=$?" >> "$d/stderr.log"
  # Guarantees on a sample of tables: first, middle, last.
  for i in 0 $((n / 2)) $((n - 1)); do
    t=$(printf 't%05d' "$i")
    "$DCB" check --table-url "$BASE/$name/$t" --wal-dir "$d/wal/${RUN}_$t" > "$d/check-$t.json" 2>> "$d/stderr.log"
  done
  python3 - "$d" > "$d/check.json" <<'PY'
import json,glob,sys
c=[json.load(open(f)) for f in sorted(glob.glob(sys.argv[1]+"/check-t*.json"))]
print(json.dumps({k:all(x[k] for x in c) for k in ("g1_no_loss","g2_no_duplicates","g3_atomic","g4_valid")}|{"tables_checked":len(c)}))
PY
  echo "$(date -u +%FT%TZ) done $name" | tee -a "$OUT/progress.log"
done
echo "$(date -u +%FT%TZ) all done" | tee -a "$OUT/progress.log"
