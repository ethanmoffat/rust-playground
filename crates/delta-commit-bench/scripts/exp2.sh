#!/usr/bin/env bash
# Experiment 2: many tables on one committer core. Runs on a bench VM; results under ~/results/exp2/<account>.
# Usage: exp2.sh <storage-account> [table counts...]   (default: 1 100 1000 5000)
# Each table gets one encoder at RATE files/s. The commit runtime has one worker thread; its blocking
# pool (used by delta-rs for snapshot work) shares the "commit" thread name, so commit CPU can exceed 1 core.
set -uo pipefail
# THREADS (1): commit runtime worker threads.
# WAL_ROOT: where per-cell WALs go (default: the cell dir). Use /dev/shm to take WAL fsyncs out of the
# measurement (finding F13); the WAL is removed after the check.
# WAL_ARGS: extra WAL flags, for example "--wal-sync group --group-sync-ms 2 --data-wal-mbps 20".
# F14 knobs: EXEC (worker|blocking) checkpoint placement, MAX_OPS and MAX_CKPT caps (0 = none),
# JITTER_MS producer start spread, CKPT_EVERY (50).
# STALL (300): seconds without a new commit record before the cell is declared deadlocked; a gdb
# dump of all threads is saved and the process killed.
# Each table holds WAL files and connections open; 5000 tables exceed the default 1024 fds.
ulimit -n 65536

ACCT=$1; shift
COUNTS=${*:-1 100 1000 5000}
DUR=${DUR:-300}
RATE=${RATE:-0.2}
RUN=exp2${TAG:+-$TAG}
BASE=${BASE:-abfss://bench@$ACCT.dfs.core.windows.net}/$RUN
OUT=$HOME/results/$RUN/$ACCT
DCB=${DCB:-$HOME/dcb}
mkdir -p "$OUT"

for n in $COUNTS; do
  name=tables-$n; d=$OUT/$name
  [ -f "$d/check.json" ] && { echo "skip $name"; continue; }
  w=${WAL_ROOT:+$WAL_ROOT/$RUN/$ACCT/$name}; w=${w:-$d/wal}
  mkdir -p "$d"; rm -rf "$w"; echo "$w" > "$d/wal_dir.txt"
  echo "$(date -u +%FT%TZ) start $name" | tee -a "$OUT/progress.log"
  # object_store logs each internal retry (throttling included) at info.
  RUST_LOG=warn,object_store=info "$DCB" run --table-base "$BASE/$name" --tables "$n" --create --app-id "$RUN" \
    --wal-dir "$w" --out "$d/run.jsonl" --duration "$DUR" --rate "$RATE" --encoders 1 --pacing group \
    --idempotency txn --commit-threads "${THREADS:-1}" --checkpoint-every "${CKPT_EVERY:-50}" \
    --checkpoint-exec "${EXEC:-worker}" --max-ops "${MAX_OPS:-0}" --max-checkpoints "${MAX_CKPT:-0}" \
    --start-jitter-ms "${JITTER_MS:-0}" ${WAL_ARGS:-} 2> "$d/stderr.log" &
  pid=$!; last=-1; since=$(date +%s); stalled=0
  while kill -0 "$pid" 2>/dev/null; do
    sleep 15
    c=$(grep -c '"kind":"commit"' "$d/run.jsonl" 2>/dev/null); c=${c:-0}
    if [ "$c" != "$last" ]; then last=$c; since=$(date +%s)
    elif [ $(( $(date +%s) - since )) -ge "${STALL:-300}" ]; then
      echo "$(date -u +%FT%TZ) stall $name: no commit record for ${STALL:-300}s at $c records" | tee -a "$OUT/progress.log"
      sudo -n gdb -p "$pid" -batch -ex "thread apply all bt" 2>/dev/null | gzip > "$d/stall-gdb.txt.gz"
      kill -9 "$pid"; stalled=1; break
    fi
  done
  wait "$pid"; echo "exit=$?" >> "$d/stderr.log"
  if [ "$stalled" = 1 ]; then
    echo '{"stalled":true}' > "$d/check.json"
    [ -n "${WAL_ROOT:-}" ] && rm -rf "$w"
    continue
  fi
  # Guarantees on a sample of tables: first, middle, last.
  for i in 0 $((n / 2)) $((n - 1)); do
    t=$(printf 't%05d' "$i")
    "$DCB" check --table-url "$BASE/$name/$t" --wal-dir "$w/${RUN}_$t" > "$d/check-$t.json" 2>> "$d/stderr.log"
  done
  python3 - "$d" > "$d/check.json" <<'PY'
import json,glob,sys
c=[json.load(open(f)) for f in sorted(glob.glob(sys.argv[1]+"/check-t*.json"))]
print(json.dumps({k:all(x[k] for x in c) for k in ("g1_no_loss","g2_no_duplicates","g3_atomic","g4_valid")}|{"tables_checked":len(c)}))
PY
  [ -n "${WAL_ROOT:-}" ] && rm -rf "$w"
  echo "$(date -u +%FT%TZ) done $name" | tee -a "$OUT/progress.log"
done
echo "$(date -u +%FT%TZ) all done" | tee -a "$OUT/progress.log"
