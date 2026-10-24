#!/usr/bin/env bash
# Experiment 1: one commit loop, one table. Runs on a bench VM inside tmux; results under ~/results/exp1/<account>.
# Usage: exp1.sh <storage-account> [phase...]   phases: pacing scanlog ceiling loglen findings logcleanup
#   (default: all but logcleanup)
set -uo pipefail

ACCT=$1; shift
PHASES=${*:-pacing scanlog ceiling loglen findings}
DUR=${DUR:-180}
# TAG separates reruns: tables under exp1-<TAG>/, results under ~/results/exp1-<TAG>/.
RUN=exp1${TAG:+-$TAG}
BASE=abfss://bench@$ACCT.dfs.core.windows.net/$RUN
OUT=$HOME/results/$RUN/$ACCT
DCB=$HOME/dcb
mkdir -p "$OUT"

cell() { # cell <name> <run args...>
  local name=$1; shift
  local d=$OUT/$name
  [ -f "$d/check.json" ] && { echo "skip $name"; return; }
  mkdir -p "$d"; rm -rf "$d/wal"
  echo "$(date -u +%FT%TZ) start $name: $*" | tee -a "$OUT/progress.log"
  "$DCB" run --table-url "$BASE/$name" --create --app-id "$RUN-$name" --wal-dir "$d/wal" \
    --out "$d/run.jsonl" --duration "$DUR" "$@" 2> "$d/stderr.log"
  echo "exit=$?" >> "$d/stderr.log"
  "$DCB" check --table-url "$BASE/$name" --wal-dir "$d/wal" --sent "$d/run.sent.jsonl" > "$d/check.json" 2>> "$d/stderr.log"
  echo "$(date -u +%FT%TZ) done $name" | tee -a "$OUT/progress.log"
}

for phase in $PHASES; do case $phase in
  pacing)
    for rate in 1.2 3 6 12; do for pacing in group 1s 5s; do
      cell "pacing-$pacing-r$rate" --rate "$rate" --pacing "$pacing" --idempotency txn
    done; done ;;
  scanlog)
    for rate in 1.2 12; do
      cell "scanlog-group-r$rate" --rate "$rate" --pacing group --idempotency scan-log --library-max-retries 0
    done ;;
  ceiling)
    for rate in 25 50 100 200; do
      cell "ceiling-group-r$rate" --rate "$rate" --encoders 16 --pacing group --idempotency txn
    done
    cell "ceiling-1file-txn-r100" --rate 100 --encoders 16 --pacing group --idempotency txn --max-files-per-commit 1
    cell "ceiling-1file-scanlog-r100" --rate 100 --encoders 16 --pacing group --idempotency scan-log \
      --library-max-retries 0 --max-files-per-commit 1 ;;
  loglen)
    # Uncheckpointed logs of increasing length; time a fresh reader, write a checkpoint, time it again.
    for secs in 60 300 1200; do
      name=loglen-$secs; d=$OUT/$name
      DUR=$secs cell "$name" --rate 100 --encoders 16 --pacing group --idempotency txn --max-files-per-commit 1 --checkpoint off
      [ -f "$d/read-after.jsonl" ] && continue
      "$DCB" read --table-url "$BASE/$name" --repeat 5 > "$d/read-before.jsonl" 2>> "$d/stderr.log"
      "$DCB" checkpoint --table-url "$BASE/$name" > "$d/checkpoint.jsonl" 2>> "$d/stderr.log"
      "$DCB" read --table-url "$BASE/$name" --repeat 5 > "$d/read-after.jsonl" 2>> "$d/stderr.log"
    done ;;
  findings)
    # Checkpoint stall under a current-thread commit runtime, against the default multi-thread runtime.
    for rt in current multi; do
      DUR=60 cell "runtime-$rt-ck50" --rate 8 --pacing group --idempotency txn --checkpoint-every 50 --commit-runtime "$rt"
    done ;;
  logcleanup)
    # Commit latency against log length, with the post-commit expired-log cleanup (a full _delta_log LIST) on and off.
    for lc in on off; do
      cell "logcleanup-$lc-r25" --rate 25 --encoders 16 --pacing group --idempotency txn --log-cleanup "$lc"
    done ;;
  *) echo "unknown phase $phase" >&2; exit 2 ;;
esac; done
echo "$(date -u +%FT%TZ) all done" | tee -a "$OUT/progress.log"
