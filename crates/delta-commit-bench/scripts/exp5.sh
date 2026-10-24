#!/usr/bin/env bash
# Experiment 5 (reduced): N independent commit loops (separate processes, own appIds) on one table.
# Usage: exp5.sh <storage-account> [writer counts...]   (default: 2 5)
# Per-writer rates: RATES (default "1.2 12", the hot-table rate and 10x). Results under ~/results/exp5/<account>.
set -uo pipefail

ACCT=$1; shift
WRITERS=${*:-2 5}
RATES=${RATES:-1.2 12}
DUR=${DUR:-180}
RUN=exp5${TAG:+-$TAG}
BASE=abfss://bench@$ACCT.dfs.core.windows.net/$RUN
OUT=$HOME/results/$RUN/$ACCT
DCB=$HOME/dcb
mkdir -p "$OUT"

for n in $WRITERS; do for rate in $RATES; do
  name=w$n-r$rate; d=$OUT/$name; T=$BASE/$name
  [ -f "$d/check.json" ] && { echo "skip $name"; continue; }
  rm -rf "$d"; mkdir -p "$d"
  echo "$(date -u +%FT%TZ) start $name" | tee -a "$OUT/progress.log"
  "$DCB" create --table-url "$T" 2> "$d/create.log"
  pids=()
  for i in $(seq 1 "$n"); do
    mkdir -p "$d/w$i"
    "$DCB" run --table-url "$T" --app-id "$RUN-$name-w$i" --wal-dir "$d/wal" --out "$d/w$i/run.jsonl" \
      --duration "$DUR" --rate "$rate" --encoders 4 --pacing group --idempotency txn 2> "$d/w$i/stderr.log" &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; echo "exit=$?" >> "$d/exits.log"; done
  sent=(); for i in $(seq 1 "$n"); do sent+=(--sent "$d/w$i/run.sent.jsonl"); done
  "$DCB" check --table-url "$T" --wal-dir "$d/wal" "${sent[@]}" > "$d/check.json" 2>> "$d/create.log"
  echo "$(date -u +%FT%TZ) done $name" | tee -a "$OUT/progress.log"
done; done
echo "$(date -u +%FT%TZ) all done" | tee -a "$OUT/progress.log"
