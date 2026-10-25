#!/usr/bin/env bash
# Follow-up Q3: fence lifetime. For each way a txn entry can disappear, does a fenced or stale
# owner commit again, and does that duplicate data?
# Usage: scripts/fencelife.sh <table-base-url> <work-dir> [scenario...]
#   EPOCHS=off|tenure (default tenure). RET=retention seconds for the retention cases (default 10).
#   Scenarios: inproc-ret restart-ret restart-cleanup restart-cleanup-ret restart-optimize
#   restart-ret-optimize restart-ret-vacuum recreate.
# The stale owner X always crashes right after a log write (manifest still pending), then Y fences X.
set -uo pipefail
BASE=${1:?table base url}
WORK=${2:?work dir}
shift 2
SCEN=${*:-inproc-ret restart-ret restart-cleanup restart-cleanup-ret restart-optimize restart-ret-optimize restart-ret-vacuum recreate}
EPOCHS=${EPOCHS:-tenure}
RET=${RET:-10}
DCB=${DCB:-$(dirname "$0")/../../../target/release/dcb}
RUN_ID=${RUN_ID:-$(date +%s)}
mkdir -p "$WORK"

scenario() { N=q3-$1-$EPOCHS; D=$WORK/$N; T=$BASE/$RUN_ID-$N; mkdir -p "$D"; }
# In a background subshell, exec so that $! is the dcb process and SIGSTOP pauses it.
x() { if [ "$BASHPID" != "$$" ]; then exec "$@"; else "$@"; fi; }
dcb() { x "$DCB" run --epochs "$EPOCHS" --checkpoint-every 1000 --encoders 2 "$@"; }
RETP=(--prop "delta.setTransactionRetentionDuration=interval $RET seconds")
# appIds X has used: the plain owner appId, plus the epoch appIds and counter in tenure mode.
xids() { local b=inst-a/$RUN_ID-$N; echo --app-id "$b" --app-id "$b#e1" --app-id "$b#e2" --app-id "$b#epoch"; }
probe() { "$DCB" txn-probe --table-url "$T" $(xids) | sed "s/^/$1 /" >>"$D/probe.log"; }
crash_x() { # crashed stale owner with a manifest whose commit landed
  dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" --rate 10 --duration 30 \
    --fault-crash-after-log-put 20 --out "$D/x1.jsonl" "$@" 2>"$D/x1.err"
}
take_y() { # Y takes over and fences X
  dcb --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --fence-identity inst-a --rate 10 --duration "${1:-5}" \
    --out "$D/y.jsonl" 2>"$D/y.err"
}
restart_x() { # X restarts with its WAL and no memory of the takeover
  if [ "$EPOCHS" = off ]; then
    dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --rate 1 --duration 3 --out "$D/x2.jsonl" 2>"$D/x2.err"
  else
    dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --out "$D/x2.jsonl" 2>"$D/x2.err"
  fi
}
check() {
  "$DCB" check --table-url "$T" --wal-dir "$D/wal-a" --wal-dir "$D/wal-b" $(ls "$D"/*.sent.jsonl | sed 's/^/--sent /') \
    >"$D/check.json" 2>"$D/check.err"
  python3 -c "
import json; c=json.load(open('$D/check.json'))
print(f'$N'.ljust(32), *(f'{k}={c[k]!s:5}' for k in ('g1_no_loss','g2_no_duplicates','g3_atomic','g4_valid')),
      f'accepted={c[\"accepted_files\"]} missing={c[\"missing_accepted_count\"]} dups={c[\"duplicate_paths_count\"]} pending={c[\"manifests_pending\"]}')"
}

for s in $SCEN; do case $s in
inproc-ret)
  # X paused (not crashed) with its in-memory last version; Y fences; retention expires; X resumes.
  scenario $s
  dcb --table-url "$T" --create "${RETP[@]}" --app-id inst-a --wal-dir "$D/wal-a" --rate 10 --duration 40 \
    --out "$D/x1.jsonl" 2>"$D/x1.err" &
  X=$!; sleep 8; kill -STOP $X
  take_y 5; probe fenced; sleep $((RET + 5)); probe expired
  kill -CONT $X; wait $X; probe end; check ;;
restart-ret)
  scenario $s
  crash_x "${RETP[@]}"; take_y 5; probe fenced; sleep $((RET + 5)); probe expired
  restart_x; probe end; check ;;
restart-cleanup|restart-cleanup-ret)
  # Checkpoint and delete all earlier log files; with and without retention.
  scenario $s
  if [ $s = restart-cleanup-ret ]; then crash_x "${RETP[@]}"; else crash_x; fi
  take_y 5; probe fenced
  [ $s = restart-cleanup-ret ] && sleep $((RET + 5))
  "$DCB" cleanup-log --table-url "$T" >"$D/cleanup.json"; probe cleaned
  restart_x; probe end; check ;;
restart-optimize)
  # Compaction commits (remove N, add 1) plus a checkpoint between the fence and the restart.
  scenario $s
  crash_x; take_y 5; probe fenced
  "$DCB" optimize --table-url "$T" --files 5 --every-ms 500 --duration 5 --out "$D/opt.jsonl"
  "$DCB" checkpoint --table-url "$T" >"$D/ckpt.json"; probe optimized
  restart_x; probe end; check ;;
restart-ret-optimize|restart-ret-vacuum)
  # Retention expires and compaction removes X's last batch before X restarts and re-adds it.
  # The vacuum variant also deletes the removed files.
  scenario $s
  crash_x "${RETP[@]}"; take_y 5; probe fenced
  "$DCB" optimize --table-url "$T" --files 200 --every-ms 200 --duration 2 --out "$D/opt.jsonl"
  [ $s = restart-ret-vacuum ] && "$DCB" vacuum --table-url "$T" >"$D/vacuum.json"
  sleep $((RET + 5)); probe expired
  restart_x; probe end; check
  "$DCB" read --table-url "$T" --repeat 1 >"$D/read.json" 2>"$D/read.err"; echo "$N read: $(head -c 300 "$D/read.json") $(tail -n 1 "$D/read.err" | cut -c1-200)" ;;
recreate)
  # The table is deleted and recreated at the same path (local file:// only).
  scenario $s
  crash_x; take_y 5; probe fenced
  rm -rf "${T#file://}/_delta_log"
  "$DCB" create --table-url "$T" >/dev/null; probe recreated
  restart_x; probe end
  echo "$N" "$(grep -c '"outcome":"ok"' "$D/x2.jsonl") ok commits by restarted X into the new table; files now visible:" \
    "$("$DCB" read --table-url "$T" --repeat 1 | python3 -c 'import json,sys; print(json.loads(sys.stdin.readline())["files"])')" ;;
*) echo "unknown scenario $s" >&2 ;;
esac; done
