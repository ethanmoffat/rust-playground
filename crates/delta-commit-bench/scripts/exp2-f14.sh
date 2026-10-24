#!/usr/bin/env bash
# Finding F14 follow-up: blocking-pool deadlock fixes under exp 2 load, one account per VM.
# Usage: exp2-f14.sh <storage-account> [table counts...]   (default: 1000 5000)
# Variants (TAG = EXEC:MAX_OPS:MAX_CKPT:JITTER_MS):
#   base     worker checkpoint, no caps (expected to deadlock; the watchdog ends it)
#   blk      checkpoint in spawn_blocking, no caps
#   cap32/64/128  worker checkpoint, commit cap N, checkpoint cap 8
#   cap64blk      cap 64 + spawn_blocking checkpoint
#   cap64blkj     the same, starts spread over one checkpoint period (CKPT_EVERY / RATE)
set -uo pipefail
ACCT=$1; shift
COUNTS=${*:-1000 5000}
export DUR=${DUR:-600} RATE=${RATE:-0.2} CKPT_EVERY=${CKPT_EVERY:-50} STALL=${STALL:-300}
export WAL_ROOT=${WAL_ROOT:-/dev/shm/wal}
period_ms=$(python3 -c "print(int($CKPT_EVERY / $RATE * 1000))")
for v in base:worker:0:0:0 blk:blocking:0:0:0 cap32:worker:32:8:0 cap64:worker:64:8:0 cap128:worker:128:8:0 \
         cap64blk:blocking:64:8:0 "cap64blkj:blocking:64:8:$period_ms"; do
  IFS=: read -r tag exec ops ckpt jitter <<< "$v"
  TAG=f14-$tag EXEC=$exec MAX_OPS=$ops MAX_CKPT=$ckpt JITTER_MS=$jitter "$(dirname "$0")/exp2.sh" "$ACCT" $COUNTS
done
