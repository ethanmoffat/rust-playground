#!/usr/bin/env bash
# Experiment 3: one table's ceiling on one instance. Arrow batches in memory, Parquet (ZSTD 1) encoded on
# N dedicated encoder threads, streamed to storage as multipart uploads, one commit loop.
# Runs on a bench VM; results under ~/results/exp3/<account>/enc-<N>.
# Usage: exp3.sh <storage-account> [encoder counts...]   (default: 4 8 12 16)
# Leave a couple of cores for the commit loop and the uploader runtime (e.g. 16 vCPU: up to 14).
# Env: DUR (120), FILE_MB (1024), RG_MB (256), PART_MB (32), UPLOAD_CONC (4 parts per encoder), POOL_MB (1024).
set -uo pipefail

ACCT=$1; shift
COUNTS=${*:-4 8 12 16}
DUR=${DUR:-120}
FILE_MB=${FILE_MB:-1024}
RG_MB=${RG_MB:-256}
PART_MB=${PART_MB:-32}
UPLOAD_CONC=${UPLOAD_CONC:-4}
POOL_MB=${POOL_MB:-1024}
RUN=exp3${TAG:+-$TAG}
BASE=abfss://bench@$ACCT.dfs.core.windows.net/$RUN
OUT=$HOME/results/$RUN/$ACCT
DCB=$HOME/dcb
mkdir -p "$OUT"

for n in $COUNTS; do
  name=enc-$n; d=$OUT/$name
  [ -f "$d/check.json" ] && { echo "skip $name"; continue; }
  mkdir -p "$d"; rm -rf "$d/wal"
  nproc > "$d/nproc.txt"
  curl -s -m 5 -H Metadata:true \
    "http://169.254.169.254/metadata/instance/compute/vmSize?api-version=2021-02-01&format=text" > "$d/vmsize.txt"
  echo "$(date -u +%FT%TZ) start $name ($(cat "$d/vmsize.txt"), $(nproc) vCPU)" | tee -a "$OUT/progress.log"
  # object_store logs each internal retry (throttling included) at info.
  RUST_LOG=warn,object_store=info "$DCB" run --table-url "$BASE/$name" --create --app-id "$RUN-$name" \
    --wal-dir "$d/wal" --out "$d/run.jsonl" --duration "$DUR" --producer arrow --encoders "$n" --rate 0 \
    --file-mb "$FILE_MB" --row-group-mb "$RG_MB" --part-mb "$PART_MB" --upload-concurrency "$UPLOAD_CONC" \
    --pool-mb "$POOL_MB" --pacing group --idempotency txn --checkpoint-every 100 2> "$d/stderr.log"
  echo "exit=$?" >> "$d/stderr.log"
  "$DCB" check --table-url "$BASE/$name" --wal-dir "$d/wal" --sent "$d/run.sent.jsonl" > "$d/check.json" 2>> "$d/stderr.log"
  echo "$(date -u +%FT%TZ) done $name" | tee -a "$OUT/progress.log"
done
echo "$(date -u +%FT%TZ) all done" | tee -a "$OUT/progress.log"
