#!/usr/bin/env python3
"""Summarize Q4 exp2 cells: scripts/q4sum.py <results-root>, reading <root>/exp2-q4-<tag>/<acct>/tables-<n>.

Disk columns come from /proc/diskstats deltas for the WAL device over the run: utilization (io ticks over
elapsed time), writes/s, MB/s written, and mean write await. Sync columns (group mode only) are the
median of per-sample p50s and the max of per-sample p99s.
"""
import csv, glob, json, os, statistics, sys


def pct(v, p):
    v = sorted(v)
    return v[min(len(v) - 1, int(p / 100 * len(v)))] if v else None


rows = []
for d in sorted(glob.glob(sys.argv[1] + "/exp2-q4-*/*/tables-*")):
    if not os.path.exists(d + "/check.json"):
        continue
    tag = d.split("exp2-q4-")[1].split("/")[0]
    n = int(d.split("tables-")[1])
    c, s, end = [], [], None
    for l in open(d + "/run.jsonl"):
        if '"kind":"commit"' in l:
            c.append(json.loads(l))
        elif '"kind":"sample"' in l:
            s.append(json.loads(l))
        elif '"kind":"end"' in l:
            end = json.loads(l)
    if not c:
        continue
    chk = json.load(open(d + "/check.json"))
    t0, t1 = min(x["ts_ms"] for x in c), max(x["ts_ms"] for x in c)
    cm = [x["commit_ms"] for x in c]
    vis = [x["visible_oldest_ms"] for x in c]
    row = dict(tag=tag, n=n,
               ok=all(chk.get(k, False) for k in ("g1_no_loss", "g2_no_duplicates", "g3_atomic", "g4_valid")),
               stalled=chk.get("stalled", False), commits=len(c), files=sum(x["files"] for x in c),
               cps=round(len(c) / max(1, (t1 - t0) / 1000), 1),
               commit_p50=round(pct(cm, 50)), commit_p99=round(pct(cm, 99)),
               vis_p50=round(pct(vis, 50)), vis_p99=round(pct(vis, 99)),
               cpu_commit=round((end or s[-1])["cpu_secs"].get("commit", 0)))
    ws = [x["wal_sync"] for x in s if x.get("wal_sync")]
    if ws:
        dt = (s[-1]["ts_ms"] - s[0]["ts_ms"]) / 1000
        syncs = ws[-1]["syncs"] - ws[0]["syncs"]
        row.update(sync_p50=round(statistics.median(w["p50_ms"] for w in ws if w["p50_ms"] == w["p50_ms"]), 2),
                   sync_p99=round(max(w["p99_ms"] for w in ws if w["p99_ms"] == w["p99_ms"]), 2),
                   syncs_s=round(syncs / dt, 1),
                   manifests_per_sync=round((ws[-1]["manifests"] - ws[0]["manifests"]) / max(1, syncs), 1))
    dk = [x["wal_disk"] for x in s if x.get("wal_disk")]
    if len(dk) > 1:
        dt = (s[-1]["ts_ms"] - s[0]["ts_ms"]) / 1000
        a, b = dk[0], dk[-1]
        w = b["writes"] - a["writes"]
        row.update(device=b["device"], disk_util_pct=round(100 * (b["io_ticks_ms"] - a["io_ticks_ms"]) / 1000 / dt, 1),
                   writes_s=round(w / dt), mb_s=round((b["sectors_written"] - a["sectors_written"]) * 512 / 1e6 / dt, 1),
                   write_await_ms=round((b["write_ticks_ms"] - a["write_ticks_ms"]) / max(1, w), 2))
    rows.append(row)
keys = list(dict.fromkeys(k for r in rows for k in r))
w = csv.DictWriter(sys.stdout, fieldnames=keys)
w.writeheader()
for r in rows:
    w.writerow(r)
