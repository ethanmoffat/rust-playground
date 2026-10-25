#!/usr/bin/env python3
"""Summarize scripts/ownership.sh and scripts/fencelife.sh output: one row per scenario directory.

Usage: scripts/own_sum.py <work-dir>...
Columns: G1 to G4 from check.json, file counts, distinct txn appIds, and per-process ownership
events (fence, claim, acquire, lost, drain_done) from the run logs.
"""
import collections, glob, json, os, sys

EVENTS = ("fence", "claim", "acquire", "lost", "drain_done")


def events(path):
    counts = collections.Counter()
    for line in open(path):
        if not line.startswith("{"):
            continue
        kind = json.loads(line).get("kind")
        if kind in EVENTS:
            counts[kind] += 1
    return counts


print("| scenario | G1 | G2 | G3 | G4 | accepted | missing | dups | pending | appIds | events |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
for work in sys.argv[1:]:
    for d in sorted(glob.glob(f"{work}/*/check.json")):
        d = os.path.dirname(d)
        c = json.load(open(f"{d}/check.json"))
        ev = []
        for log in sorted(glob.glob(f"{d}/*.jsonl")):
            if log.endswith(".sent.jsonl") or log.endswith("opt.jsonl"):
                continue
            counts = events(log)
            if counts:
                proc = os.path.basename(log).removesuffix(".jsonl")
                ev.append(proc + ":" + ",".join(f"{k}={counts[k]}" for k in EVENTS if counts[k]))
        g = ["pass" if c[k] else "FAIL" for k in ("g1_no_loss", "g2_no_duplicates", "g3_atomic", "g4_valid")]
        print(f"| {os.path.basename(d)} | {' | '.join(g)} | {c['accepted_files']} | "
              f"{c['missing_accepted_count']} | {c['duplicate_paths_count']} | {c['manifests_pending']} | "
              f"{len(c['txn_app_ids'])} | {' '.join(ev)} |")
