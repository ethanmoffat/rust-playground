#!/usr/bin/env python3
"""Summarize scripts/ownership.sh and scripts/fencelife.sh output: one row per scenario directory.

Usage: scripts/own_sum.py <work-dir>...
Columns: G1 to G4 from check.json, file counts, distinct txn appIds, per-process ownership events
(fence, claim, acquire, lost, drain_done), drain time (claim to drain_done, max over drains), and for
routed scenarios the lag from each route switch to the new target's first commit.
"""
import collections, glob, json, os, sys

EVENTS = ("fence", "claim", "acquire", "lost", "drain_done")


def records(d):
    out = []
    for log in glob.glob(f"{d}/*.jsonl"):
        if log.endswith(".sent.jsonl") or log.endswith("opt.jsonl"):
            continue
        for line in open(log):
            if line.startswith("{"):
                e = json.loads(line)
                if "ts_ms" in e:
                    e["_proc"] = log
                    out.append(e)
    return sorted(out, key=lambda e: e["ts_ms"])


def drain_ms(recs):
    worst, start = None, {}
    for e in recs:
        if e.get("kind") == "claim":
            start[e["_proc"]] = e["ts_ms"]
        elif e.get("kind") == "drain_done" and e["_proc"] in start:
            ms = e["ts_ms"] - start[e["_proc"]]
            worst = ms if worst is None else max(worst, ms)
    return worst


def switch_lags(d, recs):
    path = f"{d}/route.log"
    if not os.path.exists(path):
        return ""
    sw = [(int(t), who) for t, who in (l.split() for l in open(path)) if who.startswith("inst-")]
    lags = []
    for t, who in sw[1:]:
        lags.append(next((e["ts_ms"] - t for e in recs if e["ts_ms"] >= t and e.get("kind") == "commit"
                          and e.get("app_id", "").startswith(who + "/")), None))
    f = sorted(x for x in lags if x is not None)
    if len(lags) > 3:
        return f"{len(lags)} switches, p50 {f[len(f) // 2] if f else '-'} max {max(f) if f else '-'}"
    return ", ".join(f"{w} +{a if a is not None else '-'}" for a, (_, w) in zip(lags, sw[1:]))


def events(path):
    counts = collections.Counter()
    for line in open(path):
        if not line.startswith("{"):
            continue
        kind = json.loads(line).get("kind")
        if kind in EVENTS:
            counts[kind] += 1
    return counts


print("| scenario | G1 | G2 | G3 | G4 | accepted | missing | dups | pending | appIds | drain ms | switch lag ms | events |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
for work in sys.argv[1:]:
    for d in sorted(glob.glob(f"{work}/*/check.json")):
        d = os.path.dirname(d)
        c = json.load(open(f"{d}/check.json"))
        recs = records(d)
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
              f"{len(c['txn_app_ids'])} | {drain_ms(recs) if drain_ms(recs) is not None else '-'} | "
              f"{switch_lags(d, recs)} | {' '.join(ev)} |")
