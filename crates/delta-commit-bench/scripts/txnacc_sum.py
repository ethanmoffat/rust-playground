#!/usr/bin/env python3
"""Summarize scripts/txnacc.sh output: one row per seeded appId count."""
import glob, json, os, statistics, sys

work = sys.argv[1]


def lines(path):
    return [json.loads(l) for l in open(path)] if os.path.exists(path) else []


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))] if xs else float("nan")


print("| appIds | commit p50 ms | commit p99 ms | checkpoint ms (loop, p50) | checkpoint bytes | "
      "reader load ms (p50) | writer RSS MB (max) | commits |")
print("|---|---|---|---|---|---|---|---|")
for d in sorted(glob.glob(f"{work}/n*"), key=lambda d: int(d.rsplit("n", 1)[1])):
    n = int(d.rsplit("n", 1)[1])
    run = lines(f"{d}/run.jsonl")
    commits = [r for r in run if r.get("kind") == "commit" and r.get("outcome") == "ok"]
    cms = [r["commit_ms"] for r in commits]
    ck = [r["checkpoint_ms"] for r in run if r.get("checkpoint_ms") is not None]
    rss = [r["rss_mb"] for r in run if r.get("kind") == "sample"]
    ckpt = json.load(open(f"{d}/ckpt1.json"))
    loads = [r["load_ms"] for r in lines(f"{d}/read1.jsonl")[1:]]
    print(f"| {n} | {pct(cms, 50):.0f} | {pct(cms, 99):.0f} | {statistics.median(ck) if ck else float('nan'):.0f} | "
          f"{ckpt['bytes']} | {statistics.median(loads):.0f} | {max(rss):.0f} | {len(commits)} |")
