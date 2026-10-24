#!/usr/bin/env python3
"""Summarize experiment cells: one row per cell directory containing run.jsonl and check.json.

Usage: summarize.py <results-dir> [--csv]
"""
import json
import os
import sys


def pct(values, p):
    if not values:
        return float("nan")
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


def load(path):
    out = []
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("{"):
                    out.append(json.loads(line))
    return out


def cell(d):
    recs = load(os.path.join(d, "run.jsonl"))
    commits = [r for r in recs if r.get("kind") == "commit"]
    ok = [r for r in commits if r["outcome"] == "ok"]
    end = next((r for r in recs if r.get("kind") == "end"), {})
    check = {}
    if os.path.exists(os.path.join(d, "check.json")):
        try:
            check = json.load(open(os.path.join(d, "check.json")))
        except json.JSONDecodeError:
            pass
    stderr = open(os.path.join(d, "stderr.log")).read() if os.path.exists(os.path.join(d, "stderr.log")) else ""
    outcomes = {}
    for r in commits:
        outcomes[r["outcome"]] = outcomes.get(r["outcome"], 0) + 1
    n = max(len(ok), 1)

    def per_commit(key):
        return sum(r["storage"][key] for r in ok) / n

    elapsed = end.get("elapsed_s") or float("nan")
    cps = [r.get("checkpoint_ms") for r in ok if r.get("checkpoint_ms")]
    row = {
        "cell": os.path.basename(d),
        "commits": len(ok),
        "commits_per_s": round(len(ok) / elapsed, 2) if elapsed == elapsed else "",
        "files": end.get("files_produced", ""),
        "files_per_commit": round(sum(r["files"] for r in ok) / n, 2),
        "commit_p50": round(pct([r["commit_ms"] for r in ok], 50), 1),
        "commit_p99": round(pct([r["commit_ms"] for r in ok], 99), 1),
        "commit_p999": round(pct([r["commit_ms"] for r in ok], 99.9), 1),
        "vis_p50": round(pct([r["visible_oldest_ms"] for r in ok], 50), 1),
        "vis_p99": round(pct([r["visible_oldest_ms"] for r in ok], 99), 1),
        "vis_p999": round(pct([r["visible_oldest_ms"] for r in ok], 99.9), 1),
        "lib_retries": sum(r["library_retries"] for r in commits),
        "conflicts": sum(r["storage"]["log_put_create_conflict"] for r in commits),
        "log_get_pc": round(per_commit("log_get"), 2),
        "log_head_pc": round(per_commit("log_head"), 2),
        "log_list_pc": round(per_commit("log_list"), 2),
        "log_put_pc": round(per_commit("log_put_create") + per_commit("log_put_other"), 2),
        "checkpoints": len(cps),
        "checkpoint_p50": round(pct(cps, 50), 1) if cps else "",
        "cpu_s": ";".join(f"{k}={v:.1f}" for k, v in sorted((end.get("cpu_secs") or {}).items())),
        "rss_mb": end.get("rss_mb", ""),
        "upload_failed": stderr.count("upload failed"),
        "outcomes": ";".join(f"{k}={v}" for k, v in sorted(outcomes.items())),
        "G1": check.get("g1_no_loss", ""),
        "G2": check.get("g2_no_duplicates", ""),
        "G3": check.get("g3_atomic", ""),
        "G4": check.get("g4_valid", ""),
        "sent_not_accepted": check.get("sent_not_accepted", ""),
    }
    for name, label in (("read-before.jsonl", "read_before_p50"), ("read-after.jsonl", "read_after_p50")):
        reads = load(os.path.join(d, name))
        if reads:
            row[label] = round(pct([r["load_ms"] for r in reads], 50), 1)
            row["log_version"] = reads[0].get("version")
    ck = load(os.path.join(d, "checkpoint.jsonl"))
    if ck:
        row["checkpoint_ms"] = round(ck[0]["ms"], 1)
        row["checkpoint_bytes"] = ck[0].get("bytes")
    prod = end.get("producer")
    if prod:
        row.update(arrow_columns(d, recs, prod, end))
    return row


def arrow_columns(d, recs, prod, end):
    """Throughput and encoder CPU for `--producer arrow` cells (GB = 1e9 bytes)."""
    secs = prod["produce_s"] or float("nan")
    enc = prod["encoders"]
    samples = [r for r in recs if r.get("kind") == "sample" and r.get("producer")]
    # Steady-state raw rate: median over sample intervals, dropping the first and last interval.
    rates = [(b["producer"]["raw_bytes"] - a["producer"]["raw_bytes"]) / ((b["ts_ms"] - a["ts_ms"]) / 1000)
             for a, b in zip(samples, samples[1:]) if b["ts_ms"] > a["ts_ms"]]
    steady = rates[1:-1] or rates
    # Encoder threads have exited by the end record; their CPU is accumulated in the producer stats.
    enc_cpu = prod["encoder_cpu_exited_s"]
    cpu = dict(end.get("cpu_secs") or {})
    cpu["encoder"] = enc_cpu
    nic = prod.get("nic_tx_delta_bytes")
    info = lambda name: open(os.path.join(d, name)).read().strip() if os.path.exists(os.path.join(d, name)) else ""
    return {
        "vm_size": info("vmsize.txt"),
        "nproc": info("nproc.txt"),
        "encoders": enc,
        "raw_GBps": round(prod["raw_bytes"] / secs / 1e9, 3),
        "raw_GBps_steady": round(pct(steady, 50) / 1e9, 3) if steady else "",
        "parquet_GBps": round(prod["parquet_bytes"] / secs / 1e9, 3),
        "ratio": round(prod["raw_bytes"] / max(prod["parquet_bytes"], 1), 2),
        "raw_GB": round(prod["raw_bytes"] / 1e9, 1),
        "parquet_files": prod["files"],
        "mean_file_MB": round(prod["parquet_bytes"] / max(prod["files"], 1) / 1e6, 1),
        "enc_cpu_s": round(enc_cpu, 1),
        "enc_util": round(enc_cpu / (enc * secs), 3) if enc else "",
        "raw_MBps_per_enc_core": round(prod["raw_bytes"] / max(enc_cpu, 1e-9) / 1e6, 1),
        "upload_wait_frac": round(prod["upload_wait_s"] / (enc * secs), 3) if enc else "",
        "upload_errors": prod.get("upload_errors", ""),
        "cpu_s": ";".join(f"{k}={v:.1f}" for k, v in sorted(cpu.items())),
        "rss_max_mb": round(max([r["rss_mb"] for r in samples] or [end.get("rss_mb") or 0])),
        "nic_tx_GBps": round(nic / secs / 1e9, 3) if nic is not None else "",
    }


def main():
    root = sys.argv[1]
    rows = [cell(os.path.join(root, d)) for d in sorted(os.listdir(root)) if os.path.isfile(os.path.join(root, d, "run.jsonl"))]
    cols = []
    for r in rows:
        cols += [k for k in r if k not in cols]
    if "--csv" in sys.argv:
        print(",".join(cols))
        for r in rows:
            print(",".join(str(r.get(c, "")) for c in cols))
    else:
        print("| " + " | ".join(cols) + " |")
        print("|" + "---|" * len(cols))
        for r in rows:
            print("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |")


if __name__ == "__main__":
    main()
