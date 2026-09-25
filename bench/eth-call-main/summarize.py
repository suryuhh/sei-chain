#!/usr/bin/env python3
"""summarize.py <results.tsv>: per-side medians and correctness checks of run.sh results."""
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1]), delimiter="\t"))
def med(side, key):
    vals = []
    for r in rows:
        if r["label"] == side:
            try:
                vals.append(float(r[key]))
            except ValueError:
                pass
    return statistics.median(vals) if vals else float("nan")
keys = ["call_answered", "est_answered", "node_tx_s", "cpu_us_per_tx", "call_p50_ms", "call_p99_ms", "est_p50_ms", "est_p99_ms"]
for side in ("base", "patched"):
    n = sum(1 for r in rows if r["label"] == side)
    print(f"MEDIAN side={side} runs={n} " + " ".join(f"{k}={med(side, k):.4g}" for k in keys))
for side in ("base", "patched"):
    bad = [r for r in rows if r["label"] == side and (r["call_mismatched"] not in ("0", "?") or r["est_mismatched"] not in ("0", "?") or r["node_rc"] != "0" or r["client_rc"] != "0")]
    print(f"CHECK side={side} runs_with_mismatch_or_failure={len(bad)}")
print("APPHASH " + " ".join(sorted({r["label"] + ":" + r["apphash"] for r in rows})))
