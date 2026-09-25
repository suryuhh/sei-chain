#!/usr/bin/env python3
"""Paired comparison of base and head readings written by compare.sh.

Each run directory holds <i>-base.json and <i>-head.json, the RESULT object a workload's run.py printed for pair i.
For every figure the workload declares, each pair gives one effect: the relative change 100 * (head - base) / |base|
(percent), or the absolute change head - base in the figure's own unit for a figure declared absolute (memory, a
share). The summary is the mean of those per-pair effects and a two-sided Student-t interval on n - 1 degrees of
freedom (mean +/- t * sd / sqrt(n)), 95% unless BENCH_CONFIDENCE says otherwise. With one pair there is no interval.

Exit status: 0 when every gate held on every reading and every output digest was identical across all readings;
1 otherwise (the failing gate or digest is printed).

Usage: stats.py summarize <run.json>      one line for one reading
       stats.py compare <runs-dir> <label>  the paired table and the gate/digest verdict
"""
import json
import math
import os
import statistics
import sys


def _incomplete_beta_continued_fraction(a, b, x):
    tiny = 1e-300
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    d = 1.0 / (d if abs(d) > tiny else tiny)
    result = d
    for iteration in range(1, 201):
        doubled = 2 * iteration
        coefficient = iteration * (b - iteration) * x / ((qam + doubled) * (a + doubled))
        d = 1.0 + coefficient * d
        d = d if abs(d) > tiny else tiny
        c = 1.0 + coefficient / c
        c = c if abs(c) > tiny else tiny
        d = 1.0 / d
        result *= d * c
        coefficient = -((a + iteration) * (qab + iteration) * x / ((a + doubled) * (qap + doubled)))
        d = 1.0 + coefficient * d
        d = d if abs(d) > tiny else tiny
        c = 1.0 + coefficient / c
        c = c if abs(c) > tiny else tiny
        d = 1.0 / d
        delta = d * c
        result *= delta
        if abs(delta - 1.0) <= 3e-14:
            return result
    raise ArithmeticError("Student-t beta fraction did not converge")


def _regularized_incomplete_beta(a, b, x):
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    scale = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log1p(-x))
    if x < (a + 1.0) / (a + b + 2.0):
        return scale * _incomplete_beta_continued_fraction(a, b, x) / a
    return 1.0 - scale * _incomplete_beta_continued_fraction(b, a, 1.0 - x) / b


def student_t_critical(confidence, degrees_of_freedom):
    """Two-sided Student-t critical value, by bisection on the CDF."""
    target_cdf = (1.0 + confidence) / 2.0

    def cdf(value):
        coordinate = degrees_of_freedom / (degrees_of_freedom + value * value)
        return 1.0 - 0.5 * _regularized_incomplete_beta(degrees_of_freedom / 2.0, 0.5, coordinate)

    low, high = 0.0, 1.0
    while cdf(high) < target_cdf:
        high *= 2.0
        if high > 1e12:
            raise ArithmeticError("Student-t critical value is unbounded")
    for _ in range(100):
        midpoint = (low + high) / 2.0
        if cdf(midpoint) < target_cdf:
            low = midpoint
        else:
            high = midpoint
    return high


def effect(base, head, kind):
    if kind == "absolute":
        return head - base
    if base == 0:
        raise ZeroDivisionError("a relative change of a zero base reading is undefined")
    return 100.0 * (head - base) / abs(base)


def paired_summary(effects, confidence):
    n = len(effects)
    mean = statistics.fmean(effects)
    if n < 2:
        return mean, None, None
    sd = 0.0 if effects.count(effects[0]) == n else statistics.stdev(effects)
    half = student_t_critical(confidence, n - 1) * sd / math.sqrt(n)
    return mean, mean - half, mean + half


def is_number(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


def fmt(v, unit=""):
    if not is_number(v):
        return "n/a"
    a = abs(v)
    s = f"{v:,.0f}" if a >= 1000 else (f"{v:.1f}" if a >= 10 else f"{v:.3g}")
    return s + (" " + unit if unit else "")


def summarize(path):
    r = json.load(open(path))
    spec = r.get("figure_spec", {})
    figs = r.get("figures", {})
    parts = [f"{k}={fmt(figs.get(k), spec.get(k, {}).get('unit', ''))}" for k in spec]
    failed = [g for g, ok in (r.get("gates") or {}).items() if ok is not True]
    gates = "gates ok" if not failed and r.get("gates") else "GATE FAILED: " + ",".join(failed or ["no gates reported"])
    return "  ".join(parts) + "  " + gates


def compare(runs_dir, label):
    confidence = float(os.environ.get("BENCH_CONFIDENCE", "0.95"))
    pairs = []
    i = 0
    while os.path.exists(os.path.join(runs_dir, f"{i}-base.json")) and os.path.exists(os.path.join(runs_dir, f"{i}-head.json")):
        pairs.append((json.load(open(os.path.join(runs_dir, f"{i}-base.json"))),
                      json.load(open(os.path.join(runs_dir, f"{i}-head.json")))))
        i += 1
    if not pairs:
        print("no complete pair of readings", file=sys.stderr)
        return 1
    spec = pairs[0][0].get("figure_spec", {})
    primary = pairs[0][0].get("primary")
    ok = True
    print(f"\n== {label}: {len(pairs)} pair(s), base then head alternating; change = head vs base, "
          f"{int(round(confidence * 100))}% paired t interval")
    print(f"{'figure':<24}{'base':>24}{'head':>24}   change")
    order = ([primary] if primary in spec else []) + [k for k in spec if k != primary]
    for name in order:
        s = spec[name]
        b = [p[0].get("figures", {}).get(name) for p in pairs]
        h = [p[1].get("figures", {}).get(name) for p in pairs]
        complete = [(x, y) for x, y in zip(b, h) if is_number(x) and is_number(y)]
        unit = s.get("unit", "")
        if not complete:
            print(f"{name:<24}{'n/a':>24}{'n/a':>24}   n/a")
            continue
        kind = s.get("effect", "relative")
        try:
            effects = [effect(x, y, kind) for x, y in complete]
        except ZeroDivisionError:
            print(f"{name:<24}{fmt(statistics.fmean(x for x, _ in complete), unit):>24}{fmt(statistics.fmean(y for _, y in complete), unit):>24}   n/a (zero base)")
            continue
        mean, lo, hi = paired_summary(effects, confidence)
        u = "%" if kind == "relative" else (" " + unit)
        interval = f" [{lo:+.1f}, {hi:+.1f}]" if lo is not None else ""
        each = " ".join(f"{e:+.1f}" for e in effects)
        mark = "*" if name == primary else " "
        print(f"{name + mark:<24}{fmt(statistics.fmean(x for x, _ in complete), unit):>24}{fmt(statistics.fmean(y for _, y in complete), unit):>24}   "
              f"{mean:+.1f}{u}{interval}   (per pair: {each}; {s.get('better', '')} is better)")
    # gates: every reading
    for i, (b, h) in enumerate(pairs):
        for side, r in (("base", b), ("head", h)):
            gates = r.get("gates") or {}
            failed = [g for g, v in gates.items() if v is not True]
            if not gates or failed:
                ok = False
                print(f"GATE FAILED: pair {i} {side}: {', '.join(failed) or 'no gates reported'}"
                      + (f"  problems: {r.get('problems')}" if r.get("problems") else ""))
    # digests: identical across every reading of both sides
    ref = None
    for i, (b, h) in enumerate(pairs):
        for side, r in (("base", b), ("head", h)):
            d = r.get("digests")
            if d is None:
                continue
            canon = json.dumps(d, sort_keys=True)
            if ref is None:
                ref = (canon, i, side)
            elif canon != ref[0]:
                ok = False
                print(f"OUTPUT DIFFERS: pair {i} {side} digests differ from pair {ref[1]} {ref[2]}:\n  {canon[:600]}\n  {ref[0][:600]}")
    print("\nverdict: " + (("all gates held and every output digest is identical on both sides" if ref else "all gates held on both sides") if ok
                          else "FAILED: an output-equality gate or digest differs (see above)"))
    return 0 if ok else 1


def main():
    if len(sys.argv) == 3 and sys.argv[1] == "summarize":
        print(summarize(sys.argv[2]))
        return 0
    if len(sys.argv) == 4 and sys.argv[1] == "compare":
        return compare(sys.argv[2], sys.argv[3])
    if len(sys.argv) == 2 and sys.argv[1] == "self-test":
        assert abs(student_t_critical(0.95, 1) - 12.706204736) < 1e-6
        assert abs(student_t_critical(0.95, 5) - 2.570581836) < 1e-6
        m, lo, hi = paired_summary([1.0, 2.0, 3.0], 0.95)
        assert m == 2.0 and abs(hi - m - 4.302652730 / math.sqrt(3)) < 1e-6
        assert paired_summary([5.0], 0.95) == (5.0, None, None)
        assert paired_summary([2.0, 2.0], 0.95) == (2.0, 2.0, 2.0)
        assert effect(100.0, 110.0, "relative") == 10.0 and effect(100.0, 110.0, "absolute") == 10.0
        print("SELF_TEST ok")
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
