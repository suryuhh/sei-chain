#!/usr/bin/env python3
"""Log-poll benchmark: CPU per eth_getLogs-shaped FilterLogs call at the littidx receipt store.

Setup builds the sei-db/ledger_db/receipt test binary (bench_logpoll_test.go copied into that package under the
`benchharness` build tag) and fills a littidx receipt store (Backend littidx, AsyncWriteBuffer 0) with the tree's
own code: 2,000 blocks x 2,000 receipts, per block 70% transfers of one hot token (a tenth with an Approval), 10%
transfers over 63 cold tokens, one transfer of a rare token, and 20% router swaps over 64 pools (half on one hot
pool), each swap writing Transfer in, Transfer out, Sync and Swap. The store is then reopened and every poll shape run
for 20 s, so the store's own background work after the fill lands in setup. One reading: one process opens the store,
makes one untimed call, then times FilterLogs for the variant's poll over at least 4 s and 3 calls, GOMAXPROCS = the
machine's CPUs.

Variants (getlogs-cold:<variant>):
  cold      a cold token's Transfer events (address + Transfer) over all 2,000 blocks (default)
  rare      a rare token's Transfer events over all 2,000 blocks (one per block)
  cold-w8   a cold token's Transfer events over the last 8 blocks
  dense     the hot token's Transfer events over the last 8 blocks (1,400 matches per block)
  addr      a cold pool's events by address only over all 2,000 blocks (one criteria group)

Figures: cpu_ms_per_call = process CPU (getrusage) per call; wall_ms_per_call = wall per call.
Gates: run_ok (exit 0, at least 3 calls, every call returned the untimed call's log count, the store's latest height
is 2,000, the poll named is the variant's) and answer_matches (the answer's log count and SHA-256 over every log's
JSON, in order, equal the pinned values of the shipped code). The log count and digest are also compared across base
and head.

Usage (compare.sh drives it):
  run.py setup   --tree CHECKOUT --out DIR --workload W   copy the harness in, build, fill and settle the store
  run.py measure --tree CHECKOUT --out DIR --workload W   one reading; prints RESULT {json}
  run.py self-test
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "lib"))
import common  # noqa: E402

PKG = os.path.join("sei-db", "ledger_db", "receipt")
HARNESS = "bench_logpoll_test.go"
BIN_NAME = "logpoll.test"
WINDOW_MS = 4000
MIN_CALLS = 3
FILL_BLOCKS = 2000
SETTLE_S = 20
LATEST = 2000
WITNESS_TOLERANCE = 0.02  # CPU and wall per call on a second clock each must agree within 2%

# variant -> the harness's poll shape
SHAPES = {"cold": "ct_cold", "rare": "ct_rare", "cold-w8": "ct_cold_w8", "dense": "dense_w8", "addr": "ce_all"}
DEFAULT_VARIANT = "cold"

# The shipped code's answer to each shape on this fill: (log count, first 16 hex of the SHA-256 over the logs' JSON).
EXPECTED = {
    "ct_cold": (6319, "308017375925abe9"),
    "ct_rare": (2000, "e9ca8fd3b4108167"),
    "ct_cold_w8": (26, "cf5a84308e8e24a0"),
    "dense_w8": (11200, "c22cf87d96257047"),
    "ce_all": (12698, "0cdedb3c17f92fe6"),
}

GATES = ("run_ok", "answer_matches")
FIGURES = {
    "cpu_ms_per_call": {"unit": "ms CPU/call", "better": "lower", "effect": "relative"},
    "wall_ms_per_call": {"unit": "ms/call", "better": "lower", "effect": "relative"},
}


def shape_of(workload):
    if workload == "getlogs-cold":
        return SHAPES[DEFAULT_VARIANT]
    if workload and workload.startswith("getlogs-cold:"):
        return SHAPES.get(workload.split(":", 1)[1])
    return None


def parse_line(stdout):
    for raw in stdout.splitlines():
        if raw.startswith("LITTPOLL query="):
            return dict(tok.split("=", 1) for tok in raw.split()[1:] if "=" in tok)
    return None


def values_from(shape, rc, kv):
    """Returns (values, witness) for one run of one poll shape."""
    want_logs, want_digest = EXPECTED[shape]
    ok = rc == 0 and kv is not None
    values, witness = {}, {}
    if ok:
        calls = int(kv["calls"])
        values["cpu_ms_per_call"] = float(kv["cpu_us_per_call"]) / 1000.0
        values["wall_ms_per_call"] = float(kv["wall_us_per_call"]) / 1000.0
        witness["cpu_ms_per_call"] = float(kv["cpu2_us_per_call"]) / 1000.0
        witness["wall_ms_per_call"] = float(kv["wall2_us_per_call"]) / 1000.0
        values["calls"] = calls
        values["logs"] = int(kv["logs"])
        values["digest"] = kv["digest"]
        values["run_ok"] = calls >= MIN_CALLS and kv.get("consistent") == "true" and int(kv["latest"]) == LATEST and kv.get("query") == shape
        values["answer_matches"] = int(kv["logs"]) == want_logs and kv["digest"] == want_digest
    else:
        values["run_ok"] = False
        values["answer_matches"] = False
    return values, witness


def child_env(out, extra):
    tmp = os.path.join(out, "tmp")
    os.makedirs(tmp, exist_ok=True)
    env = dict(os.environ, LP_DIR=os.path.join(out, "store"), TMPDIR=tmp, **extra)
    for k in ("GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GODEBUG"):
        env.pop(k, None)
    return env


def run_test(out, test, extra, timeout):
    env = child_env(out, extra)
    return subprocess.run([os.path.join(out, BIN_NAME), "-test.run", f"^{test}$", "-test.count=1", f"-test.timeout={timeout}s"],
                          cwd=out, env=env, capture_output=True, text=True, timeout=timeout + 40)


def setup(tree, out):
    os.makedirs(out, exist_ok=True)
    common.place_harness(os.path.join(HERE, "harness", HARNESS), tree, PKG, HARNESS)
    env = common.go_env(tree)
    binary = os.path.join(out, BIN_NAME)
    subprocess.run(["go", "test", "-c", "-tags", "benchharness", "-o", binary, "./" + PKG + "/"], cwd=tree, env=env, check=True)
    print(f"built {binary}")
    store = os.path.join(out, "store")
    shutil.rmtree(store, ignore_errors=True)
    for test, extra, timeout in (("TestLittPollFill", {"LP_BLOCKS": str(FILL_BLOCKS)}, 1200),
                                 ("TestLittPollSettle", {"LP_SETTLE_S": str(SETTLE_S)}, 600)):
        started = time.monotonic()
        proc = run_test(out, test, extra, timeout)
        for line in (proc.stdout + proc.stderr).splitlines():
            if line.startswith("LITTPOLL") or line.startswith(("---", "ok", "FAIL", "PASS")) or "panic" in line:
                print(line[:300])
        print(f"{test} rc={proc.returncode} seconds={time.monotonic() - started:.1f}")
        if proc.returncode != 0:
            print((proc.stdout + proc.stderr)[-3000:], file=sys.stderr)
            return 1
    return 0


def measure(tree, out, workload):
    shape = shape_of(workload)
    result = {"workload": workload, "poll": shape, "figure_spec": FIGURES, "primary": "cpu_ms_per_call",
              "host": {"loadavg_before": list(os.getloadavg())}}
    binary = os.path.join(out, BIN_NAME)
    if not os.access(binary, os.X_OK) or not os.path.isdir(os.path.join(out, "store")):
        result.update({"figures": {k: None for k in FIGURES}, "gates": {g: False for g in GATES},
                       "problems": ["setup did not build the test binary or fill the store"]})
        print("RESULT " + json.dumps(result, sort_keys=True))
        return 0
    started = time.monotonic()
    proc = run_test(out, "TestLittPollQuery", {"LP_ONLY": shape, "LP_MS": str(WINDOW_MS)}, 240)
    kv = parse_line(proc.stdout)
    values, witness = values_from(shape, proc.returncode, kv)
    figures = {k: values.get(k) for k in FIGURES}
    disputes = common.apply_witness(figures, witness, WITNESS_TOLERANCE, list(FIGURES)) if values.get("run_ok") else []
    result.update({
        "figures": figures,
        "witness_disputes": disputes,
        "gates": {g: bool(values.get(g)) for g in GATES},
        "digests": {"logs": values.get("logs"), "digest": values.get("digest")},
        "calls": values.get("calls"),
        "witness": witness,
        "process_s": round(time.monotonic() - started, 2),
    })
    if not values.get("run_ok"):
        result["output_tail"] = (proc.stdout + proc.stderr)[-1500:]
    print("RESULT " + json.dumps(result, sort_keys=True))
    return 0


def teardown(out):
    if out:
        shutil.rmtree(os.path.join(out, "store"), ignore_errors=True)
        shutil.rmtree(os.path.join(out, "tmp"), ignore_errors=True)
    return 0


def self_test():
    good = {"query": "ct_cold", "latest": "2000", "logs": "6319", "calls": "20", "consistent": "true",
            "cpu_us_per_call": "1844000.0", "cpu2_us_per_call": "1844100.0", "wall_us_per_call": "166000.0",
            "wall2_us_per_call": "166000.0", "digest": "308017375925abe9"}
    v, w = values_from("ct_cold", 0, good)
    assert v["run_ok"] and v["answer_matches"] and abs(v["cpu_ms_per_call"] - 1844.0) < 1e-6 and w["cpu_ms_per_call"] > 0
    v, _ = values_from("ct_cold", 0, dict(good, logs="6320"))
    assert not v["answer_matches"]
    v, _ = values_from("ct_cold", 0, dict(good, digest="0" * 16))
    assert not v["answer_matches"]
    v, _ = values_from("ct_cold", 0, dict(good, calls="2"))
    assert not v["run_ok"]
    v, _ = values_from("ct_cold", 0, dict(good, consistent="false"))
    assert not v["run_ok"]
    v, _ = values_from("ct_cold", 1, good)
    assert not v["run_ok"] and not v["answer_matches"]
    v, _ = values_from("dense_w8", 0, good)
    assert not v["answer_matches"] and not v["run_ok"]
    assert parse_line("x\nLITTPOLL query=ce_all latest=2000 logs=1\n")["query"] == "ce_all"
    assert shape_of("getlogs-cold") == "ct_cold" and shape_of("getlogs-cold:addr") == "ce_all"
    assert shape_of("getlogs-cold:nope") is None and shape_of("getlogs") is None
    assert set(SHAPES.values()) == set(EXPECTED)
    v, w = values_from("ct_cold", 0, good)
    figs = {k: v[k] for k in FIGURES}
    assert common.apply_witness(figs, w, WITNESS_TOLERANCE, list(FIGURES)) == [] and figs["cpu_ms_per_call"] is not None
    v, w = values_from("ct_cold", 0, dict(good, cpu2_us_per_call="2000000.0"))
    figs = {k: v[k] for k in FIGURES}
    assert len(common.apply_witness(figs, w, WITNESS_TOLERANCE, list(FIGURES))) == 1 and figs["cpu_ms_per_call"] is None
    print("SELF_TEST ok")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=("check", "assets", "tools", "setup", "measure", "teardown", "self-test"))
    ap.add_argument("--tree")
    ap.add_argument("--out")
    ap.add_argument("--data")
    ap.add_argument("--tools")
    ap.add_argument("--workload")
    a = ap.parse_args()
    if a.mode == "self-test":
        return self_test()
    if a.mode in ("check", "assets", "setup", "measure") and shape_of(a.workload) is None:
        print(f"unknown workload {a.workload!r}; getlogs-cold or getlogs-cold:<{'|'.join(SHAPES)}>", file=sys.stderr)
        return 2
    if a.mode in ("check", "assets", "tools"):
        return 0
    if a.mode == "teardown":
        return teardown(os.path.abspath(a.out) if a.out else None)
    tree, out = os.path.abspath(a.tree), os.path.abspath(a.out)
    if a.mode == "setup":
        return setup(tree, out)
    return measure(tree, out, a.workload)


if __name__ == "__main__":
    sys.exit(main())
