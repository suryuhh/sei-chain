#!/usr/bin/env python3
"""eth_call under load: a node that executes blocks and answers its users' eth_calls, read at its execute loop and at
the caller.

One process of the sei-tendermint/internal/p2p test binary (built with bench_call_test.go copied into that package
under the `benchharness` build tag) runs a one-validator Autobahn EVM-only node in process: the real evmonlyapp, Giga
executor, FlatKV, littidx receipt store, block store and runExecute, at the machine's GOMAXPROCS. It presigns 120
blocks of 2,000 ERC-20 transfers (the evmonly-loadtest erc20-transfer scenario, one contract), admits them with the
application's real CheckTx before the first block, inserts them through the producer's mempool with the first block
held until the producer is at capacity, and times the node's committed blocks.

Workloads:
  rpc-call        (default variant `call`) the node's real EVM-only JSON-RPC server is up, and a separate client
                  process (GOMAXPROCS=2) sends one eth_call every 500 us, each on its own goroutine and never retried,
                  from the node's first executed block until it reports its last: open-loop demand of 2,000 calls/s
                  whatever the node's progress. Each call is creation code returning NUMBER and BALANCE(0x0), the
                  coinbase every block's fees credit; each answer is checked against the balance the node recorded as
                  that height's FinalizeBlock returned. A call answered after the node's last block, or still
                  outstanding then, is not credited.
  rpc-call:off    the same node with no RPC server and no client.

Figures: steady_txps (successful transactions per second over the node's steady span), tx_per_cpu_s (window
transactions per process CPU-second), loop_execution_ms and loop_storage_ms (main-loop execution and storage time per
window block), peak_rss_mb (the node process's lifetime maxrss, read from wait4), call_answered_pct (calls answered
within the span over calls offered, in percent) and call_p99_ms (the client's 99th percentile answer latency).

Gates: run_integrity (exit 0; 240,000 successful transactions and receipts, the pinned gas total, 120 blocks of 2,000;
the producer's nonce table never read ahead of the application), admission_regime (CheckTx ran on every transaction
before the window), execution_bound (the node, not the inserter, set the rate: a backlog of at least 10 blocks before
the first block, no steady block starved, a mean receive-to-execute depth of at least 5 blocks), state_gate (the
FlatKV LtHash, recomputed by seidb built from the base tree, equals the pinned value), apphash_gate (final app hash and
the digest over all 120 app hashes equal the pinned values), receipt_gate (receipt and log-filter digests, read by a
receipt reader built from the base tree, equal the pinned values), call_gate (the client exited cleanly, offered at
least 2,000 calls, got no wrong answer, transport error, malformed return or unfinished call, and saw every height
recorded; a refusal is not a gate failure, it is what call_answered_pct reads). On Linux each reading first waits for
the machine to be idle (two 5 s windows at 95% idle, at most 180 s); the outcome is recorded under "host", not gated.

Usage (compare.sh drives it):
  run.py tools   --tree BASE_CHECKOUT --out DIR
  run.py setup   --tree CHECKOUT --out DIR --tools DIR --workload W
  run.py measure --tree CHECKOUT --out DIR --tools DIR --workload W   one reading; prints RESULT {json}
  run.py self-test
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "lib"))
import common  # noqa: E402

PKG = os.path.join("sei-tendermint", "internal", "p2p")
HARNESS = "bench_call_test.go"
BIN_NAME = "call.test"

REPEATS = 1
BLOCKS = 120
TXS_PER_BLOCK = 2000
EXPECTED_TXS = BLOCKS * TXS_PER_BLOCK
EXPECTED_LATEST = BLOCKS + 1  # genesis state is version 1; blocks run at heights 2..121
COOLDOWN_MIN_SECONDS = 5.0
COOLDOWN_SAMPLE_SECONDS = 5.0
COOLDOWN_MAX_SECONDS = 180.0
COOLDOWN_IDLE_FRACTION = 0.95
COOLDOWN_CONSECUTIVE_WINDOWS = 2
MIN_RECEIVE_EXECUTE_DEPTH = 5.0
MIN_NONCE_CHECKS = 1000  # nonce-table answers compared with the application's own read, per process
MIN_FILL_BLOCKS = 10  # the fill before the first block hands over at least this many blocks
GAUGE_MS = "20"
PROCESS_TIMEOUT_SECONDS = 400
STORE_PREFIX = "bench-call-"

# Output of this workload on #4273's head (11431679e) and on every tree that leaves execution unchanged.
CONSTANTS = {
    "gas": 8280501840,
    "final_apphash": "96847808f191b8228618b36e4fd6b303bebf2d10addcbe17efe6278063eb004a",
    "apphash_digest": "ad8f106a16543690a84e9e94e8b4f50837cd16fd978e709995f00a09010956c9",
    "lthash": "a87b924c9489d977464005863865da01f9e1666e390ce0b69192355df9a85634",
    "receipt_digest": "12aa4bcaa3a0e6cf3f0bf1451eb72f48752d12dda81961815d7b495336cb4fee",
    "filter_digest": "789e7a426eb8b1392b23c2d1b8588614791cb6706e5c80a4dfce6f6f8eedd9b3",
}

BASE = {"NODE1_ADMIT": "self", "NODE1_WORKLOAD": "erc20-transfer", "NODE1_NONCE": "table"}
VARIANTS = {
    "off": dict(BASE),
    "call": dict(BASE, NODE1_RPCQ_LIVE="serve", NODE1_CALLQ="1"),
}
DEFAULT_VARIANT = "call"
CLIENT = {"GOMAXPROCS": "2", "NODE1_CALLQ_EVERY_US": "500", "NODE1_RPCQ_CLIENT_S": "360", "NODE1_RPCQ_FIRST": "2"}
MIN_OFFERED = 2000  # calls offered over the span, per process
HOLD_S = 360
N_HEIGHTS = BLOCKS
# every figure carries a second, independent count (see aggregate); a reading whose two counts differ by more than
# this (relative) leaves that figure out
WITNESS_TOLERANCE = 0.001
WITNESSED = ("steady_txps", "tx_per_cpu_s", "loop_execution_ms", "loop_storage_ms", "peak_rss_mb", "call_answered_share", "call_p99_ms")

GATES = ("run_integrity", "admission_regime", "execution_bound", "state_gate", "apphash_gate", "receipt_gate", "call_gate")
FIGURES = {
    "steady_txps": {"unit": "tx/s", "better": "higher", "effect": "relative"},
    "tx_per_cpu_s": {"unit": "tx/cpu-s", "better": "higher", "effect": "relative"},
    "loop_execution_ms": {"unit": "ms/block", "better": "lower", "effect": "relative"},
    "loop_storage_ms": {"unit": "ms/block", "better": "lower", "effect": "relative"},
    "peak_rss_mb": {"unit": "MB", "better": "lower", "effect": "absolute"},
    "call_answered_pct": {"unit": "% of offered calls", "better": "higher", "effect": "absolute"},
    "call_p99_ms": {"unit": "ms", "better": "lower", "effect": "relative"},
}
PRIMARY = "call_answered_pct"


def variant_of(workload):
    if workload == "rpc-call":
        return DEFAULT_VARIANT
    if workload and workload.startswith("rpc-call:") and workload.split(":", 1)[1] in VARIANTS:
        return workload.split(":", 1)[1]
    return None


# ------------------------------------------------------------------------------------------------
# machine idle wait (Linux /proc/stat; recorded, not gated)

def read_text_if_present(path):
    try:
        return path.read_text()
    except OSError:
        return ""


def proc_cpu_totals(text):
    first = text.splitlines()[0].split() if text else []
    if not first or first[0] != "cpu" or len(first) < 6:
        return None
    try:
        values = [int(value) for value in first[1:]]
    except ValueError:
        return None
    return sum(values), values[3] + values[4]


def cpu_idle_fraction(before, after):
    if before is None or after is None:
        return None
    total = after[0] - before[0]
    idle = after[1] - before[1]
    if total <= 0 or idle < 0:
        return None
    return idle / total


def quiescence_held(fractions):
    return (
        len(fractions) >= COOLDOWN_CONSECUTIVE_WINDOWS and
        all(v is not None and v >= COOLDOWN_IDLE_FRACTION for v in fractions[-COOLDOWN_CONSECUTIVE_WINDOWS:])
    )


def wait_for_quiescence(min_seconds=COOLDOWN_MIN_SECONDS, max_seconds=COOLDOWN_MAX_SECONDS):
    if not os.path.exists("/proc/stat"):
        return None, {"note": "no /proc/stat on this system: idle wait skipped"}
    started = time.monotonic()
    fractions = []
    while time.monotonic() - started < max_seconds:
        before = proc_cpu_totals(read_text_if_present(Path("/proc/stat")))
        time.sleep(COOLDOWN_SAMPLE_SECONDS)
        after = proc_cpu_totals(read_text_if_present(Path("/proc/stat")))
        fractions.append(cpu_idle_fraction(before, after))
        if time.monotonic() - started >= min_seconds and quiescence_held(fractions):
            break
    return quiescence_held(fractions), {
        "elapsed_s": time.monotonic() - started,
        "idle_fractions": fractions,
        "loadavg": read_text_if_present(Path("/proc/loadavg")).strip(),
    }


# ------------------------------------------------------------------------------------------------
# parsing the harness's NODE1 and CALLQCLIENT lines

def key_values(text):
    out = {}
    for token in text.split():
        if "=" in token:
            key, value = token.split("=", 1)
            out[key] = value
    return out


def parse_node1(stdout):
    """Returns {"lines": {kind: kv}, "wphase": {name attrs: (total_s, ms_per_block)}, "gauge": {key: kv},
    "count": {name attrs: total}}."""
    lines, wphase, gauge, count = {}, {}, {}, {}
    for raw in stdout.splitlines():
        if not raw.startswith("NODE1 "):
            continue
        parts = raw.split(" ", 2)
        if len(parts) < 3:
            continue
        kind, rest = parts[1], parts[2]
        if kind == "wphase":
            m = re.match(r"(\S+) (\S+) total_s=(\S+) ms_per_block=(\S+)$", rest)
            if m:
                wphase[f"{m.group(1)} {m.group(2)}"] = (float(m.group(3)), float(m.group(4)))
        elif kind == "count":
            m = re.match(r"(\S+) (\S+) total=(\d+)$", rest)
            if m:
                count[f"{m.group(1)} {m.group(2)}"] = int(m.group(3))
        elif kind == "gauge":
            m = re.match(r"(.+) (n=\S+ mean=\S+ min=\S+ max=\S+)$", rest)
            if m:
                gauge[m.group(1)] = key_values(m.group(2))
        elif kind in ("cfg", "admission", "window", "steady", "feed", "feeddiag", "success", "state", "proc", "paths"):
            lines[kind] = key_values(rest)
    return {"lines": lines, "wphase": wphase, "gauge": gauge, "count": count}


def parse_client(stdout):
    """The client's CALLQCLIENT lines, merged."""
    out = {}
    for raw in stdout.splitlines():
        if raw.startswith("CALLQCLIENT "):
            out.update(key_values(raw[len("CALLQCLIENT "):]))
    return out


def main_loop(parsed):
    phases = {}
    for key, (total, per_block) in parsed["wphase"].items():
        name, attrs = key.split(" ", 1)
        if name == "autobahn_main_loop_phase_duration_seconds_total" and attrs.startswith("phase="):
            phases[attrs[len("phase="):]] = (total, per_block)
    return phases


def wphase_value(parsed, name, phase):
    return parsed["wphase"].get(f"{name} phase={phase}", (0.0, 0.0))


def gauge_mean(parsed, key):
    try:
        return float(parsed["gauge"][key]["mean"])
    except (KeyError, ValueError):
        return None


# ------------------------------------------------------------------------------------------------
# readers built from the base tree, run after the node exits

def run_state_gate(tools, store):
    proc = subprocess.run(
        [os.path.join(tools, common.SEIDB), "dump-flatkv", "-d", str(store / "node0/data/state_commit/flatkv"), "--lthash-only"],
        text=True, capture_output=True, check=False, timeout=300,
    )
    total, verify_pass = "NONE", False
    for line in proc.stdout.splitlines() + proc.stderr.splitlines():
        if "result: PASS" in line:
            verify_pass = True
        m = re.search(r"TOTAL\s+lthash=([0-9a-f]+)", line)
        if m:
            total = m.group(1)
    return {"rc": proc.returncode, "total": total, "verify_pass": verify_pass}


def run_receipt_reader(tools, store):
    proc = subprocess.run(
        [os.path.join(tools, common.RECEIPT_READER), "--receipt-dir", str(store / "node0/data/ledger/receipt/littidx"),
         "--expect-latest", str(EXPECTED_LATEST)],
        text=True, capture_output=True, check=False, timeout=300,
    )
    values = {}
    for line in proc.stdout.splitlines():
        if line.startswith("latest="):
            values = key_values(line)
    return {"rc": proc.returncode, "values": values}


def remove_store(store):
    if store.exists() or store.is_symlink():
        shutil.rmtree(store, ignore_errors=True)
    if store.exists():
        raise RuntimeError(f"store cleanup left path behind: {store}")


def run_teardown():
    removed = 0
    for path in sorted(Path(tempfile.gettempdir()).glob(STORE_PREFIX + "*")):
        shutil.rmtree(path, ignore_errors=True)
        removed += 1
    survivors = list(Path(tempfile.gettempdir()).glob(STORE_PREFIX + "*"))
    print(f"cleanup_teardown_removed={removed} survivors={len(survivors)}")
    return 0 if not survivors else 1


# ------------------------------------------------------------------------------------------------
# one process and its checks

def run_process(binary, variant):
    store = Path(tempfile.mkdtemp(prefix=STORE_PREFIX))
    env = dict(os.environ)
    env.update(VARIANTS[variant])
    env.update({"NODE1_BLOCKS": str(BLOCKS), "NODE1_TXS": str(TXS_PER_BLOCK), "NODE1_DIR": str(store)})
    for key in ("GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GODEBUG"):
        env.pop(key, None)
    serving = "NODE1_RPCQ_LIVE" in VARIANTS[variant]
    if serving:
        env.update({"NODE1_RPCQ_HASHFILE": str(store / "hashes"), "NODE1_RPCQ_DONEFILE": str(store / "client-done"),
                    "NODE1_RPCQ_HOLD_S": str(HOLD_S)})
    started = time.monotonic()
    out_path = store / "node-stdout.txt"
    client, client_out = None, store / "client-stdout.txt"
    with open(out_path, "w") as out:
        proc = subprocess.Popen(
            [binary, "-test.run", "^TestNode1$", "-test.count=1", f"-test.timeout={PROCESS_TIMEOUT_SECONDS + HOLD_S}s"],
            env=env, stdout=out, stderr=subprocess.STDOUT,
        )
        if serving:
            cenv = {k: v for k, v in env.items() if k != "NODE1_RPCQ_LIVE"}
            cenv.update(CLIENT)
            with open(client_out, "w") as cout:
                client = subprocess.Popen(
                    [binary, "-test.run", "^TestCallQClient$", "-test.count=1", f"-test.timeout={PROCESS_TIMEOUT_SECONDS + HOLD_S}s"],
                    env=cenv, stdout=cout, stderr=subprocess.STDOUT,
                )
        killer = threading.Timer(PROCESS_TIMEOUT_SECONDS + HOLD_S + 60, proc.kill)
        killer.start()
        try:
            _, status, rusage = os.wait4(proc.pid, 0)
        finally:
            killer.cancel()
    proc.returncode = os.waitstatus_to_exitcode(status)
    wall = time.monotonic() - started
    client_rc = None
    if client is not None:
        try:
            client_rc = client.wait(timeout=60)
        except subprocess.TimeoutExpired:
            client.kill()
            client_rc = client.wait()
    stdout = out_path.read_text(errors="replace")
    client_stdout = client_out.read_text(errors="replace") if client is not None else ""
    return {"variant": variant, "store": store, "rc": proc.returncode, "stdout": stdout, "wall_s": wall,
            "rusage_maxrss_kb": rusage.ru_maxrss, "client_rc": client_rc, "client_stdout": client_stdout}


def validate_process(tools, run):
    parsed = parse_node1(run["stdout"])
    state = run_state_gate(tools, run["store"]) if run["rc"] == 0 else {"rc": None, "total": "NONE", "verify_pass": False}
    receipts = run_receipt_reader(tools, run["store"]) if run["rc"] == 0 else {"rc": None, "values": {}}
    return {"parsed": parsed, "state": state, "receipts": receipts}


def process_observation(run, checked):
    parsed, L = checked["parsed"], checked["parsed"]["lines"]
    variant = run["variant"]
    obs = {"rc": run["rc"], "external_wall_s": run["wall_s"], "variant": variant}

    def num(kind, key, cast=float):
        try:
            return cast(L[kind][key])
        except (KeyError, ValueError):
            return None

    steady_txs, steady_wall = num("steady", "txs", int), num("steady", "wall_s")
    steady_blocks, steady_ms = num("steady", "blocks", int), num("steady", "ms_per_block")
    window_txs, cpu_window = num("window", "txs", int), num("proc", "cpu_window_s")
    cpu_us = num("proc", "cpu_us_per_tx")
    loop = main_loop(parsed)
    exec_total = loop.get("execution", (None, None))[0]
    loop_total = sum(v[0] for v in loop.values()) if loop else None
    window_blocks = num("window", "blocks", int)
    exec_ms = loop.get("execution", (None, None))[1]
    loop_ms = sum(v[1] for v in loop.values()) if loop else None

    obs.update({
        "steady_txs": steady_txs, "steady_wall_s": steady_wall, "steady_blocks": steady_blocks, "steady_ms_per_block": steady_ms,
        "window_txs": window_txs, "cpu_window_s": cpu_window, "cpu_us_per_tx": cpu_us,
        "loop_execution_s": exec_total, "loop_total_s": loop_total, "loop_execution_ms": exec_ms, "loop_total_ms": loop_ms,
        "loop_storage_s": loop.get("storage", (None, None))[0], "window_blocks": window_blocks,
        "prepare_parse_s": wphase_value(parsed, "evmonly_prepare_phase_duration_seconds_total", "parse")[0],
        "finalize_execute_s": wphase_value(parsed, "evmonly_finalize_phase_duration_seconds_total", "execute")[0],
        "vault_commit_s": wphase_value(parsed, "autobahn_storage_tail_phase_duration_seconds_total", "vault_commit")[0],
        "push_app_hash_s": wphase_value(parsed, "autobahn_storage_tail_phase_duration_seconds_total", "push_app_hash")[0],
        "peak_rss_mb": num("proc", "peak_rss_mb"),
        "rusage_maxrss_mb": None if run["rusage_maxrss_kb"] is None else run["rusage_maxrss_kb"] / (1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0),
        "cores_busy": num("proc", "cores_busy"),
        "receive_execute_depth": gauge_mean(parsed, "depth receive-execute"),
        "capacity_wait": gauge_mean(parsed, "tendermint_internal_autobahn_producer_in_flight{phase=capacity_wait}"),
        "feed": L.get("feed", {}),
        "feeddiag": L.get("feeddiag", {}),
        "admission": L.get("admission", {}),
        "state_reader": checked["state"], "receipt_reader": checked["receipts"],
        "final_apphash": L.get("state", {}).get("final_apphash"), "apphash_digest": L.get("state", {}).get("apphash_digest"),
        "node_lthash": L.get("state", {}).get("flatkv_lthash"), "gomaxprocs": L.get("cfg", {}).get("gomaxprocs"),
    })

    client = parse_client(run.get("client_stdout", ""))
    obs["client"] = client

    def cnum(key, cast=int):
        try:
            return cast(client[key])
        except (KeyError, ValueError):
            return None

    for key in ("offered", "answered_in_span", "refused", "http_errors", "bad_return", "outstanding_at_end", "answered_late",
                "unfinished", "matched", "mismatched", "unrecorded", "recorded_heights", "heights_answered"):
        obs["call_" + key] = cnum(key)
    for key in ("lat_ms_p50", "lat_ms_p90", "lat_ms_p99", "lat_ms_max", "client_cpu_s", "span_s"):
        obs["call_" + key] = cnum(key, float)
    if variant != "off":
        n = {k: obs["call_" + k] for k in ("offered", "mismatched", "bad_return", "http_errors", "unfinished", "unrecorded",
                                           "recorded_heights", "matched")}
        # a refusal is what call_answered_pct reads, never a gate failure; a wrong answer, a transport error, a call
        # that never finished or too little demand is
        obs["call_gate"] = bool(
            run.get("client_rc") == 0 and client.get("span_ended") == "true" and all(v is not None for v in n.values()) and
            n["offered"] >= MIN_OFFERED and n["mismatched"] == 0 and n["bad_return"] == 0 and n["http_errors"] == 0 and
            n["unfinished"] == 0 and n["unrecorded"] == 0 and n["recorded_heights"] == N_HEIGHTS and n["matched"] >= 1
        )
    else:
        obs["call_gate"] = run.get("client_rc") is None and not client
    s, st = L.get("success", {}), L.get("state", {})
    obs["run_integrity"] = bool(
        run["rc"] == 0 and L.get("cfg", {}).get("workload") == "erc20-transfer" and s.get("ok") == str(EXPECTED_TXS) and s.get("failed") == "0" and
        s.get("receipts_ok") == str(EXPECTED_TXS) and s.get("gas_used") == str(CONSTANTS["gas"]) and
        st.get("blocks") == str(BLOCKS) and st.get("block_txs_min") == str(TXS_PER_BLOCK) and
        st.get("block_txs_max") == str(TXS_PER_BLOCK) and steady_txs and steady_wall and window_txs and cpu_window and loop_total
    )
    # the producer's nonce reads were answered from the harness table, which never read above the application's own
    # answer on the calls checked against it
    nd = L.get("feeddiag", {})
    try:
        nonce_ok = (nd.get("nonce_table") == "true" and nd.get("nonce_ahead") == "0" and
                    int(nd.get("nonce_checked", "0")) >= MIN_NONCE_CHECKS)
    except ValueError:
        nonce_ok = False
    obs["run_integrity"] = bool(obs["run_integrity"] and nonce_ok)
    a = L.get("admission", {})
    # where the tree returns a public key from CheckTx, it is the sender's own on every transaction
    obs["admission_regime"] = (a.get("inner_checktx") == str(EXPECTED_TXS + 1) and a.get("precheck_in_window") == "0"
                               and a.get("admission_mismatch") == "0" and a.get("key_mismatch") == "0"
                               and a.get("key_checks") in ("0", str(EXPECTED_TXS + 1)))
    # the node, not the harness inserter, sets the rate: the fill handed over a backlog before the first block, no
    # steady block began with fewer than two blocks handed over beyond it while the inserter still ran, and blocks
    # queued at the router over the window
    fd, depth = L.get("feed", {}), obs["receive_execute_depth"]
    try:
        feed_ok = (fd.get("gauge_ms") == GAUGE_MS and fd.get("steady_starved_blocks") == "0" and
                   int(fd.get("fill_txs", "0")) >= MIN_FILL_BLOCKS * TXS_PER_BLOCK)
    except ValueError:
        feed_ok = False
    obs["execution_bound"] = bool(feed_ok and depth is not None and depth >= MIN_RECEIVE_EXECUTE_DEPTH)
    sr = checked["state"]
    obs["state_gate"] = bool(sr["rc"] == 0 and sr["verify_pass"] and sr["total"] == CONSTANTS["lthash"] and st.get("flatkv_lthash") == CONSTANTS["lthash"])
    obs["apphash_gate"] = st.get("final_apphash") == CONSTANTS["final_apphash"] and st.get("apphash_digest") == CONSTANTS["apphash_digest"]
    rv = checked["receipts"]["values"]
    obs["receipt_gate"] = bool(
        checked["receipts"]["rc"] == 0 and rv.get("latest") == str(EXPECTED_LATEST) and rv.get("receipts") == str(EXPECTED_TXS) and
        rv.get("success") == str(EXPECTED_TXS) and rv.get("failed") == "0" and
        rv.get("receipt_digest") == CONSTANTS["receipt_digest"] and rv.get("filter_digest") == CONSTANTS["filter_digest"]
    )
    return obs


def aggregate(observations):
    def total(key):
        vals = [o.get(key) for o in observations]
        return None if any(v is None for v in vals) else sum(vals)

    values, witness = {}, {}
    st, sw = total("steady_txs"), total("steady_wall_s")
    values["steady_txps"] = st / sw if st and sw else None
    # a second count of the same figure: blocks x block size over blocks x the printed per-block time
    wb = [(o.get("steady_blocks"), o.get("steady_ms_per_block")) for o in observations]
    if all(b and m for b, m in wb):
        witness["steady_txps"] = sum(b * TXS_PER_BLOCK for b, _ in wb) / sum(b * m / 1000.0 for b, m in wb)
    wt, cw = total("window_txs"), total("cpu_window_s")
    values["tx_per_cpu_s"] = wt / cw if wt and cw else None
    cu = [(o.get("window_txs"), o.get("cpu_us_per_tx")) for o in observations]
    if all(t and c for t, c in cu):
        witness["tx_per_cpu_s"] = sum(t for t, _ in cu) / sum(t * c / 1e6 for t, c in cu)
    le, lt = total("loop_execution_s"), total("loop_total_s")
    values["executor_share"] = le / lt if le is not None and lt else None
    lm = [(o.get("window_blocks"), o.get("loop_execution_ms"), o.get("loop_total_ms")) for o in observations]
    if all(b and e is not None and t for b, e, t in lm):
        witness["executor_share"] = sum(b * e for b, e, _ in lm) / sum(b * t for b, _, t in lm)
    blocks = total("window_blocks")
    for figure, key in (("loop_execution_ms", "loop_execution_s"), ("loop_storage_ms", "loop_storage_s"), ("prepare_parse_ms", "prepare_parse_s")):
        v = total(key)
        values[figure] = 1000.0 * v / blocks if v is not None and blocks else None
    witness["loop_execution_ms"] = (sum(b * e for b, e, _ in lm) / sum(b for b, _, _ in lm)) if all(b and e is not None for b, e, _ in lm) else None
    witness["loop_storage_ms"] = values["loop_storage_ms"]
    witness["prepare_parse_ms"] = values["prepare_parse_ms"]
    # the node process's lifetime high-water mark, read by this wrapper from wait4, not printed by the node
    rr = [o.get("rusage_maxrss_mb") for o in observations]
    values["peak_rss_mb"] = max(rr) if all(r is not None for r in rr) else None
    witness["peak_rss_mb"] = max(r * 1024.0 for r in rr) / 1024.0 if values["peak_rss_mb"] is not None else None
    # a variant that serves no calls reads 0 on both call figures
    if all(o.get("variant") == "off" for o in observations):
        values["call_answered_share"] = witness["call_answered_share"] = 0.0
        values["call_p99_ms"] = witness["call_p99_ms"] = 0.0
    else:
        off, ans = total("call_offered"), total("call_answered_in_span")
        values["call_answered_share"] = ans / off if off and ans is not None else None
        # a second count: every offered call ends in exactly one of these classes
        rest = [total(k) for k in ("call_answered_late", "call_refused", "call_http_errors", "call_bad_return", "call_unfinished")]
        witness["call_answered_share"] = (off - sum(rest)) / off if off and all(r is not None for r in rest) else None
        lp = [o.get("call_lat_ms_p99") for o in observations]
        values["call_p99_ms"] = max(lp) if lp and all(v is not None and v >= 0 for v in lp) else None
        witness["call_p99_ms"] = values["call_p99_ms"]
    for gate in GATES:
        values[gate] = all(bool(o.get(gate)) for o in observations)
    return values, witness


def figures_of(values):
    """The reported figures: the aggregate's values, with the answered share in percent (a fixed rescale, so the paired
    absolute change reads in percentage points)."""
    out = {k: values.get(k) for k in FIGURES if k != "call_answered_pct"}
    share = values.get("call_answered_share")
    out["call_answered_pct"] = None if share is None else 100.0 * share
    return out


# ------------------------------------------------------------------------------------------------
# modes

def setup(tree, out):
    os.makedirs(out, exist_ok=True)
    env = common.go_env(tree)
    common.place_harness(os.path.join(HERE, "harness", HARNESS), tree, PKG, HARNESS)
    binary = os.path.join(out, BIN_NAME)
    subprocess.run(["go", "test", "-c", "-tags", "benchharness", "-o", binary, "./" + PKG + "/"], cwd=tree, env=env, check=True)
    print(f"built {binary}")
    return 0


def measure(tree, out, tools, workload):
    variant = variant_of(workload)
    binary = os.path.join(out, BIN_NAME)
    result = {"workload": workload, "variant": variant, "figure_spec": FIGURES, "primary": PRIMARY}
    problems = []
    if not os.access(binary, os.X_OK):
        problems.append(f"no test binary at {binary}: setup did not build it")
    for t in (common.SEIDB, common.RECEIPT_READER):
        if not os.access(os.path.join(tools, t), os.X_OK):
            problems.append(f"no {t} under {tools}: the tools step did not build it")
    if problems:
        result.update({"figures": {k: None for k in FIGURES}, "gates": {g: False for g in GATES}, "problems": problems})
        print("RESULT " + json.dumps(result, sort_keys=True))
        return 0
    quiescent, quiescence = wait_for_quiescence()
    result["host"] = {"idle_before": quiescent, "detail": quiescence}
    observations = []
    for repeat in range(REPEATS):
        run = run_process(binary, variant)
        try:
            checked = validate_process(tools, run)
            observation = process_observation(run, checked)
        finally:
            remove_store(run["store"])
        observation["repeat"] = repeat
        observations.append(observation)
        tail = "\n".join(line for line in (run["stdout"] + "\n" + run.get("client_stdout", "")).splitlines()
                         if line.startswith("NODE1 ") or line.startswith("CALLQCLIENT") or "FAIL" in line or "panic" in line)
        print(tail)
        with open(os.path.join(out, "call-stdout.txt"), "a") as fh:
            fh.write(f"=== {workload} repeat={repeat} rc={run['rc']}\n{tail}\n")
    values, witness = aggregate(observations)
    disputes = common.apply_witness(values, witness, WITNESS_TOLERANCE, WITNESSED)
    obs = [{k: v for k, v in o.items() if k != "store"} for o in observations]
    with open(os.path.join(out, "call-observations.jsonl"), "a") as fh:
        fh.write("\n".join(json.dumps(o, sort_keys=True, default=str) for o in obs) + "\n")
    last = observations[-1]
    rv = (last.get("receipt_reader") or {}).get("values") or {}
    result.update({
        "figures": figures_of(values),
        "gates": {g: bool(values[g]) for g in GATES},
        "witness": {k: v for k, v in witness.items() if v is not None},
        "witness_disputes": disputes,
        "digests": {"final_apphash": last.get("final_apphash"), "apphash_digest": last.get("apphash_digest"),
                    "lthash": (last.get("state_reader") or {}).get("total"),
                    "receipt_digest": rv.get("receipt_digest"), "filter_digest": rv.get("filter_digest")},
        "observations": obs,
    })
    print("RESULT " + json.dumps(result, sort_keys=True, default=str))
    return 0


def self_test():
    assert variant_of("rpc-call") == "call" and variant_of("rpc-call:off") == "off" and variant_of("rpc-call:call") == "call"
    assert variant_of("rpc-call:self") is None and variant_of("rpc-receipt") is None and variant_of(None) is None
    assert proc_cpu_totals("cpu  10 0 5 80 5 0 0 0 0 0\n") == (100, 85)
    assert cpu_idle_fraction((100, 85), (200, 180)) == 0.95
    assert quiescence_held([0.95, 0.96]) and not quiescence_held([0.96, 0.94])
    C = CONSTANTS

    def fixture(gas=C["gas"], apphash=C["final_apphash"], lthash=C["lthash"], inner=EXPECTED_TXS + 1, depth=24.5, ok=EXPECTED_TXS,
                starved=0, fill=60001, nonce_ahead=0, nonce_checked=3750):
        return "\n".join([
            "noise",
            "NODE1 cfg workload=erc20-transfer admit=self txs=2000 blocks=120 gomaxprocs=16",
            f"NODE1 admission presign_s=1 precheck_in_window=0 admission_mismatch=0 inner_checktx={inner} insert_s=3 key_field=true key_checks={inner} key_mismatch=0 admitter_checktx=0 carried_bytes=0",
            "NODE1 window blocks=119 txs=238000 wall_s=2.000000 blocks_per_s=59.5 tx_per_s=119000 ms_per_block=16.807",
            "NODE1 steady from_height=14 blocks=107 txs=214000 wall_s=1.712000 blocks_per_s=62.5 tx_per_s=125000 ms_per_block=16.000000",
            f"NODE1 feed fill_s=0.400 fill_txs={fill} gauge_ms=20 steady_blocks=107 steady_fed_blocks=90 steady_after_insert_blocks=17 steady_starved_blocks={starved} steady_min_ahead_blocks=3.10 steady_mean_ahead_blocks=12.00",
            f"NODE1 feeddiag win_insert_calls=180000 nonce_table=true nonce_checked={nonce_checked} nonce_behind=0 nonce_ahead={nonce_ahead}",
            f"NODE1 success ok={ok} failed=0 expected={EXPECTED_TXS} receipts_ok={ok} receipts_failed=0 receipts_missing=0 gas_used={gas} first_failure=\"\"",
            f"NODE1 state final_apphash={apphash} apphash_digest={C['apphash_digest']} blocks=120 first_height=2 last_height=121 block_txs_min=2000 block_txs_max=2000 flatkv_version=121 flatkv_lthash={lthash}",
            "NODE1 proc cpu_window_s=23.800000 cpu_us_per_tx=100.0000 cores_busy=11.9 peak_rss_mb=900 gc_window=60 gc_total=90",
            "NODE1 wphase autobahn_main_loop_phase_duration_seconds_total phase=execution total_s=1.500000 ms_per_block=12.605042",
            "NODE1 wphase autobahn_main_loop_phase_duration_seconds_total phase=storage total_s=0.500000 ms_per_block=4.201681",
            "NODE1 wphase evmonly_prepare_phase_duration_seconds_total phase=parse total_s=0.900000 ms_per_block=7.563025",
            f"NODE1 gauge depth receive-execute n=100 mean={depth} min=0 max=30",
        ]) + "\n"

    def client_fixture(offered=8000, answered=7900, refused=0, late=100, http=0, bad=0, unfinished=0, mismatched=0,
                       unrecorded=0, heights=N_HEIGHTS, matched=None, ended="true"):
        matched = answered + late - mismatched - unrecorded if matched is None else matched
        return (f"CALLQCLIENT every_us=500 span_ended={ended} span_s=4.000 offered={offered} answered_in_span={answered} refused={refused} "
                f"http_errors={http} bad_return={bad} outstanding_at_end={late} answered_late={late} unfinished={unfinished}\n"
                f"CALLQCLIENT recorded_heights={heights} heights_answered=120 matched={matched} mismatched={mismatched} "
                f"unrecorded={unrecorded} lat_ms_p50=0.1 lat_ms_p90=3.0 lat_ms_p99=6.0 lat_ms_max=12.0 client_cpu_s=1.0\n")

    good_state = {"rc": 0, "total": C["lthash"], "verify_pass": True}
    good_receipts = {"rc": 0, "values": {"latest": str(EXPECTED_LATEST), "receipts": str(EXPECTED_TXS), "success": str(EXPECTED_TXS), "failed": "0",
                                         "receipt_digest": C["receipt_digest"], "filter_digest": C["filter_digest"]}}

    def obs(variant="off", state=good_state, receipts=good_receipts, rc=0, client=None, client_rc=None, **kw):
        serving = variant != "off"
        run = {"variant": variant, "rc": rc, "wall_s": 10.0, "rusage_maxrss_kb": 900 * 1024, "stdout": fixture(**kw), "store": Path("x"),
               "client_stdout": (client_fixture() if client is None else client) if serving else (client or ""),
               "client_rc": (0 if client_rc is None else client_rc) if serving else client_rc}
        return process_observation(run, {"parsed": parse_node1(run["stdout"]), "state": state, "receipts": receipts})

    for variant in VARIANTS:
        values, witness = aggregate([obs(variant)])
        assert all(values[g] for g in GATES), (variant, values)
        assert abs(values["steady_txps"] - 125000) < 1e-6 and abs(witness["steady_txps"] - 125000) < 1e-6
        if variant == "off":
            assert values["call_answered_share"] == 0.0 and values["call_p99_ms"] == 0.0
        else:
            assert abs(values["call_answered_share"] - 7900 / 8000) < 1e-12 and abs(witness["call_answered_share"] - 7900 / 8000) < 1e-12
            assert values["call_p99_ms"] == 6.0
            assert abs(figures_of(values)["call_answered_pct"] - 98.75) < 1e-9
    # most calls refused: the gate still holds and the figure reads the refusals
    values, witness = aggregate([obs("call", client=client_fixture(answered=300, refused=7690, late=10))])
    assert values["call_gate"] and abs(values["call_answered_share"] - 300 / 8000) < 1e-12 and abs(witness["call_answered_share"] - 300 / 8000) < 1e-12
    for bad in (dict(client=client_fixture(mismatched=1)), dict(client=client_fixture(http=1, answered=7899)),
                dict(client=client_fixture(bad=1, answered=7899)), dict(client=client_fixture(unfinished=1, answered=7899)),
                dict(client=client_fixture(unrecorded=1)), dict(client=client_fixture(heights=119)),
                dict(client=client_fixture(offered=1999, answered=1899)), dict(client=client_fixture(ended="false")),
                dict(client=client_fixture(answered=0, late=0, refused=8000, matched=0)),
                dict(client_rc=1), dict(client="")):
        values, _ = aggregate([obs("call", **bad)])
        assert not values["call_gate"], bad
    values, _ = aggregate([obs("off", client=client_fixture(), client_rc=0)])
    assert not values["call_gate"], "off with a client"
    for gate, kw in {"run_integrity": dict(gas=C["gas"] + 1), "apphash_gate": dict(apphash="00" * 32),
                     "admission_regime": dict(inner=5), "execution_bound": dict(depth=0.5)}.items():
        for variant in VARIANTS:
            values, _ = aggregate([obs(variant, **kw)])
            assert not values[gate], (gate, variant)
    for kw in (dict(starved=1), dict(fill=1999)):
        values, _ = aggregate([obs(**kw)])
        assert not values["execution_bound"], kw
    values, _ = aggregate([obs(state={"rc": 0, "total": "ab" * 32, "verify_pass": True})])
    assert not values["state_gate"]
    values, _ = aggregate([obs(receipts={"rc": 0, "values": dict(good_receipts["values"], receipt_digest="cd" * 32)})])
    assert not values["receipt_gate"]
    values, _ = aggregate([obs(rc=1)])
    assert not values["run_integrity"]
    values, witness = aggregate([obs("call")])
    assert common.apply_witness(values, witness, WITNESS_TOLERANCE, WITNESSED) == [] and values["steady_txps"] is not None
    values, witness = aggregate([obs("call")])
    witness["steady_txps"] *= 1.01
    d = common.apply_witness(values, witness, WITNESS_TOLERANCE, WITNESSED)
    assert [x["figure"] for x in d] == ["steady_txps"] and values["steady_txps"] is None
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
    if a.mode == "teardown":
        return run_teardown()
    if a.mode in ("check", "assets", "setup", "measure") and variant_of(a.workload) is None:
        print(f"unknown workload {a.workload!r}; one of rpc-call, " + ", ".join(f"rpc-call:{v}" for v in VARIANTS), file=sys.stderr)
        return 2
    if a.mode == "check":
        return 0
    if a.mode == "assets":
        print("")
        return 0
    if a.mode == "tools":
        common.build_tools(os.path.abspath(a.tree), os.path.abspath(a.out))
        return 0
    tree, out = os.path.abspath(a.tree), os.path.abspath(a.out)
    if a.mode == "setup":
        return setup(tree, out)
    return measure(tree, out, os.path.abspath(a.tools), a.workload)


if __name__ == "__main__":
    sys.exit(main())
