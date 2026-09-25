#!/usr/bin/env python3
"""RPC benchmark: a node that executes blocks while serving its users' JSON-RPC reads, read at its execute loop.

One process of the sei-tendermint/internal/p2p test binary (bench_rpc_test.go copied into that package under the
`benchharness` build tag) runs a one-validator Autobahn EVM-only node in process (the real evmonlyapp, Giga executor,
FlatKV, littidx receipt store, fsynced block store and hash vault, runExecute) on 120 blocks of 2,000 presigned
ERC-20 transfers it admitted itself through its own CheckTx, with its real EVM-only JSON-RPC server up. A separate
client process (GOMAXPROCS 6, 256 connections) follows eth_blockNumber and makes the variant's requests for executed
transactions whatever the node's progress on earlier ones (open loop). The node holds its server after the window
until the client has drained, outside every timed figure. After the node exits, its store is checked by readers
built from the base tree.

Variants (compare.sh rpc-receipt[:variant]):
  serve     every transaction's receipt, requested once 1 s after its block is first reported (1 lookup per tx)
  serve10   the same for every tenth transaction (0.1 lookups per tx)
  off       no server and no client
  latest10  one eth_getBlockByNumber("latest", false) per tenth executed transaction
  wallet, wallet10  one wallet flow (eth_getBlockByNumber latest, eth_getTransactionCount, eth_getTransactionByHash,
            eth_getTransactionReceipt) per executed transaction, or per tenth
  call20    one eth_call at "latest" per twentieth executed transaction

Figures: steady_txps, tx_per_cpu_s, loop execution and storage ms per block, node peak RSS (wait4), the client's
answer latency p50 and p99 from each request's due time, served_pct (answered, not refused, over requested) and
span_outstanding_pct (requests due in the node's steady span still unanswered at its end).

Gates: run_integrity, admission_regime, execution_bound, state_gate (LtHash recomputed by seidb), apphash_gate,
receipt_gate (receipt and log digests), rpc_gate (every request made and answered, every answer's blockHash equal to
the node's own BlockByNumber hash for that height, the answer digests equal to the shipped tree's), client_headroom
(the client used at most 60% of its processors, so the node, not the client, set what was served).
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
TOOLS = ""
BIN_PATH = ""
WITNESS_TOLERANCE = 0.001

REPEATS = 1
BLOCKS = 120
TXS_PER_BLOCK = 2000
EXPECTED_TXS = BLOCKS * TXS_PER_BLOCK
EXPECTED_LATEST = BLOCKS + 1  # genesis state is version 1; blocks run at heights 2..121
MIN_RECEIVE_EXECUTE_DEPTH = 5.0
MIN_NONCE_CHECKS = 1000  # table answers compared with the app's own read, per process
MIN_FILL_BLOCKS = 10  # the fill before the first block hands over at least this many blocks
GAUGE_MS = "20"
PROCESS_TIMEOUT_SECONDS = 400
STORE_PREFIX = "bench-rpc-"

# Pinned outputs of the self-admitted ERC-20 blocks; serving RPC changes none of them.
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
    "serve": dict(BASE, NODE1_RPCQ_LIVE="serve", NODE1_RPCQ_EVERY="1"),
    "serve10": dict(BASE, NODE1_RPCQ_LIVE="serve", NODE1_RPCQ_EVERY="10"),
    "latest10": dict(BASE, NODE1_RPCQ_LIVE="serve", NODE1_RPCQ_EVERY="10"),
    "wallet": dict(BASE, NODE1_RPCQ_LIVE="serve", NODE1_RPCQ_EVERY="1"),
    "wallet10": dict(BASE, NODE1_RPCQ_LIVE="serve", NODE1_RPCQ_EVERY="10"),
    "call20": dict(BASE, NODE1_RPCQ_LIVE="serve", NODE1_RPCQ_EVERY="20"),
}
CLIENT = {"GOMAXPROCS": "6", "NODE1_RPCQ_DELAY_MS": "1000", "NODE1_RPCQ_WORKERS": "256", "NODE1_RPCQ_CLIENT_S": "360",
          "NODE1_RPCQ_FIRST": "2"}
# per variant: the client's method, its delay after a block is first reported, and whether a block's requests are spread
# over the time until the next head advance (independent users) or all due at once
VARIANT_CLIENT = {
    "serve": {"NODE1_RPCQ_METHOD": "receipt", "NODE1_RPCQ_DELAY_MS": "1000", "NODE1_RPCQ_SPREAD": "0"},
    "serve10": {"NODE1_RPCQ_METHOD": "receipt", "NODE1_RPCQ_DELAY_MS": "1000", "NODE1_RPCQ_SPREAD": "0"},
    "latest10": {"NODE1_RPCQ_METHOD": "latest", "NODE1_RPCQ_DELAY_MS": "0", "NODE1_RPCQ_SPREAD": "1"},
    "wallet": {"NODE1_RPCQ_METHOD": "wallet", "NODE1_RPCQ_DELAY_MS": "1000", "NODE1_RPCQ_SPREAD": "1"},
    "wallet10": {"NODE1_RPCQ_METHOD": "wallet", "NODE1_RPCQ_DELAY_MS": "1000", "NODE1_RPCQ_SPREAD": "1"},
    "call20": {"NODE1_RPCQ_METHOD": "call", "NODE1_RPCQ_DELAY_MS": "0", "NODE1_RPCQ_SPREAD": "1"},
}
HOLD_S = 360
# sha256 over every tenth transaction's answer, its blockHash fields blanked, in transaction order, as the shipped
# tree answers
ANSWER_DIGEST10 = "c7b1326b869e83c4b70fb5dd405aab6eb209917f8992fb204187c55ecf37a7c1"
# the same over every tenth transaction's eth_getTransactionByHash answer, as the shipped tree answers
TX_DIGEST10 = "b73f7c2befd88510fd474a436dd5d5830fd7c6d97c02f39de0fbe56e8ff22c13"
N_HEIGHTS = BLOCKS

BIN_NAME = "rpc.test"

GATES = ("run_integrity", "admission_regime", "execution_bound", "state_gate", "apphash_gate", "receipt_gate", "rpc_gate",
         "client_headroom")
# the client may average at most this share of its processors over its run; above it, the client, not the node,
# sets how much of the offered demand is served inside the span
CLIENT_BUSY_MAX_FRAC = 0.6


# ------------------------------------------------------------------------------------------------
# parsing the harness's NODE1 lines

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
    """The client's RPCQ* summary lines, merged."""
    out = {}
    for raw in stdout.splitlines():
        for tag in ("RPCQCLIENT ", "RPCQSPAN ", "RPCQWALLET ", "RPCQLATEST ", "RPCQCALL "):
            if raw.startswith(tag):
                out.update({(tag.strip().lower() + "." + k if tag != "RPCQCLIENT " else k): v
                            for k, v in key_values(raw[len(tag):]).items()})
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
# readers built from the base tree

def run_state_gate(store):
    SEIDB = os.path.join(TOOLS, common.SEIDB)
    proc = subprocess.run(
        [SEIDB, "dump-flatkv", "-d", str(store / "node0/data/state_commit/flatkv"), "--lthash-only"],
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


def run_receipt_reader(store):
    RECEIPT_READER = os.path.join(TOOLS, common.RECEIPT_READER)
    proc = subprocess.run(
        [RECEIPT_READER, "--receipt-dir", str(store / "node0/data/ledger/receipt/littidx"), "--expect-latest", str(EXPECTED_LATEST)],
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

def run_process(variant):
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
    out_path = store / "node1-stdout.txt"
    client, client_out = None, store / "client-stdout.txt"
    with open(out_path, "w") as out:
        proc = subprocess.Popen(
            [BIN_PATH, "-test.run", "^TestNode1$", "-test.count=1", f"-test.timeout={PROCESS_TIMEOUT_SECONDS + HOLD_S}s"],
            env=env, stdout=out, stderr=subprocess.STDOUT,
        )
        if serving:
            cenv = {k: v for k, v in env.items() if k != "NODE1_RPCQ_LIVE"}
            cenv.update(CLIENT)
            cenv.update(VARIANT_CLIENT[variant])
            with open(client_out, "w") as cout:
                client = subprocess.Popen(
                    [BIN_PATH, "-test.run", "^TestRPCQClient$", "-test.count=1", f"-test.timeout={PROCESS_TIMEOUT_SECONDS + HOLD_S}s"],
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


def validate_process(run):
    parsed = parse_node1(run["stdout"])
    L = parsed["lines"]
    state = run_state_gate(run["store"]) if run["rc"] == 0 else {"rc": None, "total": "NONE", "verify_pass": False}
    receipts = run_receipt_reader(run["store"]) if run["rc"] == 0 else {"rc": None, "values": {}}
    return {"parsed": parsed, "state": state, "receipts": receipts}


def process_observation(run, checked):
    parsed, L = checked["parsed"], checked["parsed"]["lines"]
    variant = run["variant"]
    ok = True
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
        "arm_lthash": L.get("state", {}).get("flatkv_lthash"), "gomaxprocs": L.get("cfg", {}).get("gomaxprocs"),
        # engagement of batch verification against carried keys: an observation, never a gate
        "carried_batches_verified": parsed["count"].get("giga_evmonly_carried_key_batches_total outcome=verified", 0),
        "carried_batches_fallback": parsed["count"].get("giga_evmonly_carried_key_batches_total outcome=fallback", 0),
        # bytes the admitting validator's CheckTx reported beyond the fields every tree carries: the sidecar's size
        "carried_bytes": int(L.get("admission", {}).get("carried_bytes", "0") or 0),
    })

    client = parse_client(run.get("client_stdout", ""))
    obs["client"] = client
    for key in ("lat_ms_p50", "lat_ms_p90", "lat_ms_p99", "lat_ms_max", "client_cpu_s", "elapsed_s"):
        try:
            obs["rpc_" + key] = float(client[key])
        except (KeyError, ValueError):
            obs["rpc_" + key] = None
    if variant != "off":
        obs.update(client_observation(variant, client))
        obs["rpc_gate"] = bool(run.get("client_rc") == 0 and rpc_gate(variant, client))
        obs["client_headroom"] = client_headroom(obs.get("rpc_client_cpu_s"), obs.get("rpc_elapsed_s"))
    else:
        obs["rpc_gate"] = run.get("client_rc") is None and not client
        obs["client_headroom"] = True
    s, st = L.get("success", {}), L.get("state", {})
    obs["run_integrity"] = bool(
        run["rc"] == 0 and L.get("cfg", {}).get("workload") == "erc20-transfer" and s.get("ok") == str(EXPECTED_TXS) and s.get("failed") == "0" and
        s.get("receipts_ok") == str(EXPECTED_TXS) and s.get("gas_used") == str(CONSTANTS["gas"]) and
        st.get("blocks") == str(BLOCKS) and st.get("block_txs_min") == str(TXS_PER_BLOCK) and
        st.get("block_txs_max") == str(TXS_PER_BLOCK) and steady_txs and steady_wall and window_txs and cpu_window and loop_total
    )
    # the producer's app nonce reads were answered from the harness table, which never read above the app's own
    # answer on the calls checked against it
    nd = L.get("feeddiag", {})
    try:
        nonce_ok = (nd.get("nonce_table") == "true" and nd.get("nonce_ahead") == "0" and
                    int(nd.get("nonce_checked", "0")) >= MIN_NONCE_CHECKS)
    except ValueError:
        nonce_ok = False
    obs["run_integrity"] = bool(obs["run_integrity"] and nonce_ok)
    a = L.get("admission", {})
    if VARIANTS[variant]["NODE1_ADMIT"] == "self":
        # where the tree returns a public key from CheckTx, it is the sender's own on every tx
        obs["admission_regime"] = (a.get("inner_checktx") == str(EXPECTED_TXS + 1) and a.get("precheck_in_window") == "0"
                                   and a.get("admission_mismatch") == "0" and a.get("key_mismatch") == "0"
                                   and a.get("key_checks") in ("0", str(EXPECTED_TXS + 1)))
    else:
        obs["admission_regime"] = a.get("inner_checktx") == "0" and a.get("admission_mismatch") == "0"
        if variant == "carried":
            # the admitting validator: a second application's real CheckTx on every tx, all before the window
            obs["admission_regime"] = (obs["admission_regime"] and a.get("admitter_checktx") == str(EXPECTED_TXS + 1)
                                       and a.get("precheck_in_window") == "0")
    # the node, not the harness inserter, sets the rate: the fill handed over a backlog before the first block, no steady
    # block began with fewer than two blocks handed over beyond it while the inserter still ran, and blocks queued at the
    # router over the window
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


def client_int(client, key):
    try:
        return int(client[key])
    except (KeyError, ValueError):
        return None


def client_observation(variant, client):
    """What the variant's requests got: served (answered, not refused) over requested, and the node's steady span's
    requests due, answered within it and still outstanding at its end."""
    method = VARIANT_CLIENT[variant]["NODE1_RPCQ_METHOD"]
    requested = client_int(client, "requested")
    served = client_int(client, "rpcqcall.checked") if method == "call" else client_int(client, "answered")
    return {
        "rpc_requested": requested, "rpc_served": served,
        "rpc_refused": client_int(client, "rpcqcall.refused") if method == "call" else 0,
        "rpc_span_due": client_int(client, "rpcqspan.due"),
        "rpc_span_answered": client_int(client, "rpcqspan.answered_in_span"),
        "rpc_span_outstanding": client_int(client, "rpcqspan.outstanding_at_end"),
    }


def client_headroom(cpu_s, elapsed_s):
    """True when the client's CPU over its run stays within CLIENT_BUSY_MAX_FRAC of its processors."""
    if cpu_s is None or not elapsed_s:
        return False
    return cpu_s / elapsed_s <= CLIENT_BUSY_MAX_FRAC * int(CLIENT["GOMAXPROCS"])


def rpc_gate(variant, client):
    """Every request of the variant's demand was made and returned, and every answer checked is the one the shipped
    tree gives, its block hash the node's own for that height."""
    method = VARIANT_CLIENT[variant]["NODE1_RPCQ_METHOD"]
    every = int(VARIANTS[variant]["NODE1_RPCQ_EVERY"])
    want = (EXPECTED_TXS + every - 1) // every
    c = lambda key: client.get(key)
    common = (c("every") == str(every) and c("requested") == str(want) and c("answered") == str(want) and
              c("unanswered") == "0" and c("wrong_block") == "0" and c("node_heights") == str(N_HEIGHTS) and
              c("hash_mismatch") == "0" and c("rpcqspan.due") not in (None, "0"))
    receipts = c("answer_digest10") == ANSWER_DIGEST10 and c("hash_checked") == str(want)
    latest = c("rpcqlatest.checked") == str((want + 24) // 25) and c("rpcqlatest.mismatch") == "0"
    if method == "receipt":
        return common and receipts
    if method == "latest":
        return common and latest
    if method == "wallet":
        return (common and receipts and latest and c("rpcqwallet.tx_checked") == str(want) and
                c("rpcqwallet.tx_mismatch") == "0" and c("rpcqwallet.count_mismatch") == "0" and
                c("rpcqwallet.tx_digest10") == TX_DIGEST10)
    if method == "call":
        checked, refused = client_int(client, "rpcqcall.checked"), client_int(client, "rpcqcall.refused")
        # every call returned an answer or a refusal; some answers were checked, and none was another block's state
        return bool(common and checked is not None and refused is not None and checked + refused == want and
                    checked >= want // 100 and c("rpcqcall.state_mismatch") == "0")
    return False


def aggregate(observations):
    def total(key):
        vals = [o.get(key) for o in observations]
        return None if any(v is None for v in vals) else sum(vals)

    values, witness = {}, {}
    st, sw = total("steady_txs"), total("steady_wall_s")
    values["steady_txps"] = st / sw if st and sw else None
    # a second count of the same row: blocks x block size over blocks x the printed per-block time
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
    for view, key in (("loop_execution_ms", "loop_execution_s"), ("loop_storage_ms", "loop_storage_s"), ("prepare_parse_ms", "prepare_parse_s")):
        v = total(key)
        values[view] = 1000.0 * v / blocks if v is not None and blocks else None
    witness["loop_execution_ms"] = (sum(b * e for b, e, _ in lm) / sum(b for b, _, _ in lm)) if all(b and e is not None for b, e, _ in lm) else None
    witness["loop_storage_ms"] = values["loop_storage_ms"]
    witness["prepare_parse_ms"] = values["prepare_parse_ms"]
    # the node process's lifetime high-water mark, read by this wrapper from wait4
    rr = [o.get("rusage_maxrss_mb") for o in observations]
    values["peak_rss_mb"] = max(rr) if all(r is not None for r in rr) else None
    witness["peak_rss_mb"] = max(r * 1024.0 for r in rr) / 1024.0 if values["peak_rss_mb"] is not None else None
    lp = [o.get("rpc_lat_ms_p99") for o in observations]
    # a variant that serves nothing reads 0 here
    values["rpc_lat_p99_ms"] = max(lp) if lp and all(v is not None for v in lp) else (0.0 if all(o.get("variant") == "off" for o in observations) else None)
    witness["rpc_lat_p99_ms"] = values["rpc_lat_p99_ms"]
    lp50 = [o.get("rpc_lat_ms_p50") for o in observations]
    values["rpc_lat_p50_ms"] = max(lp50) if lp50 and all(v is not None for v in lp50) else (0.0 if all(o.get("variant") == "off" for o in observations) else None)
    witness["rpc_lat_p50_ms"] = values["rpc_lat_p50_ms"]
    # requests served (answered, not refused) over requests made; a variant that serves nothing reads 100
    if all(o.get("variant") == "off" for o in observations):
        values["served_pct"] = 100.0
        values["span_outstanding_pct"] = 0.0
    else:
        rq, sv = total("rpc_requested"), total("rpc_served")
        values["served_pct"] = 100.0 * sv / rq if rq and sv is not None else None
        rf = total("rpc_refused")
        witness["served_pct"] = 100.0 * (rq - rf) / rq if rq and rf is not None else None
        due, out = total("rpc_span_due"), total("rpc_span_outstanding")
        values["span_outstanding_pct"] = 100.0 * out / due if due and out is not None else None
        ans = total("rpc_span_answered")
        witness["span_outstanding_pct"] = 100.0 * (due - ans) / due if due and ans is not None else None
    witness.setdefault("served_pct", values["served_pct"])
    witness.setdefault("span_outstanding_pct", values["span_outstanding_pct"])
    for gate in GATES:
        values[gate] = all(bool(o.get(gate)) for o in observations)
    return values, witness


def self_test():
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

    def client_fixture(variant, every=None, answered=None, wrong=0, digest=None, requested=None, mismatch=0, heights=N_HEIGHTS, checked=None,
                       due=20000, tx_mismatch=0, tx_digest=TX_DIGEST10, count_mismatch=0, latest_mismatch=0, latest_checked=None,
                       call_checked=None, call_refused=0, state_mismatch=0, client_cpu=9.0):
        every = int(VARIANTS[variant]["NODE1_RPCQ_EVERY"]) if every is None else every
        want = (EXPECTED_TXS + every - 1) // every
        requested = want if requested is None else requested
        answered = requested if answered is None else answered
        digest = ANSWER_DIGEST10 if digest is None else digest
        checked = answered if checked is None else checked
        method = VARIANT_CLIENT[variant]["NODE1_RPCQ_METHOD"]
        extra = f"RPCQSPAN method={method} span_s=2.0 due={due} answered_in_span={due - 10} outstanding_at_end=10\n"
        if method == "wallet":
            extra += f"RPCQWALLET tx_checked={answered} tx_mismatch={tx_mismatch} count_mismatch={count_mismatch} tx_digest10={tx_digest}\n"
        if method in ("wallet", "latest"):
            lc = (want + 24) // 25 if latest_checked is None else latest_checked
            extra += f"RPCQLATEST checked={lc} mismatch={latest_mismatch}\n"
        if method == "call":
            cc = answered - call_refused if call_checked is None else call_checked
            extra += f'RPCQCALL checked={cc} state_mismatch={state_mismatch} refused={call_refused} first_refusal="x y"\n'
        return extra + (f"RPCQCLIENT node_heights={heights} hash_checked={checked} hash_mismatch={mismatch}\n"
                f"RPCQCLIENT every={every} requested={requested} answer_digest={'ef' * 32} answer_digest10={digest}\n"
                f"RPCQCLIENT txs={EXPECTED_TXS} answered={answered} unanswered={requested - answered} wrong_block={wrong} http_errors=0 null_retries=0 delay_ms=1000 workers=256 lat_ms_p50=300.0 lat_ms_p90=500.0 lat_ms_p99=600.0 lat_ms_max=700.0 elapsed_s=7.0 client_cpu_s={client_cpu}\n")

    good_state = {"rc": 0, "total": C["lthash"], "verify_pass": True}
    good_receipts = {"rc": 0, "values": {"latest": str(EXPECTED_LATEST), "receipts": str(EXPECTED_TXS), "success": str(EXPECTED_TXS), "failed": "0",
                                         "receipt_digest": C["receipt_digest"], "filter_digest": C["filter_digest"]}}

    def obs(variant="off", state=good_state, receipts=good_receipts, rc=0, client=None, client_rc=None, **kw):
        serving = variant != "off"
        run = {"variant": variant, "rc": rc, "wall_s": 10.0, "rusage_maxrss_kb": 900 * 1024, "stdout": fixture(**kw), "store": Path("x"),
               "client_stdout": (client_fixture(variant) if client is None else client) if serving else (client or ""),
               "client_rc": (0 if client_rc is None else client_rc) if serving else client_rc}
        return process_observation(run, {"parsed": parse_node1(run["stdout"]), "state": state, "receipts": receipts})

    for variant in VARIANTS:
        values, witness = aggregate([obs(variant)])
        assert all(values[g] for g in GATES), (variant, values)
        assert abs(values["steady_txps"] - 125000) < 1e-6 and abs(witness["steady_txps"] - 125000) < 1e-6
        if variant != "off":
            assert values["rpc_lat_p99_ms"] == 600.0
    for variant in VARIANTS:
        if variant == "off":
            continue
        method = VARIANT_CLIENT[variant]["NODE1_RPCQ_METHOD"]
        bads = [dict(answered=5), dict(wrong=1), dict(every=3), dict(mismatch=1), dict(heights=119), dict(due=0)]
        if method in ("receipt", "wallet"):
            bads += [dict(digest="ab" * 32), dict(checked=7)]
        if method in ("latest", "wallet"):
            bads += [dict(latest_mismatch=1), dict(latest_checked=3)]
        if method == "wallet":
            bads += [dict(tx_mismatch=1), dict(tx_digest="ab" * 32), dict(count_mismatch=1)]
        if method == "call":
            bads += [dict(state_mismatch=1), dict(call_checked=10), dict(call_checked=100, call_refused=100)]
        for bad in [dict(client=client_fixture(variant, **b)) for b in bads] + [dict(client_rc=1), dict(client="")]:
            values, _ = aggregate([obs(variant, **bad)])
            assert not values["rpc_gate"], (variant, bad)
    # a client near saturation fails the run: 6 procs above 60% (26 CPU-s over 7 s); 17.5 CPU-s, which failed the
    # 4-proc client, holds at 6
    for variant in VARIANTS:
        if variant == "off":
            continue
        values, _ = aggregate([obs(variant, client=client_fixture(variant, client_cpu=26.0))])
        assert values["rpc_gate"] and not values["client_headroom"], (variant, "saturated client")
        values, _ = aggregate([obs(variant, client=client_fixture(variant, client_cpu=17.5))])
        assert values["client_headroom"], (variant, "a 4-proc client's saturation holds at 6 procs")
        values, _ = aggregate([obs(variant, client=client_fixture(variant, client_cpu=16.5))])
        assert values["client_headroom"], (variant, "client within headroom")
    assert not client_headroom(None, 7.0) and not client_headroom(9.0, 0)
    # a refused call is returned, not served: served_pct reads what the callers got
    values, witness = aggregate([obs("call20", client=client_fixture("call20", call_refused=9000))])
    assert values["rpc_gate"] and abs(values["served_pct"] - 25.0) < 1e-9 and abs(witness["served_pct"] - 25.0) < 1e-9, values
    values, _ = aggregate([obs("wallet")])
    assert values["served_pct"] == 100.0 and abs(values["span_outstanding_pct"] - 0.05) < 1e-9, values
    values, _ = aggregate([obs("off", client=client_fixture("serve"), client_rc=0)])
    assert not values["rpc_gate"], "off with a client"
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
    print("SELF_TEST ok")
    return 0



FIGURES = {
    "steady_txps": {"unit": "tx/s", "better": "higher", "effect": "relative"},
    "tx_per_cpu_s": {"unit": "tx/cpu-s", "better": "higher", "effect": "relative"},
    "loop_execution_ms": {"unit": "ms/block", "better": "lower", "effect": "relative"},
    "loop_storage_ms": {"unit": "ms/block", "better": "lower", "effect": "relative"},
    "peak_rss_mb": {"unit": "MB", "better": "lower", "effect": "absolute"},
    "rpc_lat_p50_ms": {"unit": "ms", "better": "lower", "effect": "relative"},
    "rpc_lat_p99_ms": {"unit": "ms", "better": "lower", "effect": "relative"},
    "served_pct": {"unit": "% of requests", "better": "higher", "effect": "absolute"},
    "span_outstanding_pct": {"unit": "% of requests due", "better": "lower", "effect": "absolute"},
}
WITNESSED = ("steady_txps", "tx_per_cpu_s", "loop_execution_ms", "loop_storage_ms", "peak_rss_mb", "rpc_lat_p99_ms",
             "served_pct", "span_outstanding_pct", "rpc_lat_p50_ms")


def resolve(workload):
    name, _, variant = (workload or "").partition(":")
    if name != "rpc-receipt":
        return None
    variant = variant or "serve"
    return variant if variant in VARIANTS else None


def setup(tree, out):
    os.makedirs(out, exist_ok=True)
    common.place_harness(os.path.join(HERE, "harness", "bench_rpc_test.go"), tree, PKG, "bench_rpc_test.go")
    env = common.go_env(tree)
    binary = os.path.join(out, BIN_NAME)
    subprocess.run(["go", "test", "-c", "-tags", "benchharness", "-o", binary, "./" + PKG + "/"], cwd=tree, env=env, check=True)
    print(f"built {binary}")
    return 0


def measure(out, variant, workload):
    result = {"workload": workload, "figure_spec": FIGURES, "primary": "steady_txps", "repeats": REPEATS}
    missing = [p for p in (BIN_PATH, os.path.join(TOOLS, common.SEIDB), os.path.join(TOOLS, common.RECEIPT_READER)) if not os.access(p, os.X_OK)]
    if missing:
        result.update({"figures": {k: None for k in FIGURES}, "gates": {g: False for g in GATES}, "problems": [f"missing {p}" for p in missing]})
        print("RESULT " + json.dumps(result, sort_keys=True))
        return 0
    observations = []
    for repeat in range(REPEATS):
        run = run_process(variant)
        try:
            checked = validate_process(run)
            observation = process_observation(run, checked)
        finally:
            remove_store(run["store"])
        observation["repeat"] = repeat
        observations.append(observation)
        tail = "\n".join(line for line in (run["stdout"] + "\n" + run.get("client_stdout", "")).splitlines()
                         if line.startswith("NODE1 ") or line.startswith("RPCQ") or "FAIL" in line or "panic" in line)
        print(tail)
    values, witness = aggregate(observations)
    figures = {k: values.get(k) for k in FIGURES}
    disputes = common.apply_witness(figures, witness, WITNESS_TOLERANCE, [k for k in WITNESSED if witness.get(k) is not None or k in ("steady_txps", "tx_per_cpu_s")])
    gates = {g: bool(values.get(g)) for g in GATES}
    result.update({"figures": figures, "gates": gates, "witness": {k: v for k, v in witness.items() if v is not None},
                   "witness_disputes": disputes, "observations": observations, "host": {"loadavg_after": list(os.getloadavg())}})
    print("RESULT " + json.dumps(result, sort_keys=True, default=str))
    return 0


def main():
    global TOOLS, BIN_PATH
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
    if a.mode == "tools":
        common.build_tools(os.path.abspath(a.tree), os.path.abspath(a.out))
        return 0
    variant = resolve(a.workload)
    if variant is None:
        print(f"unknown rpc workload {a.workload!r}: rpc-receipt[:{'|'.join(VARIANTS)}] (default serve)", file=sys.stderr)
        return 2
    if a.mode == "check":
        return 0
    if a.mode == "assets":
        print("")
        return 0
    if a.mode == "setup":
        return setup(os.path.abspath(a.tree), os.path.abspath(a.out))
    TOOLS = os.path.abspath(a.tools)
    BIN_PATH = os.path.join(os.path.abspath(a.out), BIN_NAME)
    return measure(os.path.abspath(a.out), variant, a.workload)


if __name__ == "__main__":
    sys.exit(main())
