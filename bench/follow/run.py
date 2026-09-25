#!/usr/bin/env python3
"""Fullnode follow benchmark: how much of the chain a real EVM-only fullnode (the node RPC users read from) keeps.

Two harnesses, both copied into sei-tendermint/internal/p2p under the `benchharness` build tag:

bench_committee_test.go (variants committee8, committee16, committee40; `fullsync` alone is committee16)
  One process runs N validators over TCP (fsyncing WALs and LittDB block stores, the p2p package's testApp as the
  application, the shipped BlockInterval 400 ms and ViewTimeout 1.5 s, the shipped MaxInboundFullnodePeers 10) and
  one real fullnode router (NewGigaFullnodeRouter, its own stores and app) dialing the committee. Every validator is
  fed 5 tx/s, so each seals on its 400 ms interval: N validators make about N x 2.5 global blocks/s of about 2
  transactions each (light load: block-rate intake, not bandwidth). 5 s warm-up, then a 30 s span.
  Figures: follow_share = global blocks the fullnode finalized in the span over those validator 0 finalized in it
  (primary); chain_blocks_per_s; fullnode_blocks_per_s; follow_lag_blocks = validator 0's height minus the
  fullnode's at the span's end; process_cores_busy; peak_rss_mb (whole process).
  Gates: run_integrity (exit 0, the three ML lines, a 30 s span, no insert error, at least 98% of the offered
  transactions inserted); chain_regime (validator 0 finalized at least 80% of N x 2.5 blocks/s); agreement_gate (at
  the fullnode's last height its AppHash, a chain over every block hash, and a chained digest of every finalized
  transaction equal validator 0's; its blocks are contiguous with committee proposers; validator N-1 agrees with
  validator 0); follow_engaged (the fullnode finalized a block in the span). Setup also runs committee8 once with one
  transaction dropped from the fullnode's record and fails unless agreement_gate refuses it.

bench_follow_test.go (variants paced50, rtt100, rtt250, serve10)
  One validator's Autobahn EVM-only node in process (real evmonlyapp, Giga executor, FlatKV, receipt store, LittDB
  block store, runExecute), self-admitted ERC-20 transfers.
  paced50: the producer paced at 5,000 tx/s in 100-tx blocks (about 50 global blocks/s), 1,200 blocks, plus one real
    EVM-only fullnode in the same process (own stores, runExecute over its own application, recovering every
    sender) dialing the validator's Giga port over TCP.
  rtt100 / rtt250: paced50's chain at 10,000 tx/s (about 100 global blocks/s), the fullnode dialing through a
    loopback relay on port 29917 whose packets alone netem delays half the round trip each way (Linux, root, tc).
  serve10: 120 saturated blocks of 2,000 transfers with 10 stub fullnodes streaming from the validator: the
    validator's cost of serving followers.
  Figures: follow_share = blocks the fullnode executed inside the validator's steady span over blocks the validator
  finalized in it (primary, except serve10: steady_txps); steady_txps; tx_per_cpu_s; chain_blocks_per_s;
  follow_lag_blocks; stub_share; peak_rss_mb.
  Gates: run_integrity (every transaction successful with a receipt, gas equal to the pinned constant, exact block
  sizes, the harness's nonce table never answered above the application's own read); admission_regime;
  execution_bound (the producer's pace sets the chain's rate within 10%; for rtt variants the relayed connect reads
  the round trip within 10%, loopback ping under 5 ms, and the relay carried the fullnode's blocks; serve10: the node
  sets the rate); state_gate (seidb dump-flatkv, built from the base ref, recomputes the validator's FlatKV LtHash:
  equal to the pinned constant); apphash_gate (final AppHash and the digest over every per-block AppHash equal the
  pinned constants); receipt_gate (a receipt reader built from the base ref: latest height and every receipt
  successful; serve10 also its receipt and log digests); follow_gate (the fullnode executed at least 100 heights,
  each carrying exactly the validator's AppHash; serve10: all 10 stubs connected and committed).

Usage (compare.sh drives it):
  run.py check|assets --workload W
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
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "lib"))
import common  # noqa: E402

HARNESS_DIR = os.path.join(HERE, "harness")
PKG = os.path.join("sei-tendermint", "internal", "p2p")
STORE_PREFIX = "bench-follow-"
# a figure's second, independent count (e.g. blocks over the span's own clock) must agree with it within this share
WITNESS_TOLERANCE = 0.001

COOLDOWN_MIN_SECONDS = 5.0
COOLDOWN_SAMPLE_SECONDS = 5.0
COOLDOWN_MAX_SECONDS = 180.0
COOLDOWN_IDLE_FRACTION = 0.95
COOLDOWN_CONSECUTIVE_WINDOWS = 2

# ------------------------------------------------------------------------------------------------
# committee harness constants

COMMITTEE_BIN = "committee.test"
COMMITTEE_TIMEOUT_SECONDS = 240
WARM_S = 5
SPAN_S = 30
TX_PER_PRODUCER_PER_S = 5
SEALS_PER_PRODUCER_PER_S = 2.5  # BlockInterval 400 ms
CHAIN_FLOOR_SHARE = 0.80  # the chain must run at least this share of the seal rate its variant states
OFFERED_FLOOR_SHARE = 0.98
COMMITTEE = {"committee8": 8, "committee16": 16, "committee40": 40}
COMMITTEE_GATES = ("run_integrity", "chain_regime", "agreement_gate", "follow_engaged")
COMMITTEE_FIGURES = {
    "follow_share": {"unit": "share of the chain's blocks", "better": "higher", "effect": "absolute"},
    "chain_blocks_per_s": {"unit": "blocks/s", "better": "higher", "effect": "relative"},
    "fullnode_blocks_per_s": {"unit": "blocks/s", "better": "higher", "effect": "relative"},
    "follow_lag_blocks": {"unit": "blocks", "better": "lower", "effect": "absolute"},
    "process_cores_busy": {"unit": "cores", "better": "lower", "effect": "relative"},
    "peak_rss_mb": {"unit": "MB", "better": "lower", "effect": "absolute"},
}

# ------------------------------------------------------------------------------------------------
# follow harness constants

FOLLOW_BIN = "follow.test"
REPEATS = 1
MIN_NONCE_CHECKS = 1000  # table answers compared with the application's own read, per 2,000-tx block size
MIN_FILL_BLOCKS = 10  # the fill before the first block hands over at least this many blocks
GAUGE_MS = "20"
FOLLOW_TIMEOUT_SECONDS = 400
RELAY_PORT = 29917  # the fullnode's connection to the validator passes through a relay on this loopback port
MAX_UNDELAYED_RTT_MS = 5.0  # loopback ping, which the delay must not reach
BASE = {"NODE1_ADMIT": "self", "NODE1_WORKLOAD": "erc20-transfer", "NODE1_NONCE": "table"}
# paced50, rtt100 and rtt250 share one workload and its constants (the same 1,200 blocks of exactly 100; equal on the
# shipped and the streaming builds at 50 and 100 blocks/s); their receipt digests are compared across the two sides
# rather than pinned. serve10's constants are the 120 x 2,000 ERC-20 workload's.
PACED_CONSTANTS = {"gas": 4140250920,
                   "final_apphash": "7e088b3f005ab67ba89eb297ede928c7e8c4455a234e7f24e9337b9758c4b258",
                   "apphash_digest": "26637a3720a4bba46207ffffd32000df5ce6f7f0f70ad3bf85c64d6f2ac87ecf",
                   "lthash": "264e4be28140274ac63927eebf648ffa3d4c7545c896abd8fd9c55edfee7fc23",
                   "receipt_digest": None, "filter_digest": None}
FOLLOW = {
    "paced50": {"env": dict(BASE, NODE1_TXS="100", NODE1_BLOCKS="1200", NODE1_MAX_TPS="5000", NODE1_FOLLOW_EXEC="1"),
                "blocks": 1200, "txs_per_block": 100, "pace": 50.0, "constants": PACED_CONSTANTS},
    "rtt100": {"env": dict(BASE, NODE1_TXS="100", NODE1_BLOCKS="1200", NODE1_MAX_TPS="10000", NODE1_FOLLOW_EXEC="1",
                           NODE1_FOLLOW_RELAY_PORT=str(RELAY_PORT)),
               "blocks": 1200, "txs_per_block": 100, "pace": 100.0, "rtt_ms": 100.0, "constants": PACED_CONSTANTS},
    "rtt250": {"env": dict(BASE, NODE1_TXS="100", NODE1_BLOCKS="1200", NODE1_MAX_TPS="10000", NODE1_FOLLOW_EXEC="1",
                           NODE1_FOLLOW_RELAY_PORT=str(RELAY_PORT)),
               "blocks": 1200, "txs_per_block": 100, "pace": 100.0, "rtt_ms": 250.0, "constants": PACED_CONSTANTS},
    "serve10": {"env": dict(BASE, NODE1_TXS="2000", NODE1_BLOCKS="120", NODE1_FOLLOWERS="10"),
                "blocks": 120, "txs_per_block": 2000,
                "constants": {"gas": 8280501840,
                              "final_apphash": "96847808f191b8228618b36e4fd6b303bebf2d10addcbe17efe6278063eb004a",
                              "apphash_digest": "ad8f106a16543690a84e9e94e8b4f50837cd16fd978e709995f00a09010956c9",
                              "lthash": "a87b924c9489d977464005863865da01f9e1666e390ce0b69192355df9a85634",
                              "receipt_digest": "12aa4bcaa3a0e6cf3f0bf1451eb72f48752d12dda81961815d7b495336cb4fee",
                              "filter_digest": "789e7a426eb8b1392b23c2d1b8588614791cb6706e5c80a4dfce6f6f8eedd9b3"}},
}
RTT_TOLERANCE = 0.10
PACE_TOLERANCE = 0.10
MIN_FOLLOW_CHECKED = 100  # heights the fullnode executed and matched against the validator's AppHash
N_STUBS = 10
FOLLOW_GATES = ("run_integrity", "admission_regime", "execution_bound", "state_gate", "apphash_gate", "receipt_gate", "follow_gate")
FOLLOW_FIGURES = {
    "follow_share": {"unit": "share of the chain's blocks", "better": "higher", "effect": "absolute"},
    "steady_txps": {"unit": "tx/s", "better": "higher", "effect": "relative"},
    "tx_per_cpu_s": {"unit": "tx/cpu-s", "better": "higher", "effect": "relative"},
    "chain_blocks_per_s": {"unit": "blocks/s", "better": "higher", "effect": "relative"},
    "follow_lag_blocks": {"unit": "blocks", "better": "lower", "effect": "absolute"},
    "stub_share": {"unit": "share", "better": "higher", "effect": "absolute"},
    "peak_rss_mb": {"unit": "MB", "better": "lower", "effect": "absolute"},
}

VARIANTS = tuple(COMMITTEE) + tuple(FOLLOW)


def variant_of(workload):
    """`fullsync` or `fullsync:<variant>` -> the variant, or None."""
    if workload == "fullsync":
        return "committee16"
    if workload and workload.startswith("fullsync:") and workload.split(":", 1)[1] in VARIANTS:
        return workload.split(":", 1)[1]
    return None


def netem_problem(variant):
    """Why an rtt variant cannot run here, or None."""
    if not FOLLOW.get(variant, {}).get("rtt_ms"):
        return None
    if not sys.platform.startswith("linux"):
        return f"{variant} delays a loopback port with Linux netem; this is {sys.platform}"
    if os.geteuid() != 0:
        return f"{variant} changes loopback queueing with tc and must run as root"
    if shutil.which("tc") is None:
        return f"{variant} needs tc (iproute2) on PATH"
    return None


def rss_mb(maxrss):
    """wait4's ru_maxrss in MB: bytes on macOS, KiB on Linux."""
    return maxrss / (1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0)


# ------------------------------------------------------------------------------------------------
# host idleness before a reading: recorded, not gated (Linux /proc only)

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
    return (len(fractions) >= COOLDOWN_CONSECUTIVE_WINDOWS and
            all(v is not None and v >= COOLDOWN_IDLE_FRACTION for v in fractions[-COOLDOWN_CONSECUTIVE_WINDOWS:]))


def wait_for_quiescence(min_seconds=COOLDOWN_MIN_SECONDS, max_seconds=COOLDOWN_MAX_SECONDS):
    if not os.path.exists("/proc/stat"):
        return {"idle_before_run": None, "note": "no /proc/stat; host idleness not read"}
    started = time.monotonic()
    fractions = []
    while time.monotonic() - started < max_seconds:
        before = proc_cpu_totals(read_text_if_present(Path("/proc/stat")))
        time.sleep(COOLDOWN_SAMPLE_SECONDS)
        after = proc_cpu_totals(read_text_if_present(Path("/proc/stat")))
        fractions.append(cpu_idle_fraction(before, after))
        if time.monotonic() - started >= min_seconds and quiescence_held(fractions):
            break
    return {"idle_before_run": quiescence_held(fractions), "elapsed_s": time.monotonic() - started,
            "idle_fractions": fractions, "loadavg": read_text_if_present(Path("/proc/loadavg")).strip()}


def key_values(text):
    out = {}
    for token in text.split():
        if "=" in token:
            key, value = token.split("=", 1)
            out[key] = value
    return out


# ------------------------------------------------------------------------------------------------
# committee: one run

def committee_parse(stdout):
    lines = {"start": {}, "span": {}, "agree": {}}
    for line in stdout.splitlines():
        if line.startswith("ML start_ms="):
            lines["start"] = key_values(line[3:])
        elif line.startswith("ML chain_blocks="):
            lines["span"] = key_values(line[3:])
        elif line.startswith("ML agree "):
            lines["agree"] = key_values(line[9:])
    return lines


def committee_run(binary, variant, plant=False):
    n = COMMITTEE[variant]
    store = Path(tempfile.mkdtemp(prefix=STORE_PREFIX))
    env = dict(os.environ)
    for key in ("GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GODEBUG", "ML_PLANT"):
        env.pop(key, None)
    env.update({"ML_RUN": "1", "ML_N": str(n), "ML_RATE": str(TX_PER_PRODUCER_PER_S * n), "ML_WARM_S": str(WARM_S),
                "ML_SECS": str(SPAN_S), "ML_BI_MS": "400", "ML_VT_MS": "1500", "ML_DIR": str(store / "nodes")})
    if plant:
        env["ML_PLANT"] = "1"
    started = time.monotonic()
    out_path = Path(tempfile.gettempdir()) / (STORE_PREFIX + "out-" + store.name + ".txt")
    try:
        with open(out_path, "w") as out:
            proc = subprocess.Popen(
                [binary, "-test.run", "^TestCommitteeFollow$", "-test.count=1", f"-test.timeout={COMMITTEE_TIMEOUT_SECONDS}s"],
                env=env, stdout=out, stderr=subprocess.STDOUT)
            killer = threading.Timer(COMMITTEE_TIMEOUT_SECONDS + 30, proc.kill)
            killer.start()
            try:
                _, status, rusage = os.wait4(proc.pid, 0)
            finally:
                killer.cancel()
        rc = os.waitstatus_to_exitcode(status)
        wall = time.monotonic() - started
        stdout = out_path.read_text(errors="replace")
    finally:
        shutil.rmtree(store, ignore_errors=True)
        if out_path.exists():
            out_path.unlink()
    return {"variant": variant, "n": n, "rc": rc, "stdout": stdout, "wall_s": wall,
            "cpu_s": rusage.ru_utime + rusage.ru_stime, "rusage_maxrss": rusage.ru_maxrss}


def committee_observe(run):
    parsed = committee_parse(run["stdout"])
    span, agree, start = parsed["span"], parsed["agree"], parsed["start"]
    n = run["n"]
    obs = {"variant": run["variant"], "n": n, "rc": run["rc"], "wall_s": run["wall_s"], "cpu_s": run["cpu_s"],
           "peak_rss_mb": rss_mb(run["rusage_maxrss"])}

    def num(d, key, cast=float):
        try:
            return cast(d[key])
        except (KeyError, ValueError):
            return None

    for key, cast in (("chain_blocks", int), ("fullnode_blocks", int), ("chain_blocks_per_s", float),
                      ("chain_tx_per_s", float), ("txs_per_block", float), ("fullnode_blocks_per_s", float),
                      ("lag_start", int), ("lag_end", int), ("v0_height", int), ("fullnode_height", int),
                      ("inserted", int), ("span_s", float)):
        obs[key] = num(span, key, cast)
    obs["start_ms"] = num(start, "start_ms", int)
    obs["insert_errors"] = num(agree, "insert_errors", int)
    obs["offered"] = num(agree, "offered", int)
    for key in ("fullnode_apphash_equal", "fullnode_txdigest_equal", "fullnode_blocks_contiguous", "validator_apphash_equal"):
        obs[key] = agree.get(key) == "true"
    obs["compared_height"] = num(agree, "compared_height", int)

    parsed_ok = bool(span) and bool(agree) and obs["chain_blocks"] is not None and obs["fullnode_blocks"] is not None
    expected_offered = TX_PER_PRODUCER_PER_S * n * (WARM_S + SPAN_S)
    obs["run_integrity"] = bool(
        run["rc"] == 0 and parsed_ok and obs["span_s"] == SPAN_S and obs["insert_errors"] == 0 and
        obs["offered"] == expected_offered and obs["inserted"] is not None and
        obs["inserted"] >= OFFERED_FLOOR_SHARE * expected_offered)
    floor = CHAIN_FLOOR_SHARE * SEALS_PER_PRODUCER_PER_S * n
    obs["chain_floor_blocks_per_s"] = floor
    obs["chain_regime"] = bool(parsed_ok and obs["chain_blocks_per_s"] is not None and obs["chain_blocks_per_s"] >= floor)
    obs["agreement_gate"] = bool(parsed_ok and obs["fullnode_apphash_equal"] and obs["fullnode_txdigest_equal"] and
                                 obs["fullnode_blocks_contiguous"] and obs["validator_apphash_equal"])
    obs["follow_engaged"] = bool(parsed_ok and obs["fullnode_blocks"] and obs["fullnode_blocks"] > 0 and
                                 obs["compared_height"] and obs["compared_height"] > 1)
    obs["cpu_cores_busy"] = run["cpu_s"] / run["wall_s"] if run["wall_s"] else None
    return obs


def committee_figures(obs):
    values, witness = {}, {}
    cb, fb = obs.get("chain_blocks"), obs.get("fullnode_blocks")
    values["follow_share"] = fb / cb if cb and fb is not None else None
    cps, fps = obs.get("chain_blocks_per_s"), obs.get("fullnode_blocks_per_s")
    if cps and fps is not None:
        witness["follow_share"] = fps / cps
    values["chain_blocks_per_s"] = cps
    witness["chain_blocks_per_s"] = cb / SPAN_S if cb else None
    values["fullnode_blocks_per_s"] = fps
    witness["fullnode_blocks_per_s"] = fb / SPAN_S if fb is not None else None
    values["follow_lag_blocks"] = obs.get("lag_end")
    witness["follow_lag_blocks"] = obs.get("lag_end")
    values["process_cores_busy"] = obs.get("cpu_cores_busy")
    witness["process_cores_busy"] = obs.get("cpu_cores_busy")
    values["peak_rss_mb"] = obs.get("peak_rss_mb")
    witness["peak_rss_mb"] = obs.get("peak_rss_mb")
    gates = {g: bool(obs.get(g)) for g in COMMITTEE_GATES}
    return values, gates, {k: v for k, v in witness.items() if v is not None}


def committee_plant_check(binary):
    """Runs committee8 once with the fullnode's record planted; passes only if the agreement gate refuses it."""
    obs = committee_observe(committee_run(binary, "committee8", plant=True))
    refused = obs["run_integrity"] and not obs["agreement_gate"]
    print("PLANT_CHECK " + json.dumps({"agreement_gate": obs["agreement_gate"], "run_integrity": obs["run_integrity"],
                                       "fullnode_txdigest_equal": obs["fullnode_txdigest_equal"], "passed": refused}))
    return 0 if refused else 1


# ------------------------------------------------------------------------------------------------
# follow: parsing the harness's NODE1 lines

def expected(variant):
    m = FOLLOW[variant]
    return m["blocks"], m["txs_per_block"], m["blocks"] * m["txs_per_block"], m["blocks"] + 1


def parse_node1(stdout):
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
        elif kind in ("cfg", "admission", "window", "steady", "feed", "feeddiag", "success", "state", "proc", "paths", "follow", "relay"):
            lines[kind] = key_values(rest)
    return {"lines": lines, "wphase": wphase, "gauge": gauge, "count": count}


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
# follow: readers built from the base ref, run after the process exits

def run_state_gate(seidb, store):
    proc = subprocess.run([seidb, "dump-flatkv", "-d", str(store / "node0/data/state_commit/flatkv"), "--lthash-only"],
                          text=True, capture_output=True, check=False, timeout=300)
    total, verify_pass = "NONE", False
    for line in proc.stdout.splitlines() + proc.stderr.splitlines():
        if "result: PASS" in line:
            verify_pass = True
        m = re.search(r"TOTAL\s+lthash=([0-9a-f]+)", line)
        if m:
            total = m.group(1)
    return {"rc": proc.returncode, "total": total, "verify_pass": verify_pass}


def run_receipt_reader(reader, store, latest):
    proc = subprocess.run([reader, "--receipt-dir", str(store / "node0/data/ledger/receipt/littidx"), "--expect-latest", str(latest)],
                          text=True, capture_output=True, check=False, timeout=300)
    values = {}
    for line in proc.stdout.splitlines():
        if line.startswith("latest="):
            values = key_values(line)
    return {"rc": proc.returncode, "values": values}


# ------------------------------------------------------------------------------------------------
# follow: the relay round trip, and the netem delay on its port

def loopback_rtt_ms():
    """The median of five loopback ping round trips, in ms, or None."""
    try:
        out = subprocess.run(["ping", "-c", "5", "-i", "0.2", "-n", "127.0.0.1"], capture_output=True, text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    times = sorted(float(m) for m in re.findall(r"time=([0-9.]+) ms", out))
    return times[len(times) // 2] if times else None


def relay_rtt_ms():
    """The median of five TCP connect times to RELAY_PORT on loopback, in ms, or None. A connect returns once the
    SYN-ACK arrives, so it reads one round trip of the delayed path. The listener is released before this returns."""
    try:
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", RELAY_PORT))
        srv.listen(16)
    except OSError:
        return None
    # accepts are polled: on Linux a close() does not wake a thread blocked in accept()
    srv.settimeout(0.1)
    stop = threading.Event()

    def accept():
        while not stop.is_set():
            try:
                c, _ = srv.accept()
                c.close()
            except socket.timeout:
                continue
            except OSError:
                return
    acceptor = threading.Thread(target=accept, daemon=True)
    acceptor.start()
    times = []
    try:
        for _ in range(5):
            t = time.monotonic()
            c = socket.create_connection(("127.0.0.1", RELAY_PORT), timeout=10)
            times.append((time.monotonic() - t) * 1000)
            c.close()
            time.sleep(0.05)
    except OSError:
        return None
    finally:
        stop.set()
        acceptor.join(timeout=5)
        srv.close()
    return statistics.median(times)


def relay_port_free():
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(("127.0.0.1", RELAY_PORT))
        return True
    except OSError:
        return False
    finally:
        s.close()


def set_loopback_delay(rtt_ms):
    """Delays loopback packets to or from RELAY_PORT by half of rtt_ms each (0 removes the delay); returns whether tc
    accepted every command."""
    try:
        subprocess.run(["tc", "qdisc", "del", "dev", "lo", "root"], capture_output=True)
        if not rtt_ms:
            return True
        cmds = [
            ["tc", "qdisc", "add", "dev", "lo", "root", "handle", "1:", "prio", "bands", "3", "priomap"] + ["0"] * 16,
            ["tc", "qdisc", "add", "dev", "lo", "parent", "1:3", "handle", "30:", "netem", "delay", f"{rtt_ms / 2:g}ms", "limit", "100000"],
            ["tc", "filter", "add", "dev", "lo", "parent", "1:0", "protocol", "ip", "prio", "1", "u32", "match", "ip", "dport", str(RELAY_PORT), "0xffff", "flowid", "1:3"],
            ["tc", "filter", "add", "dev", "lo", "parent", "1:0", "protocol", "ip", "prio", "1", "u32", "match", "ip", "sport", str(RELAY_PORT), "0xffff", "flowid", "1:3"],
        ]
        return all(subprocess.run(c, capture_output=True).returncode == 0 for c in cmds)
    except OSError:
        return not rtt_ms


# ------------------------------------------------------------------------------------------------
# follow: one process and its checks

def follow_process(binary, variant):
    rtt_ms = FOLLOW[variant].get("rtt_ms")
    rtt_read = undelayed = None
    if rtt_ms:
        if not set_loopback_delay(rtt_ms):
            set_loopback_delay(0)
            return {"variant": variant, "store": None, "rc": 97, "stdout": "", "wall_s": 0.0, "rusage_maxrss": 0,
                    "rtt_ms": None, "undelayed_rtt_ms": None}
        rtt_read = relay_rtt_ms()
        undelayed = loopback_rtt_ms()
    port_free = relay_port_free()
    try:
        run = follow_node(binary, variant)
    finally:
        if rtt_ms:
            set_loopback_delay(0)
    run["rtt_ms"] = rtt_read
    run["undelayed_rtt_ms"] = undelayed
    run["relay_port_free"] = port_free
    return run


def follow_node(binary, variant):
    store = Path(tempfile.mkdtemp(prefix=STORE_PREFIX))
    env = dict(os.environ)
    env.update(FOLLOW[variant]["env"])
    env.update({"NODE1_DIR": str(store), "NODE1_GAUGE_MS": GAUGE_MS})
    for key in ("GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GODEBUG"):
        env.pop(key, None)
    started = time.monotonic()
    out_path = store / "node-stdout.txt"
    with open(out_path, "w") as out:
        proc = subprocess.Popen([binary, "-test.run", "^TestNode1$", "-test.count=1", f"-test.timeout={FOLLOW_TIMEOUT_SECONDS}s"],
                                env=env, stdout=out, stderr=subprocess.STDOUT)
        killer = threading.Timer(FOLLOW_TIMEOUT_SECONDS + 60, proc.kill)
        killer.start()
        try:
            _, status, rusage = os.wait4(proc.pid, 0)
        finally:
            killer.cancel()
    rc = os.waitstatus_to_exitcode(status)
    wall = time.monotonic() - started
    stdout = out_path.read_text(errors="replace")
    return {"variant": variant, "store": store, "rc": rc, "stdout": stdout, "wall_s": wall, "rusage_maxrss": rusage.ru_maxrss}


def follow_validate(run, tools):
    parsed = parse_node1(run["stdout"])
    seidb, reader = os.path.join(tools, common.SEIDB), os.path.join(tools, common.RECEIPT_READER)
    state = run_state_gate(seidb, run["store"]) if run["rc"] == 0 else {"rc": None, "total": "NONE", "verify_pass": False}
    receipts = run_receipt_reader(reader, run["store"], expected(run["variant"])[3]) if run["rc"] == 0 else {"rc": None, "values": {}}
    return {"parsed": parsed, "state": state, "receipts": receipts}


def follow_observe(run, checked):
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
    loop = main_loop(parsed)
    loop_total = sum(v[0] for v in loop.values()) if loop else None
    obs.update({
        "steady_txs": steady_txs, "steady_wall_s": steady_wall, "steady_blocks": steady_blocks, "steady_ms_per_block": steady_ms,
        "window_txs": window_txs, "cpu_window_s": cpu_window, "cpu_us_per_tx": num("proc", "cpu_us_per_tx"),
        "loop_execution_s": loop.get("execution", (None, None))[0], "loop_total_s": loop_total,
        "window_blocks": num("window", "blocks", int),
        "rusage_maxrss_mb": None if run["rusage_maxrss"] is None else rss_mb(run["rusage_maxrss"]),
        "cores_busy": num("proc", "cores_busy"),
        "receive_execute_depth": gauge_mean(parsed, "depth receive-execute"),
        "feed": L.get("feed", {}), "feeddiag": L.get("feeddiag", {}), "admission": L.get("admission", {}),
        "state_reader": checked["state"], "receipt_reader": checked["receipts"],
        "final_apphash": L.get("state", {}).get("final_apphash"), "apphash_digest": L.get("state", {}).get("apphash_digest"),
        "tree_lthash": L.get("state", {}).get("flatkv_lthash"), "gomaxprocs": L.get("cfg", {}).get("gomaxprocs"),
    })
    blocks_n, tpb, txs_n, latest_n = expected(variant)
    C = FOLLOW[variant]["constants"]
    fl = L.get("follow", {})

    def fnum(key, cast=float):
        try:
            return cast(fl[key])
        except (KeyError, ValueError):
            return None

    obs.update({
        "follow_exec_blocks": fnum("exec_blocks", int), "follow_val_blocks": fnum("val_blocks", int),
        "follow_val_wall_s": fnum("val_wall_s"), "follow_exec_blocks_per_s": fnum("exec_blocks_per_s"),
        "chain_blocks_per_s": fnum("chain_blocks_per_s"), "follow_lag_blocks": fnum("exec_lag_blocks", int),
        "follow_checked": fnum("checked", int), "follow_mismatched": fnum("mismatched", int), "follow_unmatched": fnum("unmatched", int),
        "stub_mean_share": fnum("stub_mean_share"), "stub_min_height_at_end": fnum("stub_min_height_at_end", int),
    })
    s, st = L.get("success", {}), L.get("state", {})
    obs["run_integrity"] = bool(
        run["rc"] == 0 and L.get("cfg", {}).get("workload") == "erc20-transfer" and s.get("ok") == str(txs_n) and s.get("failed") == "0" and
        s.get("receipts_ok") == str(txs_n) and s.get("gas_used") == str(C["gas"]) and
        st.get("blocks") == str(blocks_n) and st.get("block_txs_min") == str(tpb) and
        st.get("block_txs_max") == str(tpb) and steady_txs and steady_wall and window_txs and cpu_window and loop_total)
    # the producer's nonce reads were answered from the harness table, which never read above the application's own
    # answer on the calls checked against it
    nd = L.get("feeddiag", {})
    try:
        nonce_ok = (nd.get("nonce_table") == "true" and nd.get("nonce_ahead") == "0" and
                    int(nd.get("nonce_checked", "0")) >= MIN_NONCE_CHECKS * tpb // 2000)
    except ValueError:
        nonce_ok = False
    obs["run_integrity"] = bool(obs["run_integrity"] and nonce_ok)
    a = L.get("admission", {})
    obs["admission_regime"] = (a.get("inner_checktx") == str(txs_n + 1) and a.get("precheck_in_window") == "0"
                               and a.get("admission_mismatch") == "0" and a.get("key_mismatch") == "0"
                               and a.get("key_checks") in ("0", str(txs_n + 1)))
    fd = L.get("feed", {})
    try:
        feed_ok = (fd.get("gauge_ms") == GAUGE_MS and fd.get("steady_starved_blocks") == "0" and
                   int(fd.get("fill_txs", "0")) >= MIN_FILL_BLOCKS * tpb)
    except ValueError:
        feed_ok = False
    cb = obs["chain_blocks_per_s"]
    pace, rtt_ms = FOLLOW[variant].get("pace"), FOLLOW[variant].get("rtt_ms")
    obs["relay_rtt_ms"] = run.get("rtt_ms")
    obs["relay_port_free"] = run.get("relay_port_free")
    obs["undelayed_rtt_ms"] = run.get("undelayed_rtt_ms")
    rl = L.get("relay", {})
    if pace:
        # the producer's pace sets the chain's block rate; with a round trip, the relayed connect read it, loopback
        # ping did not, and the fullnode's blocks passed through the relay
        try:
            relayed = int(rl.get("conns", "0")) >= 1 and int(rl.get("bytes_to_followers", "0")) > 0
        except ValueError:
            relayed = False
        rtt_ok = not rtt_ms or (run.get("rtt_ms") is not None and abs(run["rtt_ms"] - rtt_ms) <= RTT_TOLERANCE * rtt_ms
                                and run.get("undelayed_rtt_ms") is not None and run["undelayed_rtt_ms"] < MAX_UNDELAYED_RTT_MS
                                and relayed)
        obs["execution_bound"] = bool(feed_ok and rtt_ok and cb is not None and abs(cb - pace) <= PACE_TOLERANCE * pace)
        obs["follow_gate"] = bool(fl.get("exec") == "true" and (obs["follow_checked"] or 0) >= MIN_FOLLOW_CHECKED and
                                  obs["follow_mismatched"] == 0 and obs["follow_unmatched"] == 0)
    else:
        # the node, not the harness inserter, sets the rate; the receive-execute depth gauge is shared with the stub
        # followers' data layers here, so it is printed and not gated
        obs["execution_bound"] = bool(feed_ok)
        obs["follow_gate"] = bool(fl.get("stubs") == str(N_STUBS) and (obs["stub_min_height_at_end"] or 0) >= 2)
    sr = checked["state"]
    obs["state_gate"] = bool(sr["rc"] == 0 and sr["verify_pass"] and sr["total"] == C["lthash"] and st.get("flatkv_lthash") == C["lthash"])
    obs["apphash_gate"] = st.get("final_apphash") == C["final_apphash"] and st.get("apphash_digest") == C["apphash_digest"]
    rv = checked["receipts"]["values"]
    obs["receipt_digests"] = {"receipt_digest": rv.get("receipt_digest"), "filter_digest": rv.get("filter_digest")}
    obs["receipt_gate"] = bool(
        checked["receipts"]["rc"] == 0 and rv.get("latest") == str(latest_n) and rv.get("receipts") == str(txs_n) and
        rv.get("success") == str(txs_n) and rv.get("failed") == "0" and
        (C["receipt_digest"] is None or (rv.get("receipt_digest") == C["receipt_digest"] and rv.get("filter_digest") == C["filter_digest"])))
    return obs


def follow_aggregate(observations):
    def total(key):
        vals = [o.get(key) for o in observations]
        return None if any(v is None for v in vals) else sum(vals)

    values, witness = {}, {}
    st, sw = total("steady_txs"), total("steady_wall_s")
    values["steady_txps"] = st / sw if st and sw else None
    wb = [(o.get("steady_blocks"), o.get("steady_ms_per_block"), expected(o["variant"])[1]) for o in observations]
    if all(b and m for b, m, _ in wb):
        witness["steady_txps"] = sum(b * t for b, _, t in wb) / sum(b * m / 1000.0 for b, m, _ in wb)
    wt, cw = total("window_txs"), total("cpu_window_s")
    values["tx_per_cpu_s"] = wt / cw if wt and cw else None
    cu = [(o.get("window_txs"), o.get("cpu_us_per_tx")) for o in observations]
    if all(t and c for t, c in cu):
        witness["tx_per_cpu_s"] = sum(t for t, _ in cu) / sum(t * c / 1e6 for t, c in cu)
    # the node process's lifetime high-water mark, read from wait4
    rr = [o.get("rusage_maxrss_mb") for o in observations]
    values["peak_rss_mb"] = max(rr) if all(r is not None for r in rr) else None
    witness["peak_rss_mb"] = max(r * 1024.0 for r in rr) / 1024.0 if values["peak_rss_mb"] is not None else None
    fe, fv = total("follow_exec_blocks"), total("follow_val_blocks")
    values["follow_share"] = fe / fv if fe is not None and fv else None
    fr = [(o.get("follow_exec_blocks_per_s"), o.get("chain_blocks_per_s"), o.get("follow_val_wall_s")) for o in observations]
    if all(e is not None and c and w for e, c, w in fr):
        witness["follow_share"] = sum(e * w for e, _, w in fr) / sum(c * w for _, c, w in fr)
    cws = [(o.get("follow_val_blocks"), o.get("follow_val_wall_s")) for o in observations]
    values["chain_blocks_per_s"] = sum(b for b, _ in cws) / sum(w for _, w in cws) if all(b and w for b, w in cws) else None
    witness["chain_blocks_per_s"] = values["chain_blocks_per_s"]
    lg = [o.get("follow_lag_blocks") for o in observations]
    values["follow_lag_blocks"] = max(lg) if all(v is not None for v in lg) else None
    witness["follow_lag_blocks"] = values["follow_lag_blocks"]
    ss = [o.get("stub_mean_share") for o in observations]
    values["stub_share"] = sum(ss) / len(ss) if ss and all(v is not None for v in ss) else None
    witness["stub_share"] = values["stub_share"]
    gates = {g: all(bool(o.get(g)) for o in observations) for g in FOLLOW_GATES}
    return values, gates, {k: v for k, v in witness.items() if v is not None}


# ------------------------------------------------------------------------------------------------
# modes

def tools(base_tree, out):
    common.build_tools(base_tree, out)
    return 0


def setup(tree, out, variant):
    os.makedirs(out, exist_ok=True)
    pkg = os.path.join(tree, PKG)
    if variant in COMMITTEE:
        common.place_harness(os.path.join(HARNESS_DIR, "bench_committee_test.go"), tree, PKG, "bench_committee_test.go")
        # built as the harness was for the published figures: without the tree's GOEXPERIMENT (the committee runs
        # the p2p package's testApp, not the EVM executor)
        env = {k: v for k, v in os.environ.items() if k != "GOEXPERIMENT"}
        binary = os.path.join(out, COMMITTEE_BIN)
        subprocess.run(["go", "test", "-c", "-tags", "benchharness", "-o", binary, "./" + PKG + "/"], cwd=tree, env=env, check=True)
        print(f"built {binary}")
        print("the agreement gate must refuse a planted fullnode record:")
        return committee_plant_check(binary)
    common.place_harness(os.path.join(HARNESS_DIR, "bench_follow_test.go"), tree, PKG, "bench_follow_test.go")
    env = common.go_env(tree)
    binary = os.path.join(out, FOLLOW_BIN)
    subprocess.run(["go", "test", "-c", "-tags", "benchharness", "-o", binary, "./" + PKG + "/"], cwd=tree, env=env, check=True)
    print(f"built {binary} ({pkg})")
    return 0


def measure(tree, out, tools_dir, workload, variant):
    result = {"workload": workload, "variant": variant}
    problem = netem_problem(variant)
    host = wait_for_quiescence() if problem is None else {}
    result["host"] = dict(host, loadavg_now=list(os.getloadavg()))
    if variant in COMMITTEE:
        result.update({"figure_spec": COMMITTEE_FIGURES, "primary": "follow_share"})
        binary = os.path.join(out, COMMITTEE_BIN)
        if not os.access(binary, os.X_OK):
            result.update({"figures": {k: None for k in COMMITTEE_FIGURES}, "gates": {g: False for g in COMMITTEE_GATES},
                           "problems": [f"no test binary at {binary}: setup did not build it"]})
            print("RESULT " + json.dumps(result, sort_keys=True))
            return 0
        run = committee_run(binary, variant)
        obs = committee_observe(run)
        tail = [line for line in run["stdout"].splitlines() if line.startswith("ML ") or "FAIL" in line or "panic" in line]
        print("\n".join(tail[:20]))
        figures, gates, witness = committee_figures(obs)
        disputes = common.apply_witness(figures, witness, WITNESS_TOLERANCE, [k for k, v in figures.items() if v is not None])
        result.update({"figures": figures, "gates": gates, "witness": witness, "witness_disputes": disputes, "observation": obs})
        print("RESULT " + json.dumps(result, sort_keys=True, default=str))
        return 0

    primary = "steady_txps" if variant == "serve10" else "follow_share"
    result.update({"figure_spec": FOLLOW_FIGURES, "primary": primary})
    binary = os.path.join(out, FOLLOW_BIN)
    problems = []
    if problem:
        problems.append(problem)
    if not os.access(binary, os.X_OK):
        problems.append(f"no test binary at {binary}: setup did not build it")
    for t in (common.SEIDB, common.RECEIPT_READER):
        if not os.access(os.path.join(tools_dir, t), os.X_OK):
            problems.append(f"no {t} in {tools_dir}: the tools step did not build it")
    if problems:
        result.update({"figures": {k: None for k in FOLLOW_FIGURES}, "gates": {g: False for g in FOLLOW_GATES}, "problems": problems})
        print("RESULT " + json.dumps(result, sort_keys=True))
        return 0
    observations = []
    for repeat in range(REPEATS):
        run = follow_process(binary, variant)
        try:
            checked = follow_validate(run, tools_dir) if run["store"] is not None else \
                {"parsed": parse_node1(""), "state": {"rc": None, "total": "NONE", "verify_pass": False}, "receipts": {"rc": None, "values": {}}}
            obs = follow_observe(run, checked)
        finally:
            if run["store"] is not None:
                shutil.rmtree(run["store"], ignore_errors=True)
        obs["repeat"] = repeat
        observations.append(obs)
        for line in run["stdout"].splitlines():
            if line.startswith("NODE1 ") or "FAIL" in line or "panic" in line:
                print(line[:400])
        if run["rc"] != 0:
            print(f"node exited rc={run['rc']} relay_port_free={run.get('relay_port_free')}; last lines:")
            print("\n".join(line[:300] for line in run["stdout"].splitlines()[-25:]))
    figures, gates, witness = follow_aggregate(observations)
    disputes = common.apply_witness(figures, witness, WITNESS_TOLERANCE, [k for k, v in figures.items() if v is not None])
    # the validator's chain and receipts must be the same on both sides
    o = observations[0]
    digests = {"final_apphash": o.get("final_apphash"), "apphash_digest": o.get("apphash_digest"),
               "lthash": o.get("tree_lthash"), **o.get("receipt_digests", {})}
    result.update({"figures": figures, "gates": gates, "witness": witness, "witness_disputes": disputes, "digests": digests,
                   "observations": [{k: v for k, v in x.items() if k != "store"} for x in observations]})
    print("RESULT " + json.dumps(result, sort_keys=True, default=str))
    return 0


def teardown():
    if sys.platform.startswith("linux") and os.geteuid() == 0 and shutil.which("tc"):
        set_loopback_delay(0)
    removed = 0
    for path in sorted(Path(tempfile.gettempdir()).glob(STORE_PREFIX + "*")):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink()
        removed += 1
    print(f"teardown removed={removed}")
    return 0


# ------------------------------------------------------------------------------------------------
# self-test: parsers and gates on synthetic output, each gate failing on a planted change

def self_test():
    assert variant_of("fullsync") == "committee16" and variant_of("fullsync:rtt100") == "rtt100"
    assert variant_of("fullsync:x") is None and variant_of("eth8") is None
    assert proc_cpu_totals("cpu  10 0 5 80 5 0 0 0 0 0\n") == (100, 85)
    assert cpu_idle_fraction((100, 85), (200, 180)) == 0.95
    assert quiescence_held([0.95, 0.96]) and not quiescence_held([0.96, 0.94])

    # committee
    n = 8
    good = ("ML start_ms=1200 n=8 producers=8 rate=40 bi_ms=400 vt_ms=1500 txb=150 fullnode_height=8\n"
            "ML chain_blocks=590 fullnode_blocks=300 chain_blocks_per_s=19.6667 chain_tx_per_s=40.0 txs_per_block=2.0 "
            "fullnode_blocks_per_s=10.0000 follow_share=0.5085 lag_start=30 lag_end=320 v0_height=700 fullnode_height=380 "
            "inserted=1408 span_s=30\n"
            "ML agree fullnode_height=380 compared_height=380 fullnode_apphash_equal=true fullnode_txdigest_equal=true "
            "fullnode_blocks_contiguous=true validator_last_height=699 validator_apphash_equal=true insert_errors=0 offered=1400\n")

    def crun(stdout=good, rc=0):
        return {"variant": "committee8", "n": n, "rc": rc, "stdout": stdout, "wall_s": 50.0, "cpu_s": 40.0,
                "rusage_maxrss": 200 * 1024 * (1024 if sys.platform == "darwin" else 1)}

    figures, gates, witness = committee_figures(committee_observe(crun()))
    assert all(gates.values()), gates
    assert abs(figures["follow_share"] - 300 / 590) < 1e-9 and abs(witness["follow_share"] - figures["follow_share"]) < 1e-3
    assert abs(figures["peak_rss_mb"] - 200.0) < 1e-9
    assert common.apply_witness(dict(figures), witness, WITNESS_TOLERANCE, list(figures)) == []
    for bad, gate in ((good.replace("fullnode_txdigest_equal=true", "fullnode_txdigest_equal=false"), "agreement_gate"),
                      (good.replace("validator_apphash_equal=true", "validator_apphash_equal=false"), "agreement_gate"),
                      (good.replace("insert_errors=0", "insert_errors=3"), "run_integrity"),
                      (good.replace("inserted=1408", "inserted=1000"), "run_integrity"),
                      (good.replace("chain_blocks_per_s=19.6667", "chain_blocks_per_s=12.0"), "chain_regime"),
                      (good.replace("fullnode_blocks=300 ", "fullnode_blocks=0 "), "follow_engaged"),
                      (good.replace("ML agree", "XX agree"), "agreement_gate")):
        _, gates, _ = committee_figures(committee_observe(crun(bad)))
        assert not gates[gate], gate
    _, gates, _ = committee_figures(committee_observe(crun(rc=1)))
    assert not gates["run_integrity"]

    # follow
    def fixture(variant, gas=None, apphash=None, inner=None, ok=None, starved=0, fill=None, nonce_ahead=0,
                chain=None, exec_blocks=None, checked=500, mismatched=0, unmatched=0, stubs=None, stub_min=None, exec_flag=None,
                relay_conns=1):
        blocks_n, tpb, txs_n, latest_n = expected(variant)
        C = FOLLOW[variant]["constants"]
        gas = C["gas"] if gas is None else gas
        apphash = C["final_apphash"] if apphash is None else apphash
        inner = txs_n + 1 if inner is None else inner
        ok = txs_n if ok is None else ok
        fill = 30 * tpb if fill is None else fill
        follow = bool(FOLLOW[variant].get("pace"))
        chain = (FOLLOW[variant]["pace"] * 1.01 if follow else 30.0) if chain is None else chain
        val_blocks = blocks_n - 1 - blocks_n // 10
        wall = val_blocks / chain
        exec_blocks = (int(0.97 * val_blocks) if follow else 0) if exec_blocks is None else exec_blocks
        stubs = (0 if follow else N_STUBS) if stubs is None else stubs
        stub_min = (-1 if follow else blocks_n) if stub_min is None else stub_min
        exec_flag = ("true" if follow else "false") if exec_flag is None else exec_flag
        return "\n".join([
            "noise",
            f"NODE1 cfg workload=erc20-transfer admit=self txs={tpb} blocks={blocks_n} gomaxprocs=16",
            f"NODE1 admission presign_s=1 precheck_in_window=0 admission_mismatch=0 inner_checktx={inner} insert_s=3 key_field=true key_checks={inner} key_mismatch=0 admitter_checktx=0 carried_bytes=0",
            f"NODE1 window blocks={blocks_n - 1} txs={(blocks_n - 1) * tpb} wall_s=2.000000 blocks_per_s=59.5 tx_per_s=119000 ms_per_block=16.807",
            f"NODE1 steady from_height=14 blocks={val_blocks} txs={val_blocks * tpb} wall_s={wall:.6f} blocks_per_s={chain} tx_per_s={chain * tpb} ms_per_block={1000 * wall / val_blocks:.6f}",
            f"NODE1 feed fill_s=0.400 fill_txs={fill} gauge_ms=20 steady_blocks={val_blocks} steady_fed_blocks=90 steady_after_insert_blocks=17 steady_starved_blocks={starved} steady_min_ahead_blocks=3.10 steady_mean_ahead_blocks=12.00",
            f"NODE1 feeddiag win_insert_calls=180000 nonce_table=true nonce_checked=3750 nonce_behind=0 nonce_ahead={nonce_ahead}",
            f"NODE1 follow exec={exec_flag} stubs={stubs} val_blocks={val_blocks} val_wall_s={wall:.6f} chain_blocks_per_s={chain:.2f} exec_blocks={exec_blocks} exec_share={exec_blocks / val_blocks:.6f} exec_blocks_per_s={exec_blocks / wall:.2f} exec_height_at_end=1159 val_height_at_end={latest_n} exec_lag_blocks=42 checked={checked} mismatched={mismatched} unmatched={unmatched} first_mismatch=0 stub_min_height_at_end={stub_min} stub_mean_share=0.99",
            f"NODE1 success ok={ok} failed=0 expected={txs_n} receipts_ok={ok} receipts_failed=0 receipts_missing=0 gas_used={gas} first_failure=\"\"",
            f"NODE1 state final_apphash={apphash} apphash_digest={C['apphash_digest']} blocks={blocks_n} first_height=2 last_height={latest_n} block_txs_min={tpb} block_txs_max={tpb} flatkv_version={latest_n} flatkv_lthash={C['lthash']}",
            "NODE1 proc cpu_window_s=23.800000 cpu_us_per_tx=100.0000 cores_busy=11.9 peak_rss_mb=900 gc_window=60 gc_total=90",
            "NODE1 wphase autobahn_main_loop_phase_duration_seconds_total phase=execution total_s=1.500000 ms_per_block=12.605042",
            "NODE1 wphase autobahn_main_loop_phase_duration_seconds_total phase=storage total_s=0.500000 ms_per_block=4.201681",
        ] + ([f"NODE1 relay port={RELAY_PORT} conns={relay_conns} bytes_to_followers={5000000 if relay_conns else 0}"]
             if FOLLOW[variant].get("rtt_ms") else [])) + "\n"

    def good_receipts(variant, **over):
        _, _, txs_n, latest_n = expected(variant)
        C = FOLLOW[variant]["constants"]
        v = {"latest": str(latest_n), "receipts": str(txs_n), "success": str(txs_n), "failed": "0",
             "receipt_digest": C["receipt_digest"] or "ab" * 32, "filter_digest": C["filter_digest"] or "cd" * 32}
        v.update(over)
        return {"rc": 0, "values": v}

    def fobs(variant, state=None, receipts=None, rc=0, **kw):
        state = {"rc": 0, "total": FOLLOW[variant]["constants"]["lthash"], "verify_pass": True} if state is None else state
        receipts = good_receipts(variant) if receipts is None else receipts
        rtt = kw.pop("rtt", FOLLOW[variant].get("rtt_ms"))
        undelayed = kw.pop("undelayed", 0.05)
        run = {"variant": variant, "rc": rc, "wall_s": 10.0, "rusage_maxrss": 900 * 1024, "stdout": fixture(variant, **kw),
               "store": Path("x"), "rtt_ms": rtt, "undelayed_rtt_ms": undelayed}
        return follow_observe(run, {"parsed": parse_node1(run["stdout"]), "state": state, "receipts": receipts})

    for variant in FOLLOW:
        figures, gates, witness = follow_aggregate([fobs(variant)])
        assert all(gates.values()), (variant, gates)
        assert figures["steady_txps"] and abs(figures["steady_txps"] - witness["steady_txps"]) < 1e-6 * figures["steady_txps"]
        # the fixture's printed CPU per transaction is not derived from its window, so tx_per_cpu_s is left out here
        assert common.apply_witness(dict(figures), witness, WITNESS_TOLERANCE, [k for k, v in figures.items() if v is not None and k != "tx_per_cpu_s"]) == [], variant
    figures, _, witness = follow_aggregate([fobs("paced50")])
    assert abs(figures["follow_share"] - 0.97) < 0.01 and abs(witness["follow_share"] - figures["follow_share"]) < 1e-3
    assert abs(figures["chain_blocks_per_s"] - 50.5) < 0.01
    for kw in (dict(mismatched=1), dict(unmatched=1), dict(checked=99), dict(exec_flag="false")):
        assert not follow_aggregate([fobs("paced50", **kw)])[1]["follow_gate"], kw
    for kw in (dict(chain=40.0), dict(chain=60.0), dict(starved=1), dict(fill=1)):
        assert not follow_aggregate([fobs("paced50", **kw)])[1]["execution_bound"], kw
    for kw in (dict(chain=80.0), dict(chain=120.0), dict(rtt=None), dict(rtt=0.05), dict(rtt=120.0), dict(undelayed=50.0),
               dict(relay_conns=0)):
        assert not follow_aggregate([fobs("rtt100", **kw)])[1]["execution_bound"], kw
    assert not follow_aggregate([fobs("rtt100", mismatched=1)])[1]["follow_gate"]
    for kw in (dict(chain=80.0), dict(rtt=None), dict(rtt=100.0), dict(rtt=280.0), dict(undelayed=125.0),
               dict(undelayed=None), dict(relay_conns=0)):
        assert not follow_aggregate([fobs("rtt250", **kw)])[1]["execution_bound"], kw
    assert not follow_aggregate([fobs("rtt250", mismatched=1)])[1]["follow_gate"]
    for kw in (dict(stubs=9), dict(stub_min=1)):
        assert not follow_aggregate([fobs("serve10", **kw)])[1]["follow_gate"], kw
    for variant in FOLLOW:
        for gate, kw in {"run_integrity": dict(gas=1), "apphash_gate": dict(apphash="00" * 32), "admission_regime": dict(inner=5)}.items():
            assert not follow_aggregate([fobs(variant, **kw)])[1][gate], (gate, variant)
        assert not follow_aggregate([fobs(variant, state={"rc": 0, "total": "ab" * 32, "verify_pass": True})])[1]["state_gate"]
        assert not follow_aggregate([fobs(variant, receipts=good_receipts(variant, success="1"))])[1]["receipt_gate"]
        assert not follow_aggregate([fobs(variant, rc=1)])[1]["run_integrity"]
    assert not follow_aggregate([fobs("serve10", receipts=good_receipts("serve10", receipt_digest="ee" * 32))])[1]["receipt_gate"]
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
        return teardown()
    if a.mode == "tools":
        return tools(os.path.abspath(a.tree), os.path.abspath(a.out))
    variant = variant_of(a.workload)
    if variant is None:
        print(f"unknown fullsync workload {a.workload!r}; fullsync or fullsync:<{'|'.join(VARIANTS)}>", file=sys.stderr)
        return 2
    if a.mode == "check":
        problem = netem_problem(variant)
        if problem:
            print("refused: " + problem, file=sys.stderr)
            return 2
        return 0
    if a.mode == "assets":
        return 0
    tree, out = os.path.abspath(a.tree), os.path.abspath(a.out)
    if a.mode == "setup":
        return setup(tree, out, variant)
    return measure(tree, out, os.path.abspath(a.tools) if a.tools else "", a.workload, variant)


if __name__ == "__main__":
    sys.exit(main())
