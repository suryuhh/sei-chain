#!/usr/bin/env python3
"""Execute-loop benchmark: one Autobahn EVM-only validator in process, read at its execute loop.

One process of the sei-tendermint/internal/p2p test binary (the harness copied into that package under the
`benchharness` build tag) runs a one-validator Autobahn EVM-only node with the real evmonlyapp, Giga executor,
FlatKV, littidx receipt store, fsynced block store and hash vault, and runExecute, at GOMAXPROCS = OCCWorkers =
ParseWorkers = the machine's CPUs. It presigns 120 blocks of 2,000 transactions, admits them, inserts them through
the producer's mempool with the first block held until the producer is at capacity, and times the node's committed
blocks. Each reading runs two fresh processes and sums them. After each process exits, the committed store is
checked by readers built from the base tree (bench/lib/common.py): the FlatKV LtHash recomputed by seidb
dump-flatkv, and digests over every receipt and log by the receipt reader.

Workloads (compare.sh name[:variant]):
  loop-erc20[:self|foreign|carried]  conflict-free ERC-20 transfers (the evmonly-loadtest erc20-transfer scenario).
      self: the node's own CheckTx admitted every transaction before the first block (its sender cache is warm);
      foreign: the same admission data without CheckTx, so PrepareBlock recovers every sender, as a validator does
      for blocks it did not admit; carried: a second application's CheckTx admitted them and the node receives the
      producer's sender keys with the block (on a tree without the sender-key sidecar this equals foreign).
  loop-native[:self|foreign|carried]  native 21,000-gas value transfers (the evmonly-loadtest default scenario).
  loop-replay-conflicts[:eth|eth_nochain|eth_chainonly|sei|hot1|chain1|eth_touch|eth_touchcode|cf]  ERC-20 blocks
      (carried) whose conflict shape replays recorded traffic: eth lays window b mod 8 of eight Ethereum mainnet
      windows on block b, each transaction keeping its real sender (nonce chains kept) and paying its real hottest
      written key; eth_nochain keeps the holders with one transaction per sender; eth_chainonly keeps the senders
      with no shared holder; sei lays three pooled Sei mainnet windows; hot1 pays one holder with every transfer;
      chain1 makes each block one sender's 2,000-long nonce chain; eth_touch calls 64 contracts that read every
      storage key each real transaction read and read-modify-write every key it wrote; eth_touchcode adds each
      touched contract's real runtime code; cf is the conflict-free block.
  loop-swaps[:swap|swap_hot|swap_native|swap_native_mixed]  SushiSwap V2 router swaps (the vendored factory and
      router, 64 pools); swap_hot puts a fifth of each block on pool 0; swap_native trades WSEI paid in the native
      coin; swap_native_mixed pays half in and half out in the native coin.

Figures: steady_txps (transactions over the steady span's wall, summed over the processes), tx_per_cpu_s, the
executor's share of the main loop, per-block loop execution, storage and prepare-parse time, peak RSS of the node
process (wait4); loop-native adds fed_txps (the steady span up to the last block the inserter still fed).

Gates, per process: run_integrity (exit 0, every transaction succeeded with its receipt, the pinned total gas, 120
blocks of exactly 2,000, the laid schedule is the one named, the producer's nonce table never ran ahead of the
application), admission_regime (the admission path the variant names ran on every transaction), execution_bound
(the node, not the inserter, set the rate: a backlog of at least 10 blocks before the first, no steady block
starved, and on average at least 5 blocks queued between receive and execute), state_gate (the recomputed LtHash
equals the node's and the pinned value), apphash_gate (the final app hash and the digest over all 120 app hashes
equal the pinned values), receipt_gate (latest height, receipt counts and the receipt and log digests equal the
pinned values).
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "lib"))
import common  # noqa: E402

PKG = os.path.join("sei-tendermint", "internal", "p2p")
BIN_NAME = "loop.test"
REPEATS = 2
BLOCKS = 120
TXS_PER_BLOCK = 2000
EXPECTED_TXS = BLOCKS * TXS_PER_BLOCK
EXPECTED_LATEST = BLOCKS + 1  # genesis state is version 1; blocks run at heights 2..121
MIN_RECEIVE_EXECUTE_DEPTH = 5.0
MIN_NONCE_CHECKS = 1000  # table answers compared with the app's own read, per process
MIN_NONCE_CHECKS_BY_VARIANT = {"chain1": 1}
MIN_FILL_BLOCKS = 10  # the fill before the first block hands over at least this many blocks
GAUGE_MS = "20"
PROCESS_TIMEOUT_SECONDS = 400
STORE_PREFIX = "bench-loop-"
WITNESS_TOLERANCE = 0.001
REPLAY_ASSET = "loop-replay.tar.xz"

# Pinned outputs. Every change measured with this benchmark leaves them unchanged.
ERC20_CONSTANTS = {"gas": 8280501840, "final_apphash": "96847808f191b8228618b36e4fd6b303bebf2d10addcbe17efe6278063eb004a", "apphash_digest": "ad8f106a16543690a84e9e94e8b4f50837cd16fd978e709995f00a09010956c9", "lthash": "a87b924c9489d977464005863865da01f9e1666e390ce0b69192355df9a85634", "receipt_digest": "12aa4bcaa3a0e6cf3f0bf1451eb72f48752d12dda81961815d7b495336cb4fee", "filter_digest": "789e7a426eb8b1392b23c2d1b8588614791cb6706e5c80a4dfce6f6f8eedd9b3"}
# Transfers log nothing, so filter_digest is the SHA-256 of empty input and receipt_digest carries the receipt check.
NATIVE_CONSTANTS = {"gas": 5040000000, "final_apphash": "fee71190f09387a0b0c4cf4634ace1edd8c5f0361230313c6028ee376d7c9734", "apphash_digest": "22519220bad47c3d509da05bc0a494dd7cbfb53696be36602800e6aa3b29e63a", "lthash": "7935ae3aa7e25718bbf3d0907749d76bf2e7cb17067b435a8f14e68e03e36579", "receipt_digest": "90790170baefb24e4456de9929a7c833a1c62df42e48bed4b39505b319624639", "filter_digest": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
CHAIN1_CONSTANTS = {"gas": 8314705440, "final_apphash": "69b8f93a99eaadba6b5ddf2572f749bccc3893679acc6ff41124453e8a6526d0", "apphash_digest": "31b8f84904271ccf609ac258039e759e2f4a94d8367ad6a6e69f19dc831e85d1", "lthash": "564c875dca4e27a3aed1946643df22266e3bfdc5f8a21007e0b8b8fcbf949d47", "receipt_digest": "63b77625e5eed683290b66a054cf2a1a15e568dde85634780f18da2eb5bf244c", "filter_digest": "6c86e644de695f4b4bc38466e7057d7be39fdeb72d3eb19a25cadf07d708e8cf"}
TOUCH_CONSTANTS = {"gas": 14952957900, "final_apphash": "a384c80b7e3fdadc66da2590adb706f4b9b3a589894f5594586fbe2f34bd5a0a", "apphash_digest": "f51a88c98b6d2282a7b83af5a684d4914de8022c47a592e795fdfda817305fee", "lthash": "87af84e7bfe1515df29efe5ec15dfe0005f86d775c832861d2744bbb4d2093e1", "receipt_digest": "7be0914764c808fa2b04a9d141a36b8b96db6a01ecb5f9c6389b4f851aeb3d3a", "filter_digest": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"}
TOUCHCODE_CONSTANTS = dict(TOUCH_CONSTANTS, lthash="aac86cbbe8328dbb39c9279e15ba88c882ef534311e570c9e61b9743ed3862a0")
SWAP_CONSTANTS = {"gas": 24017509270, "final_apphash": "4d02f4115ccde954d5efe372294886babca64971e301d011866d4339cf920d0d", "apphash_digest": "a456e4bad446912980a9ab053c5660400f0e548a0d920c411c9264335806d4c9", "lthash": "48b8dd1b5d8038ced81b82ef8c956b0a0e09495cad9c4fa80b6c5c19644dadcf", "receipt_digest": "0f166e95a531b88adf69a09dfd607ffca4f5b12261c56a5a301da6705e5436e9", "filter_digest": "781ca3b08bc8dc5c9952750723d4539a6e3449ba9e82d6a5d7742694dc560a7c"}
SWAP_HOT_CONSTANTS = {"gas": 23688185070, "final_apphash": "4ffdecc0f75bd51825ded5ebfe465009d584dda2f2f383128c6bd2afb602a377", "apphash_digest": "151412475d5c83ac9c666bfa485c7ccc670894b2ea006a16d43cab385f6bf30c", "lthash": "7314ae4f9e1ece5a6523ec17d1d2ce4d20e732d9b01c8bce79083e564b19eeea", "receipt_digest": "4b908b479afb508aacb238c7780f3314c1bbb8af0c367d8eadc86b96505a58f2", "filter_digest": "99b747efef519125c301f91c35012e61e90110c7a6fa58e9b2cba3c674400f30"}
SWAP_NATIVE_CONSTANTS = {"gas": 25128519442, "final_apphash": "265bdd6de5c8f924687888c73086dff5563ca580a434ddd8890ef0dc72807689", "apphash_digest": "c817d2d975d24d5b9312e44a538ad5c7edddc5af42766923c151b556d11af4b1", "lthash": "1aa9f8c1b7d8860f5bda9863bd41b4a2a7d5d29bb613daf9cc25bc939fb9764b", "receipt_digest": "4941519d6b35329131636f023835d4a27e536c8870e04fa652d35200db6c6341", "filter_digest": "e03c07e7fb6c6f13b1b66e66317f28c41adcd4998ceec3646c303b1d781f8ac7"}
SWAP_NATIVE_MIXED_CONSTANTS = {"gas": 25440148872, "final_apphash": "2a855bf03bdbcbb4d5b62d0860838e41e54c4fde2b385b938353d7ed727f3a68", "apphash_digest": "68f8e9206a4b89050f7d41893f08330e9a17dfe0420cf654240ad23f329934ab", "lthash": "974f5c0933d1b9b25c80300318a7b5d06943a6fedc74e09b4457355c5f589ad1", "receipt_digest": "825dd0e3f2243effb9821fa8dd34a7227b64cc54df2b29ed2a2fa6090ecef6cd", "filter_digest": "cb461d7a6a081174d00bf7a1c7c2e1389da14b99bc5f8761d9af1ed24b76a6ac"}
SEI_CONSTANTS = {"gas": 8282098320, "final_apphash": "7509996e3a0c4d3a582bcd8565ef92c9f3c8b45bf523b22ebc02103536416558", "apphash_digest": "7c9e1e1ab68b77f910ba09486168b5d6dfd8c23a0b712090a70703d5b22378fe", "lthash": "91759c17c240fb97418dd22824fe66ea03d077d22a7c08df136a88453ec78bfe", "receipt_digest": "6cd37ce844118996e9ba03c55c3228689abaf04b77223f51f55ff858ab0b4417", "filter_digest": "091ab65cbac098078053a4981357b746cb73519d1aea80d0f4dd55a120a889a9"}
REPLAY_CONSTANTS = {
    "cf": ERC20_CONSTANTS,
    "eth": {"gas": 8285669820, "final_apphash": "cf9bdc39b7ecc2dc000af76967b8879bbf5e4d214c744c2a85426e169cfa4822", "apphash_digest": "0288d2fceb14568242bfb805b2f8ec4d1793c5e26dc81d926ef060c6b4d91e1e", "lthash": "41c030c8f5cfc365b559f99a290f808c6fb44ae716bea4f7b2d9e8dc4716db41", "receipt_digest": "1f9ab4fa8058443cb0651ac339bf6e85cbc1bee5ca81db44710b8138fc97fd61", "filter_digest": "740bfe5d2bd1ed6521ae3e2929fb1123acf0f75e7f7e0f124d979006290da4a4"},
    "eth_nochain": {"gas": 8285669100, "final_apphash": "9489b3265d9b271ce10c6e69bc0119574f7518e8e5811ea60af6c1e5bd7795c0", "apphash_digest": "6a91cc37612e0b87caab5e39fc587c75b9fbc4004c2b12fa082f0347bfacd463", "lthash": "7ccd26150ccd8e802fea46ce0aa8c6cfced4ec66e08ae3f0641c05e2c677b1b9", "receipt_digest": "f376d73aaa0693fc62e8b8edabad392c15561d2e41cdb574c0feb65b96448a4f", "filter_digest": "2523bd68a3f1a7784386e062d26c0d1be36fffdf4c7ebafad5aaf903c1d5cb85"},
    "eth_chainonly": {"gas": 8280491580, "final_apphash": "11809cb13bee40f3d2f5e11d3c347e5f32324b647455625c3353cd0c0c3d6fff", "apphash_digest": "ae2ad12ec4efed9c10a8d29dedb747d0551dbedb9a4388058a20a1d2e13817ca", "lthash": "7e2143e38802019c651383aa4ff67b4a2d7aca7c6d09ace5a2b92e46d46944c4", "receipt_digest": "509bed173438dd4f8d786a09b2fc22947e0a04542d02ac4854fb878ef51ac44d", "filter_digest": "8bb0ea5769657960428881f09184e023e131f619bf163fa68b2d3bbe8c2c858f"},
    "chain1": CHAIN1_CONSTANTS, "sei": SEI_CONSTANTS, "eth_touch": TOUCH_CONSTANTS, "eth_touchcode": TOUCHCODE_CONSTANTS,
    "swap": SWAP_CONSTANTS, "swap_hot": SWAP_HOT_CONSTANTS, "swap_native": SWAP_NATIVE_CONSTANTS, "swap_native_mixed": SWAP_NATIVE_MIXED_CONSTANTS,
    "hot1": {"gas": 8280737100, "final_apphash": "6692abde0b0bcba655a655d373f4d97b0fb99ccfd510b7cf06c54a4b291714b3", "apphash_digest": "8c18ef99edfa555ff1867f152c35e9cc8da1efe620affeae3e3f47295989f856", "lthash": "4b105e1ee9a0e19c6d871bd2109c2b1c4f2f069b141977418cda66af6bf22743", "receipt_digest": "98ed181dd7a7596f26ae647a6e8c8b7e943b5b786f2e5eac12a0f4aec0fb7ec0", "filter_digest": "69ab0129572054f44b6d6a7fbc91fd81d9f05eff8d96e04d9ee1195dbafddb8a"},
}
REPLAY_WINDOWS = {"eth": 8, "eth_nochain": 8, "eth_chainonly": 8, "chain1": 8, "sei": 3}
TOUCH_WINDOWS = {"eth_touch": 8, "eth_touchcode": 8}
TOUCH_FILE = {"eth_touch": "eth_touch", "eth_touchcode": "eth_touch"}
TOUCH_CODE_FILE = {"eth_touchcode": "eth_touchcode_codes"}
TOUCH_CODE_SHA256 = "6982df35cc34f78b928d790e94d5341df6849b429336679f6a694c19904fb22a"
TOUCH_CODE_TOTAL_BYTES = "716016"
SWAP_FILE = {"swap": "swap_bins", "swap_hot": "swap_bins", "swap_native": "swap_bins_native", "swap_native_mixed": "swap_bins_native"}
SWAP_POOLS = {"swap": "64", "swap_hot": "64", "swap_native": "64", "swap_native_mixed": "64"}
SWAP_HOT = {"swap": "0", "swap_hot": "200", "swap_native": "0", "swap_native_mixed": "0"}
SWAP_NATIVE = {"swap_native": "1", "swap_native_mixed": "1"}
SWAP_NATIVE_OUT = {"swap_native": "0", "swap_native_mixed": "500"}
SWAP_SHA256 = "14048e05f3d87fb136ddfd2870406d9f4c13cd17e3131fab5ee2419c6147c4e5"
SWAP_CODE_SHA256 = "734ddcf93e53259e0951980baaf8fe90fad5532cd2c1d75cbd96a39d96bac720"
SWAP_NATIVE_SHA256 = "9f4cbffee84ae8ea0c7df6e6cb8dd88590f9d36a8e5e5e92621a38ec5a9297db"
SWAP_NATIVE_CODE_SHA256 = "11f652ec94b4d9eb0488df2443a784b7078ca525ac0ca5be20b3cacd666c78fd"
SWAP_FILE_SHA256 = {"swap_bins": SWAP_SHA256, "swap_bins_native": SWAP_NATIVE_SHA256}
HOT_PERMILLE = {"hot1": "1000"}
SWAP_VARIANTS = tuple(SWAP_FILE)
CONFLICT_VARIANTS = tuple(v for v in REPLAY_CONSTANTS if v not in SWAP_FILE)

# compare.sh workload name -> (kind, variants, default variant)
WORKLOADS = {
    "loop-erc20": ("erc20", ("self", "foreign", "carried"), "carried"),
    "loop-native": ("native", ("self", "foreign", "carried"), "carried"),
    "loop-replay-conflicts": ("replay", CONFLICT_VARIANTS, "eth"),
    "loop-swaps": ("replay", SWAP_VARIANTS, "swap"),
}
GATES = ("run_integrity", "admission_regime", "execution_bound", "state_gate", "apphash_gate", "receipt_gate")
FIGURES = {
    "steady_txps": {"unit": "tx/s", "better": "higher", "effect": "relative"},
    "tx_per_cpu_s": {"unit": "tx/cpu-s", "better": "higher", "effect": "relative"},
    "executor_share": {"unit": "fraction", "better": "lower", "effect": "absolute"},
    "loop_execution_ms": {"unit": "ms/block", "better": "lower", "effect": "relative"},
    "loop_storage_ms": {"unit": "ms/block", "better": "lower", "effect": "relative"},
    "prepare_parse_ms": {"unit": "ms/block", "better": "lower", "effect": "relative"},
    "peak_rss_mb": {"unit": "MB", "better": "lower", "effect": "absolute"},
}
NATIVE_FIGURES = dict(list(FIGURES.items())[:1] + [("fed_txps", {"unit": "tx/s", "better": "higher", "effect": "relative"})] + list(FIGURES.items())[1:])
WITNESSED = ("steady_txps", "tx_per_cpu_s", "executor_share", "loop_execution_ms", "loop_storage_ms", "prepare_parse_ms", "peak_rss_mb")


def resolve(workload):
    """(name, kind, variant) for a compare.sh workload string, or None."""
    name, _, variant = workload.partition(":")
    if name not in WORKLOADS:
        return None
    kind, variants, default = WORKLOADS[name]
    variant = variant or default
    return (name, kind, variant) if variant in variants else None


def replay_dir(data):
    return os.path.join(data, "loop-replay")


def variant_env(kind, variant, data):
    if kind in ("erc20", "native"):
        return {"NODE1_ADMIT": variant, "NODE1_WORKLOAD": "erc20-transfer" if kind == "erc20" else "transfer", "NODE1_NONCE": "table"}
    d = replay_dir(data)
    m = variant
    return {"NODE1_ADMIT": "carried", "NODE1_WORKLOAD": "erc20-transfer", "NODE1_NONCE": "table",
            "NODE1_REPLAY": os.path.join(d, f"{m}.json") if m in REPLAY_WINDOWS else "",
            "NODE1_REPLAY_TOUCH": os.path.join(d, f"{TOUCH_FILE[m]}.json") if m in TOUCH_WINDOWS else "",
            "NODE1_REPLAY_TOUCH_CODE": os.path.join(d, f"{TOUCH_CODE_FILE[m]}.json") if m in TOUCH_CODE_FILE else "",
            "NODE1_SWAP": os.path.join(d, f"{SWAP_FILE[m]}.json") if m in SWAP_FILE else "",
            "NODE1_SWAP_POOLS": SWAP_POOLS.get(m, ""), "NODE1_SWAP_HOT_PERMILLE": SWAP_HOT[m] if m in SWAP_FILE else "",
            "NODE1_SWAP_NATIVE": SWAP_NATIVE.get(m, ""), "NODE1_SWAP_NATIVE_OUT_PERMILLE": SWAP_NATIVE_OUT.get(m, ""),
            "NODE1_HOT_PERMILLE": HOT_PERMILLE.get(m, "0"), "NODE1_HOT_KEYS": "1"}


def constants(kind, variant):
    if kind == "erc20":
        return ERC20_CONSTANTS
    if kind == "native":
        return NATIVE_CONSTANTS
    return REPLAY_CONSTANTS[variant]


def swap_line_expected(variant):
    native = variant in SWAP_NATIVE
    pools = int(SWAP_POOLS[variant])
    return {"native": "true" if native else "false", "out_permille": SWAP_NATIVE_OUT.get(variant, "0"), "path": f"{SWAP_FILE[variant]}.json",
            "sha256": SWAP_FILE_SHA256[SWAP_FILE[variant]], "pools": SWAP_POOLS[variant], "pairs": SWAP_POOLS[variant],
            "hot_permille": SWAP_HOT[variant], "contracts": str(2 * pools + 3 + (1 if native else 0)),
            "code_sha256": SWAP_NATIVE_CODE_SHA256 if native else SWAP_CODE_SHA256, "gas_limit": "400000"}


# ------------------------------------------------------------------------------------------------
# parsing the harness's NODE1 lines

def key_values(text):
    out = {}
    for token in text.split():
        if "=" in token:
            key, value = token.split("=", 1)
            out[key] = value
    return out


LINE_KINDS = ("cfg", "admission", "window", "steady", "fedsteady", "feed", "feeddiag", "success", "state", "proc", "paths",
              "replay", "touch", "touchcode", "swap", "hot")


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
        elif kind in LINE_KINDS:
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
# readers built from the base tree

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


def run_receipt_reader(reader, store):
    proc = subprocess.run([reader, "--receipt-dir", str(store / "node0/data/ledger/receipt/littidx"), "--expect-latest", str(EXPECTED_LATEST)],
                          text=True, capture_output=True, check=False, timeout=300)
    values = {}
    for line in proc.stdout.splitlines():
        if line.startswith("latest="):
            values = key_values(line)
    return {"rc": proc.returncode, "values": values}


# ------------------------------------------------------------------------------------------------
# one process and its checks

def run_process(binary, env_extra):
    store = Path(tempfile.mkdtemp(prefix=STORE_PREFIX))
    env = dict(os.environ)
    env.update(env_extra)
    env.update({"NODE1_BLOCKS": str(BLOCKS), "NODE1_TXS": str(TXS_PER_BLOCK), "NODE1_DIR": str(store)})
    for key in ("GOGC", "GOMEMLIMIT", "GOMAXPROCS", "GODEBUG"):
        env.pop(key, None)
    started = time.monotonic()
    out_path = store / "node-stdout.txt"
    with open(out_path, "w") as out:
        proc = subprocess.Popen([binary, "-test.run", "^TestNode1$", "-test.count=1", f"-test.timeout={PROCESS_TIMEOUT_SECONDS}s"],
                                env=env, stdout=out, stderr=subprocess.STDOUT)
        killer = threading.Timer(PROCESS_TIMEOUT_SECONDS + 60, proc.kill)
        killer.start()
        try:
            _, status, rusage = os.wait4(proc.pid, 0)
        finally:
            killer.cancel()
    rc = os.waitstatus_to_exitcode(status)
    wall = time.monotonic() - started
    stdout = out_path.read_text(errors="replace")
    return {"store": store, "rc": rc, "stdout": stdout, "wall_s": wall, "rusage_maxrss_kb": rusage.ru_maxrss}


def process_observation(kind, variant, run, checked):
    parsed, L = checked["parsed"], checked["parsed"]["lines"]
    obs = {"rc": run["rc"], "external_wall_s": run["wall_s"]}

    def num(k, key, cast=float):
        try:
            return cast(L[k][key])
        except (KeyError, ValueError):
            return None

    steady_txs, steady_wall = num("steady", "txs", int), num("steady", "wall_s")
    steady_blocks, steady_ms = num("steady", "blocks", int), num("steady", "ms_per_block")
    fed_txs, fed_wall = num("fedsteady", "txs", int), num("fedsteady", "wall_s")
    fed_blocks, fed_ms = num("fedsteady", "blocks", int), num("fedsteady", "ms_per_block")
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
        "fed_txs": fed_txs, "fed_wall_s": fed_wall, "fed_blocks": fed_blocks, "fed_ms_per_block": fed_ms,
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
        "feed": L.get("feed", {}), "feeddiag": L.get("feeddiag", {}), "admission": L.get("admission", {}),
        "state_reader": checked["state"], "receipt_reader": checked["receipts"],
        "final_apphash": L.get("state", {}).get("final_apphash"), "apphash_digest": L.get("state", {}).get("apphash_digest"),
        "node_lthash": L.get("state", {}).get("flatkv_lthash"), "gomaxprocs": L.get("cfg", {}).get("gomaxprocs"),
        # engagement readings, never gates
        "carried_batches_verified": parsed["count"].get("giga_evmonly_carried_key_batches_total outcome=verified", 0),
        "carried_batches_fallback": parsed["count"].get("giga_evmonly_carried_key_batches_total outcome=fallback", 0),
        "carried_bytes": int(L.get("admission", {}).get("carried_bytes", "0") or 0),
        "occ_reruns": parsed["count"].get("giga_occ_reruns_total -", 0),
    })
    C = constants(kind, variant)
    s, st = L.get("success", {}), L.get("state", {})
    workload_name = "transfer" if kind == "native" else "erc20-transfer"
    if kind == "replay":
        m = variant
        rp = L.get("replay", {})
        schedule_ok = (rp.get("path") == f"{m}.json" and rp.get("windows") == str(REPLAY_WINDOWS[m])) if m in REPLAY_WINDOWS else not rp
        tp = L.get("touch", {})
        schedule_ok = schedule_ok and ((tp.get("path") == f"{TOUCH_FILE[m]}.json" and tp.get("windows") == str(TOUCH_WINDOWS[m]) and tp.get("contracts") == "64")
                                       if m in TOUCH_WINDOWS else not tp)
        tc = L.get("touchcode", {})
        schedule_ok = schedule_ok and ((tc.get("path") == f"{TOUCH_CODE_FILE[m]}.json" and tc.get("sha256") == TOUCH_CODE_SHA256
                                        and tc.get("codes") == "64" and tc.get("total_bytes") == TOUCH_CODE_TOTAL_BYTES)
                                       if m in TOUCH_CODE_FILE else not tc)
        sw = L.get("swap", {})
        schedule_ok = schedule_ok and (all(sw.get(k) == v for k, v in swap_line_expected(m).items()) if m in SWAP_FILE else not sw)
        hot = L.get("hot", {})
        schedule_ok = schedule_ok and hot.get("permille") == HOT_PERMILLE.get(m, "0") and hot.get("keys") == "1"
        obs.update({"replay": rp, "touch": tp, "touchcode": tc, "swap": sw, "hot": hot})
        min_nonce = MIN_NONCE_CHECKS_BY_VARIANT.get(m, MIN_NONCE_CHECKS)
    else:
        schedule_ok = True
        min_nonce = MIN_NONCE_CHECKS
    obs["run_integrity"] = bool(
        schedule_ok and run["rc"] == 0 and L.get("cfg", {}).get("workload") == workload_name and s.get("ok") == str(EXPECTED_TXS)
        and s.get("failed") == "0" and s.get("receipts_ok") == str(EXPECTED_TXS) and s.get("gas_used") == str(C["gas"])
        and st.get("blocks") == str(BLOCKS) and st.get("block_txs_min") == str(TXS_PER_BLOCK)
        and st.get("block_txs_max") == str(TXS_PER_BLOCK) and steady_txs and steady_wall and window_txs and cpu_window and loop_total)
    # the producer's app nonce reads were answered from the harness table, which never read above the app's own answer
    nd = L.get("feeddiag", {})
    try:
        nonce_ok = nd.get("nonce_table") == "true" and nd.get("nonce_ahead") == "0" and int(nd.get("nonce_checked", "0")) >= min_nonce
    except ValueError:
        nonce_ok = False
    obs["run_integrity"] = bool(obs["run_integrity"] and nonce_ok)
    a = L.get("admission", {})
    admit = "carried" if kind == "replay" else variant
    if admit == "self":
        # where the tree returns a public key from CheckTx, it is the sender's own on every tx
        obs["admission_regime"] = (a.get("inner_checktx") == str(EXPECTED_TXS + 1) and a.get("precheck_in_window") == "0"
                                   and a.get("admission_mismatch") == "0" and a.get("key_mismatch") == "0"
                                   and a.get("key_checks") in ("0", str(EXPECTED_TXS + 1)))
    else:
        obs["admission_regime"] = a.get("inner_checktx") == "0" and a.get("admission_mismatch") == "0"
        if admit == "carried":
            # the admitting validator: a second application's real CheckTx on every tx, all before the window
            obs["admission_regime"] = (obs["admission_regime"] and a.get("admitter_checktx") == str(EXPECTED_TXS + 1)
                                       and a.get("precheck_in_window") == "0")
    fd, depth = L.get("feed", {}), obs["receive_execute_depth"]
    try:
        feed_ok = (fd.get("gauge_ms") == GAUGE_MS and fd.get("steady_starved_blocks") == "0"
                   and int(fd.get("fill_txs", "0")) >= MIN_FILL_BLOCKS * TXS_PER_BLOCK)
    except ValueError:
        feed_ok = False
    obs["execution_bound"] = bool(feed_ok and depth is not None and depth >= MIN_RECEIVE_EXECUTE_DEPTH)
    sr = checked["state"]
    obs["state_gate"] = bool(sr["rc"] == 0 and sr["verify_pass"] and sr["total"] == C["lthash"] and st.get("flatkv_lthash") == C["lthash"])
    obs["apphash_gate"] = st.get("final_apphash") == C["final_apphash"] and st.get("apphash_digest") == C["apphash_digest"]
    rv = checked["receipts"]["values"]
    obs["receipt_gate"] = bool(
        checked["receipts"]["rc"] == 0 and rv.get("latest") == str(EXPECTED_LATEST) and rv.get("receipts") == str(EXPECTED_TXS)
        and rv.get("success") == str(EXPECTED_TXS) and rv.get("failed") == "0"
        and rv.get("receipt_digest") == C["receipt_digest"] and rv.get("filter_digest") == C["filter_digest"])
    return obs


def aggregate(observations, with_fed=False):
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
    if with_fed:
        ft, fw = total("fed_txs"), total("fed_wall_s")
        values["fed_txps"] = ft / fw if ft and fw else None
        fb = [(o.get("fed_blocks"), o.get("fed_ms_per_block")) for o in observations]
        if all(b and x for b, x in fb):
            witness["fed_txps"] = sum(b * TXS_PER_BLOCK for b, _ in fb) / sum(b * x / 1000.0 for b, x in fb)
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
    gates = {gate: all(bool(o.get(gate)) for o in observations) for gate in GATES}
    return values, witness, gates


# ------------------------------------------------------------------------------------------------
# modes

def setup(tree, out, data, workload):
    name, kind, variant = resolve(workload)
    os.makedirs(out, exist_ok=True)
    if kind == "replay":
        extract_replay(data)
        harness = [("bench_loop_replay_test.go", "bench_loop_test.go"), ("bench_loop_prof_test.go", "bench_loop_prof_test.go")]
    else:
        harness = [("bench_loop_test.go", "bench_loop_test.go")]
    for src, dst in harness:
        common.place_harness(os.path.join(HERE, "harness", src), tree, PKG, dst)
    env = common.go_env(tree)
    binary = os.path.join(out, BIN_NAME)
    subprocess.run(["go", "test", "-c", "-tags", "benchharness", "-o", binary, "./" + PKG + "/"], cwd=tree, env=env, check=True)
    print(f"built {binary}")
    return 0


def extract_replay(data):
    target = replay_dir(data)
    archive = os.path.join(data, REPLAY_ASSET)
    stamp = os.path.join(target, ".from-" + str(os.path.getmtime(archive)))
    if os.path.exists(stamp):
        return
    shutil.rmtree(target, ignore_errors=True)
    with tarfile.open(archive, "r:xz") as tf:
        for entry in tf.getmembers():
            if not (entry.name == "loop-replay" or (entry.name.startswith("loop-replay/") and entry.isfile() and "/.." not in entry.name)):
                raise RuntimeError(f"unexpected archive entry {entry.name}")
        tf.extractall(data)
    open(stamp, "w").close()


def measure(tree, out, data, tools, workload):
    name, kind, variant = resolve(workload)
    spec = NATIVE_FIGURES if kind == "native" else FIGURES
    result = {"workload": workload, "figure_spec": spec, "primary": "steady_txps", "repeats": REPEATS}
    binary = os.path.join(out, BIN_NAME)
    seidb, reader = os.path.join(tools, common.SEIDB), os.path.join(tools, common.RECEIPT_READER)
    missing = [p for p in (binary, seidb, reader) if not os.access(p, os.X_OK)]
    if missing:
        result.update({"figures": {k: None for k in spec}, "gates": {g: False for g in GATES}, "problems": [f"missing {p}" for p in missing]})
        print("RESULT " + json.dumps(result, sort_keys=True))
        return 0
    env_extra = variant_env(kind, variant, data)
    observations = []
    for repeat in range(REPEATS):
        run = run_process(binary, env_extra)
        try:
            parsed = parse_node1(run["stdout"])
            state = run_state_gate(seidb, run["store"]) if run["rc"] == 0 else {"rc": None, "total": "NONE", "verify_pass": False}
            receipts = run_receipt_reader(reader, run["store"]) if run["rc"] == 0 else {"rc": None, "values": {}}
            obs = process_observation(kind, variant, run, {"parsed": parsed, "state": state, "receipts": receipts})
        finally:
            shutil.rmtree(run["store"], ignore_errors=True)
        obs["repeat"] = repeat
        observations.append(obs)
        for line in run["stdout"].splitlines():
            if line.startswith("NODE1 ") or "FAIL" in line or "panic" in line:
                print(line[:400])
    values, witness, gates = aggregate(observations, with_fed=kind == "native")
    figures = {k: values.get(k) for k in spec}
    disputes = common.apply_witness(figures, witness, WITNESS_TOLERANCE, [k for k in spec if k in WITNESSED or k == "fed_txps"])
    result.update({"figures": figures, "gates": gates, "witness": {k: v for k, v in witness.items() if v is not None},
                   "witness_disputes": disputes, "observations": observations,
                   "host": {"loadavg_after": list(os.getloadavg())}})
    print("RESULT " + json.dumps(result, sort_keys=True, default=str))
    return 0


def teardown():
    for path in Path(tempfile.gettempdir()).glob(STORE_PREFIX + "*"):
        shutil.rmtree(path, ignore_errors=True)
    return 0


def self_test():
    assert resolve("loop-erc20") == ("loop-erc20", "erc20", "carried")
    assert resolve("loop-native:self") == ("loop-native", "native", "self")
    assert resolve("loop-swaps:swap_native_mixed")[2] == "swap_native_mixed" and resolve("loop-swaps:eth") is None
    assert resolve("loop-replay-conflicts")[2] == "eth" and resolve("loop-replay-conflicts:swap") is None
    assert resolve("loop-erc20:bogus") is None and resolve("eth8") is None

    def fixture(kind="erc20", variant="carried", gas=None, apphash=None, depth=24.5, starved=0, inner=None, extra=()):
        C = constants(kind, variant)
        gas = C["gas"] if gas is None else gas
        apphash = C["final_apphash"] if apphash is None else apphash
        wl = "transfer" if kind == "native" else "erc20-transfer"
        admit = "carried" if kind == "replay" else variant
        inner = (EXPECTED_TXS + 1 if admit == "self" else 0) if inner is None else inner
        admitter = EXPECTED_TXS + 1 if admit == "carried" else 0
        return "\n".join([
            f"NODE1 cfg workload={wl} admit={admit} txs=2000 blocks=120 gomaxprocs=16",
            f"NODE1 admission presign_s=1 precheck_in_window=0 admission_mismatch=0 inner_checktx={inner} insert_s=3 key_field=true key_checks={inner} key_mismatch=0 admitter_checktx={admitter} carried_bytes=0",
            "NODE1 window blocks=119 txs=238000 wall_s=2.000000 blocks_per_s=59.5 tx_per_s=119000 ms_per_block=16.807",
            "NODE1 steady from_height=14 blocks=107 txs=214000 wall_s=1.712000 blocks_per_s=62.5 tx_per_s=125000 ms_per_block=16.000000",
            "NODE1 fedsteady blocks=90 txs=180000 wall_s=1.500000 tx_per_s=120000 ms_per_block=16.666667",
            f"NODE1 feed fill_s=0.400 fill_txs=60001 gauge_ms=20 steady_blocks=107 steady_starved_blocks={starved}",
            "NODE1 feeddiag win_insert_calls=180000 nonce_table=true nonce_checked=3750 nonce_behind=0 nonce_ahead=0",
            f"NODE1 success ok={EXPECTED_TXS} failed=0 expected={EXPECTED_TXS} receipts_ok={EXPECTED_TXS} receipts_failed=0 receipts_missing=0 gas_used={gas}",
            f"NODE1 state final_apphash={apphash} apphash_digest={C['apphash_digest']} blocks=120 first_height=2 last_height=121 block_txs_min=2000 block_txs_max=2000 flatkv_version=121 flatkv_lthash={C['lthash']}",
            "NODE1 proc cpu_window_s=23.800000 cpu_us_per_tx=100.0000 cores_busy=11.9 peak_rss_mb=900",
            "NODE1 wphase autobahn_main_loop_phase_duration_seconds_total phase=execution total_s=1.500000 ms_per_block=12.605042",
            "NODE1 wphase autobahn_main_loop_phase_duration_seconds_total phase=storage total_s=0.500000 ms_per_block=4.201681",
            "NODE1 wphase evmonly_prepare_phase_duration_seconds_total phase=parse total_s=0.900000 ms_per_block=7.563025",
            f"NODE1 gauge depth receive-execute n=100 mean={depth} min=0 max=30",
            "NODE1 hot permille=0 keys=1",
        ] + list(extra)) + "\n"

    def obs(kind="erc20", variant="carried", state=None, receipts=None, rc=0, **kw):
        C = constants(kind, variant)
        state = state or {"rc": 0, "total": C["lthash"], "verify_pass": True}
        receipts = receipts or {"rc": 0, "values": {"latest": str(EXPECTED_LATEST), "receipts": str(EXPECTED_TXS), "success": str(EXPECTED_TXS),
                                                    "failed": "0", "receipt_digest": C["receipt_digest"], "filter_digest": C["filter_digest"]}}
        run = {"rc": rc, "wall_s": 10.0, "rusage_maxrss_kb": 900 * 1024 * (1024 if sys.platform == "darwin" else 1), "stdout": fixture(kind, variant, **kw)}
        return process_observation(kind, variant, run, {"parsed": parse_node1(run["stdout"]), "state": state, "receipts": receipts})

    for kind, variant in (("erc20", "self"), ("erc20", "foreign"), ("erc20", "carried"), ("native", "carried"), ("replay", "cf")):
        values, witness, gates = aggregate([obs(kind, variant), obs(kind, variant)], with_fed=kind == "native")
        assert all(gates.values()), (kind, variant, gates)
        assert abs(values["steady_txps"] - 125000) < 1e-6 and abs(witness["steady_txps"] - 125000) < 1e-6
        assert abs(values["peak_rss_mb"] - 900) < 1e-9
        if kind == "native":
            assert abs(values["fed_txps"] - 120000) < 1e-6
    for gate, kw in {"run_integrity": dict(gas=1), "apphash_gate": dict(apphash="00" * 32), "execution_bound": dict(depth=0.5),
                     "admission_regime": dict(inner=5)}.items():
        _, _, gates = aggregate([obs(**kw), obs()])
        assert not gates[gate], gate
    _, _, gates = aggregate([obs(state={"rc": 0, "total": "ab" * 32, "verify_pass": True})])
    assert not gates["state_gate"]
    _, _, gates = aggregate([obs("native", "carried", **{}), obs("native", "carried", apphash=ERC20_CONSTANTS["final_apphash"])])
    assert not gates["apphash_gate"]
    # a replay variant whose schedule line is missing fails integrity
    _, _, gates = aggregate([obs("replay", "eth")])
    assert not gates["run_integrity"]
    _, _, gates = aggregate([obs("replay", "eth", extra=("NODE1 replay path=eth.json windows=8",))])
    assert gates["run_integrity"], gates
    figures = {"steady_txps": 100.0}
    assert common.apply_witness(figures, {"steady_txps": 100.05}, WITNESS_TOLERANCE, ["steady_txps"]) == [] and figures["steady_txps"] == 100.0
    assert common.apply_witness(figures, {"steady_txps": 101.0}, WITNESS_TOLERANCE, ["steady_txps"]) and figures["steady_txps"] is None
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
        common.build_tools(os.path.abspath(a.tree), os.path.abspath(a.out))
        return 0
    if resolve(a.workload or "") is None:
        print(f"unknown loop workload {a.workload!r}: one of " + "; ".join(
            f"{n}[:{'|'.join(v)}] (default {d})" for n, (_, v, d) in WORKLOADS.items()), file=sys.stderr)
        return 2
    if a.mode == "check":
        return 0
    if a.mode == "assets":
        print(REPLAY_ASSET if resolve(a.workload)[1] == "replay" else "")
        return 0
    tree, out, data = os.path.abspath(a.tree), os.path.abspath(a.out), os.path.abspath(a.data)
    if a.mode == "setup":
        return setup(tree, out, data, a.workload)
    return measure(tree, out, data, os.path.abspath(a.tools), a.workload)


if __name__ == "__main__":
    sys.exit(main())
