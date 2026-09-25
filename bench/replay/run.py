#!/usr/bin/env python3
"""Validator replay benchmark: recorded transactions applied as one block each through the EVM-only application.

One process of the sei-tendermint/internal/evmonlyapp test binary (built with bench_replay_test.go copied into that
package under the `benchharness` build tag) applies each window as one block at height 2 through
NewEVMOnlyApplication, on a fresh FlatKV / LtHash / receipt store whose genesis is the window's recorded start
state: PrepareBlock, then FinalizeBlock and Commit are timed, then AwaitCommits. GOMAXPROCS is BENCH_PROCS (default
16; the application sizes its executor from it), three repeats per window. Every workload takes the foreign-block
sender path a validator takes for blocks it did not admit: the CheckTx sender cache is empty; a tree with the
sender-key sidecar receives the producer's aligned keys, an older tree recovers senders during block application.

Workloads:
  eth8    eight recorded Ethereum mainnet windows of 2,000 transactions (starts 26037338 .. 26043463, every 875
          blocks), chain id 1
  sei2    two windows of Sei mainnet EVM traffic (574 and 1,131 transactions), each the transactions of consecutive
          blocks pooled into one block, chain id 1329
  mix250  the first 250 of those Sei transactions spread evenly through a 2,000-transaction block whose other
          transactions are ERC-20 transfers from fresh senders
  mix0    2,000 conflict-free ERC-20 transfers (the evmonly-loadtest ERC-20 shape)

Figures: txps = the workload's transactions over the sum of per-window median PrepareBlock + FinalizeBlock + Commit
wall; worst_window_ms = the largest per-window median; cpu_us_per_tx = the mean over windows of the per-window
median process CPU per transaction from PrepareBlock to AwaitCommits; peak_rss_mb = the largest process RSS.

Gates (every repeat of every window): run_integrity (exit 0, PASS, every window and repeat reported with the
recorded transaction, account and slot counts, and the wall-clock bracket lines present), state_gate (app hash,
FlatKV LtHash and the digest over app hash, per-transaction gas and errors equal the pinned values), outcome_gate
(successful and failed counts, matches with the source chain's receipts, and receipts stored equal the pinned
values).

Usage (compare.sh drives it):
  run.py setup   --tree CHECKOUT --out DIR --data DIR --workload W   copy the harness in, build the test binary
  run.py measure --tree CHECKOUT --out DIR --data DIR --workload W   one reading; prints RESULT {json}
  run.py self-test
"""
import argparse
import hashlib
import io
import json
import os
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tarfile
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "lib"))
import common  # noqa: E402
HARNESS_DIR = os.path.join(HERE, "harness")
PKG = os.path.join("sei-tendermint", "internal", "evmonlyapp")
APP_GO = os.path.join(PKG, "app.go")
BIN_NAME = "replay.test"

WORKLOADS = ("eth8", "sei2", "mix0", "mix250")
MIX_S = {"mix0": 0, "mix125": 125, "mix250": 250}
MIX_N = 2000
MIX_START = 100000
STARTS = (26037338, 26038213, 26039088, 26039963, 26040838, 26041713, 26042588, 26043463)
REPS = 3
DEFAULT_PROCS = 16
TXS = 2000
PROCESS_TIMEOUT_SECONDS = 600
RUN_PREFIX = "bench-replay-run-"

ARCHIVE_NAME = "windows.tar.xz"
MANIFEST_NAME = "windows-manifest.json"
SEI_ARCHIVE_NAME = "sei.tar.xz"
SEI_MANIFEST_NAME = "sei-manifest.json"
ARCHIVE_SHA256 = "977fd8397520f5b9517c0a9f82452bc84c2c2422fafe16a4a475a05fb12fdd1b"
MANIFEST_SHA256 = "7f89c87b87dd766e024817c2d2abdaa40bb394cc4cab43dbda8f57a21bca955b"
SEI_ARCHIVE_SHA256 = "4c815603d23dfea05eba2cd96faa97bab8eee98272d9611d065c0f0ac8d3f62d"
SEI_MANIFEST_SHA256 = "469707dfc5e2621784e83f3ad5bf19fb110af38f7ea1e9cf11ac4dfd74f3bee8"
SEI_STARTS = (0, 100000)
SEI_CHAIN_ID = 1329

# The harness installs an executor built as the application's newExecutor builds it, less the minimum gas price
# check. It refuses a tree whose newExecutor differs from this body (whitespace aside), since the copy would not
# match what the tree runs.
NEW_EXECUTOR_BODY = """
return evmonly.NewExecutor(evmonly.Config{
ChainConfig: a.chainConfig,
MinGasPrice: big.NewInt(evmOnlyMinGasPrice),
OCCWorkers: runtime.GOMAXPROCS(0),
ParseWorkers: runtime.GOMAXPROCS(0),
RejectUnappliableTxs: true,
BlockResultPoolSize: 1,
},
evmonly.WithStorageManager(a.storage, a.changeSetEncoder),
evmonly.WithMissingAccountState(evmOnlyFundedState{}),
evmonly.WithStoreIndependentBlockChangeSetEncoder(a.encodeCursorChangeSet),
)
"""

# Per window: what the harness must print on every repeat. app hash, LtHash and digest are the state gate; ok,
# failed, mainnet_match and receipts the outcome gate.
EXPECTED = {
    26037338: {"txs": 2000, "blobs_dropped": 17, "accts": 6465, "slots": 16223, "ok": 1732, "failed": 268, "mainnet_match": 1551, "apphash": "ebf11c26668591da", "lthash": "e314c674cbe3bd74", "digest": "06035a63dde5a613"},
    26038213: {"txs": 2000, "blobs_dropped": 15, "accts": 3194, "slots": 14075, "ok": 1824, "failed": 176, "mainnet_match": 1754, "apphash": "3a00ba90b52588b0", "lthash": "4228b492b89ef970", "digest": "cb6ac1c77f01f128"},
    26039088: {"txs": 2000, "blobs_dropped": 14, "accts": 3413, "slots": 15461, "ok": 1675, "failed": 325, "mainnet_match": 1566, "apphash": "2d73b6817e35a094", "lthash": "8821d22f42f6620e", "digest": "1911803667dfefec"},
    26039963: {"txs": 2000, "blobs_dropped": 16, "accts": 3375, "slots": 10018, "ok": 1869, "failed": 131, "mainnet_match": 1779, "apphash": "e4b28953884f36f6", "lthash": "9129ceebf9b2c645", "digest": "74e5b65b6eac3a2b"},
    26040838: {"txs": 2000, "blobs_dropped": 19, "accts": 3269, "slots": 8748, "ok": 1863, "failed": 137, "mainnet_match": 1821, "apphash": "ff14f41573db5cee", "lthash": "40e112ff8b227ac9", "digest": "de3f9cadfb9361e3"},
    26041713: {"txs": 2000, "blobs_dropped": 16, "accts": 3410, "slots": 10527, "ok": 1865, "failed": 135, "mainnet_match": 1810, "apphash": "eae4a6e1f785261f", "lthash": "51b2539d30ccb98a", "digest": "6437c53fa21a9d4d"},
    26042588: {"txs": 2000, "blobs_dropped": 15, "accts": 3585, "slots": 11819, "ok": 1848, "failed": 152, "mainnet_match": 1746, "apphash": "8391d0d8ff34e84e", "lthash": "b2bbb61948d3f04d", "digest": "3c254706633a34d8"},
    26043463: {"txs": 2000, "blobs_dropped": 13, "accts": 4016, "slots": 12780, "ok": 1854, "failed": 146, "mainnet_match": 1755, "apphash": "331ef31fe45ecc2a", "lthash": "37d7fc8bb7b0c0b7", "digest": "b987b327e91e8815"},
}

# mainnet_match counts transactions whose status and gasUsed equal Sei's receipt; Sei's gas schedule differs from the
# harness's chain config, so most mismatches are gas-only. The harness's block hash is derived from the window's name,
# which carries its start, and some of window B's transactions read it into state.
EXPECTED_SEI = {
    0: {"txs": 574, "blobs_dropped": 0, "accts": 235, "slots": 778, "ok": 457, "failed": 117, "mainnet_match": 307, "apphash": "99548c947d352ec7", "lthash": "9a748e658aa52029", "digest": "68cdc44ece02ce2b"},
    100000: {"txs": 1131, "blobs_dropped": 0, "accts": 524, "slots": 2204, "ok": 978, "failed": 153, "mainnet_match": 410, "apphash": "4625e83c19134051", "lthash": "55221f79a3cc31ee", "digest": "be418698a20fc857"},
}

# accts and slots count the Sei transactions' start state (mix0 loads one transaction for the window's timestamp and
# start state, then drops it); mainnet_match counts Sei transactions only.
EXPECTED_MIX = {
    "mix0": {MIX_START: {"txs": 2000, "blobs_dropped": 0, "accts": 3, "slots": 0, "ok": 2000, "failed": 0, "mainnet_match": 0, "apphash": "fca9975a5f9df497", "lthash": "ced6429541d687eb", "digest": "a2669470e627257f"}},
    "mix125": {MIX_START: {"txs": 2000, "blobs_dropped": 0, "accts": 199, "slots": 694, "ok": 1993, "failed": 7, "mainnet_match": 74, "apphash": "2ed28a8635005310", "lthash": "e84293b4980b21fb", "digest": "f2b16c828f5776e5"}},
    "mix250": {MIX_START: {"txs": 2000, "blobs_dropped": 0, "accts": 311, "slots": 1061, "ok": 1983, "failed": 17, "mainnet_match": 129, "apphash": "1ed64aee88b3583a", "lthash": "06115507a02303cf", "digest": "c6e98077b5e29c80"}},
}

GATES = ("run_integrity", "state_gate", "outcome_gate")
WITNESS_TOLERANCE = 0.01
FIGURES = {
    "txps": {"unit": "tx/s", "better": "higher", "effect": "relative"},
    "worst_window_ms": {"unit": "ms", "better": "lower", "effect": "relative"},
    "cpu_us_per_tx": {"unit": "us/tx", "better": "lower", "effect": "relative"},
    "peak_rss_mb": {"unit": "MB", "better": "lower", "effect": "absolute"},
}


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def procs():
    raw = os.environ.get("BENCH_PROCS", "")
    return int(raw) if raw.strip() else DEFAULT_PROCS


# ------------------------------------------------------------------------------------------------
# data: the fetched archives -> a verified cache of <block>.json.gz files the harness reads

def datasets(data):
    return {
        "eth": {"archive": os.path.join(data, ARCHIVE_NAME), "manifest": os.path.join(data, MANIFEST_NAME),
                "archive_sha256": ARCHIVE_SHA256, "manifest_sha256": MANIFEST_SHA256, "starts": STARTS,
                "cache": os.path.join(data, "replay-eth-" + ARCHIVE_SHA256[:12])},
        "sei": {"archive": os.path.join(data, SEI_ARCHIVE_NAME), "manifest": os.path.join(data, SEI_MANIFEST_NAME),
                "archive_sha256": SEI_ARCHIVE_SHA256, "manifest_sha256": SEI_MANIFEST_SHA256, "starts": SEI_STARTS,
                "cache": os.path.join(data, "replay-sei-" + SEI_ARCHIVE_SHA256[:12])},
    }


def load_manifest(spec):
    with open(spec["manifest"], "rb") as f:
        data = f.read()
    if sha256_bytes(data) != spec["manifest_sha256"]:
        raise RuntimeError(f"manifest {spec['manifest']} sha256 {sha256_bytes(data)} is not the pinned {spec['manifest_sha256']}")
    man = json.loads(data)
    if man.get("archive_sha256") != spec["archive_sha256"] or [w["start"] for w in man["windows"]] != list(spec["starts"]):
        raise RuntimeError(f"manifest {spec['manifest']} does not name the pinned archive and the pinned windows")
    return man


def accepted_tx_hashes(block_files, txs=TXS):
    """The ordered hashes of the transactions the harness accepts from these block files (blob txs and every later tx
    of a blob sender are dropped), up to txs."""
    hashes, dropped, blobs = [], set(), 0
    for data in block_files:
        bf = json.loads(data)
        for t in bf["block"]["transactions"]:
            if len(hashes) >= txs:
                break
            frm = t["from"].lower()
            if int(t["type"], 16) == 3 or frm in dropped:
                dropped.add(frm)
                blobs += 1
                continue
            hashes.append(t["hash"].lower())
    return hashes, blobs


def verify_data(directory, deep, spec):
    """Checks every pinned block file in `directory` against the dataset's manifest. Returns a list of problems (empty:
    verified). `deep` also re-derives each window's block numbers and ordered accepted transaction hashes."""
    import gzip
    man = load_manifest(spec)
    problems = []
    for w in man["windows"]:
        want_txs = w.get("txs", TXS)
        contents = []
        for entry in w["files"]:
            path = os.path.join(directory, f"{entry['block']}.json.gz")
            try:
                with gzip.open(path, "rb") as f:
                    data = f.read()
            except OSError as exc:
                problems.append(f"window {w['start']}: block {entry['block']} unreadable: {exc}")
                continue
            if sha256_bytes(data) != entry["sha256"]:
                problems.append(f"window {w['start']}: block {entry['block']} sha256 {sha256_bytes(data)[:16]} is not the pinned {entry['sha256'][:16]}")
            if deep:
                contents.append(data)
                number = int(json.loads(data)["block"]["number"], 16)
                if number != entry.get("native_block", entry["block"]):
                    problems.append(f"window {w['start']}: file {entry['block']} holds block {number}")
        if deep and len(contents) == len(w["files"]):
            hashes, blobs = accepted_tx_hashes(contents, want_txs)
            if len(hashes) != want_txs or sha256_bytes("\n".join(hashes).encode()) != w["tx_hashes_sha256"] or blobs != w["blobs_dropped"]:
                problems.append(f"window {w['start']}: accepted tx hashes/blobs do not match the manifest ({len(hashes)} txs, {blobs} dropped)")
        if w["blocks"] != [e["block"] for e in w["files"]] or w["blocks"][0] != w["start"] or w["blocks"] != list(range(w["start"], w["start"] + len(w["blocks"]))):
            problems.append(f"window {w['start']}: manifest block list is not the consecutive run from its start")
    return problems


def prepare_dataset(name, spec):
    import gzip
    import lzma
    man = load_manifest(spec)
    target = spec["cache"]
    if os.path.isdir(target):
        problems = verify_data(target, deep=True, spec=spec)
        if not problems:
            print(f"DATA {name} cached verified windows={len(man['windows'])} blocks={sum(len(w['files']) for w in man['windows'])}")
            return 0
        print(f"DATA {name} cached copy fails verification; re-extracting: " + "; ".join(problems[:5]))
    if sha256_file(spec["archive"]) != spec["archive_sha256"]:
        print(f"REFUSED: archive {spec['archive']} is not the pinned {spec['archive_sha256']}", file=sys.stderr)
        return 3
    parent = os.path.dirname(target.rstrip("/")) or "."
    os.makedirs(parent, exist_ok=True)
    stage = tempfile.mkdtemp(prefix=".replay-extract-", dir=parent)
    try:
        wanted = {e["file"]: e for w in man["windows"] for e in w["files"]}
        with open(spec["archive"], "rb") as f:
            raw = lzma.decompress(f.read())
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as tf:
            for entry_info in tf.getmembers():
                entry = wanted.get(entry_info.name)
                if entry is None or not entry_info.isfile():
                    raise RuntimeError(f"archive entry {entry_info.name!r} is not a manifest file")
                data = tf.extractfile(entry_info).read()
                if sha256_bytes(data) != entry["sha256"]:
                    raise RuntimeError(f"archive entry {entry_info.name} does not match the manifest")
                with gzip.GzipFile(os.path.join(stage, f"{entry['block']}.json.gz"), "wb", compresslevel=1, mtime=0) as g:
                    g.write(data)
        problems = verify_data(stage, deep=True, spec=spec)
        if problems:
            raise RuntimeError("; ".join(problems[:5]))
        if os.path.isdir(target):
            shutil.rmtree(target)
        os.rename(stage, target)
    except BaseException as exc:
        shutil.rmtree(stage, ignore_errors=True)
        print(f"REFUSED: {name} data extraction failed: {exc}", file=sys.stderr)
        return 3
    print(f"DATA {name} extracted verified windows={len(man['windows'])} blocks={sum(len(w['files']) for w in man['windows'])}")
    return 0


# ------------------------------------------------------------------------------------------------
# setup: copy the harness into the tree and build the test binary

def normalized(text):
    return "\n".join(" ".join(line.split()) for line in text.strip().splitlines() if line.strip() and not line.strip().startswith("//"))


def new_executor_body(app_go):
    m = re.search(r"func \(a \*evmOnlyApplication\) newExecutor\(\) \*evmonly\.Executor \{\n(.*?)\n\}\n", app_go, re.S)
    return m.group(1) if m else None


def tree_has_sidecar(tree):
    """True when the tree's FinalizeBlock request carries a sender-key sidecar and the executor exports
    RecoverSenderKey (the carried sender-key change); the harness then attaches the producer's keys."""
    def grep(rel, pattern):
        root = os.path.join(tree, rel)
        for dirpath, _, files in os.walk(root):
            for name in files:
                if name.endswith(".go") and not name.endswith("_test.go"):
                    with open(os.path.join(dirpath, name), errors="replace") as f:
                        if re.search(pattern, f.read()):
                            return True
        return False
    return grep(os.path.join("sei-tendermint", "abci", "types"), r"\n\s+SenderKeys\s+\[\]\[\]byte") and \
        grep(os.path.join("giga", "evmonly"), r"\nfunc RecoverSenderKey\(")


def tree_goexperiment(tree):
    mk = os.path.join(tempfile.mkdtemp(prefix="bench-goexp-"), "goexp.mk")
    with open(mk, "w") as f:
        f.write('bench-print-goexperiment:\n\t@echo "$(GOEXPERIMENT)"\n')
    env = {k: v for k, v in os.environ.items() if k != "GOEXPERIMENT"}
    out = subprocess.run(["make", "--no-print-directory", "-s", "-f", "Makefile", "-f", mk, "bench-print-goexperiment"],
                         cwd=tree, env=env, capture_output=True, text=True, check=True).stdout.strip()
    shutil.rmtree(os.path.dirname(mk), ignore_errors=True)
    return out


def setup(tree, out, data, workload):
    os.makedirs(out, exist_ok=True)
    name = "eth" if workload == "eth8" else "sei"
    rc = prepare_dataset(name, datasets(data)[name])
    if rc:
        return rc
    with open(os.path.join(tree, APP_GO)) as f:
        body = new_executor_body(f.read())
    if body is None or normalized(body) != normalized(NEW_EXECUTOR_BODY):
        print("REFUSED: this tree's evmOnlyApplication.newExecutor differs from the harness's copy "
              "(bench/replay/harness/bench_replay_test.go nrInstallUncheckedExecutor and NEW_EXECUTOR_BODY in this "
              "file); update both to match the tree", file=sys.stderr)
        return 3
    sidecar = tree_has_sidecar(tree)
    adapter = "bench_replay_keys_sidecar_test.go" if sidecar else "bench_replay_keys_none_test.go"
    print(f"sender_keys={'sidecar' if sidecar else 'recovered'} adapter={adapter}")
    pkg = os.path.join(tree, PKG)
    shutil.copyfile(os.path.join(HARNESS_DIR, "bench_replay_test.go"), os.path.join(pkg, "bench_replay_test.go"))
    shutil.copyfile(os.path.join(HARNESS_DIR, adapter), os.path.join(pkg, "bench_replay_keys_test.go"))
    goexp = tree_goexperiment(tree)
    print(f"goexperiment={goexp or '<none>'}")
    env = {k: v for k, v in os.environ.items() if k != "GOEXPERIMENT"}
    if goexp:
        env["GOEXPERIMENT"] = goexp
    # Built with the PGO profile the product's main package ships, as `go build ./cmd/seid` (-pgo=auto) would build
    # seid; go test -c does not look there itself. A tree without cmd/seid/default.pgo builds with -pgo=off.
    pgo = os.path.join(tree, "cmd", "seid", "default.pgo")
    pgo = pgo if os.path.isfile(pgo) else "off"
    print(f"pgo={pgo}")
    binary = os.path.join(out, BIN_NAME)
    subprocess.run(["go", "test", "-c", "-tags", "benchharness", f"-pgo={pgo}", "-o", binary, "./" + PKG + "/"],
                   cwd=tree, env=env, check=True)
    print(f"built {binary}")
    return 0


# ------------------------------------------------------------------------------------------------
# parsing the harness output

NR_RE = re.compile(r"^NR (eth-\d+) (.*)$")
MARK_RE = re.compile(r"^NRMARK (begin|end) (eth-\d+) rep=(\d+)$")
INT_KEYS = ("rep", "procs", "txs", "blobs_dropped", "accts", "slots", "ok", "failed", "mainnet_match", "gas", "receipts", "receipts_ok")
FLOAT_KEYS = ("prepare_ms", "finalize_ms", "await_commit_ms", "cpu_us_per_tx_finalize", "cpu_us_per_tx_block", "heap_inuse_mb", "peak_rss_mb")


def parse_output(lines):
    """lines: [(monotonic stamp, text)]. Returns {"rows": {start: [kv per repeat]}, "marks": {(start, rep): {begin, end}},
    "pass": bool, "failures": [text]}."""
    rows, marks, failures, passed = {}, {}, [], False
    for stamp, text in lines:
        m = NR_RE.match(text)
        if m:
            start = int(m.group(1)[4:])
            kv = dict(x.split("=", 1) for x in m.group(2).split() if "=" in x)
            try:
                rec = {k: int(kv[k]) for k in INT_KEYS}
                rec.update({k: float(kv[k]) for k in FLOAT_KEYS})
                rec.update({"apphash": kv["apphash"][:16], "lthash": kv["lthash"][:16], "digest": kv["digest"]})
            except (KeyError, ValueError):
                failures.append("unparsable NR line: " + text[:200])
                continue
            rows.setdefault(start, []).append(rec)
            continue
        m = MARK_RE.match(text)
        if m:
            key = (int(m.group(2)[4:]), int(m.group(3)))
            slot = marks.setdefault(key, {})
            if m.group(1) in slot:
                failures.append("repeated mark: " + text)
            slot[m.group(1)] = stamp
            continue
        if text.strip() == "PASS":
            passed = True
        elif "FAIL" in text or "panic" in text:
            failures.append(text[:200])
    return {"rows": rows, "marks": marks, "pass": passed, "failures": failures}


def evaluate(parsed, rc, want_procs, expected=EXPECTED, reps=REPS):
    """Figures, gates, a second count of the figures from this wrapper's own clock, and per-window readings."""
    rows, marks = parsed["rows"], parsed["marks"]
    integrity = rc == 0 and parsed["pass"] and not parsed["failures"] and sorted(rows) == sorted(expected)
    state = integrity
    outcome = integrity
    per_window, fin_medians, ext_medians = {}, [], []
    total_txs = 0
    for start, exp in expected.items():
        reps_seen = rows.get(start, [])
        ok_reps = [r["rep"] for r in reps_seen] == list(range(reps))
        shape = ok_reps and all(
            r["procs"] == want_procs and r["txs"] == exp["txs"] and r["blobs_dropped"] == exp["blobs_dropped"]
            and r["accts"] == exp["accts"] and r["slots"] == exp["slots"] for r in reps_seen)
        ext = []
        for rep in range(reps):
            mk = marks.get((start, rep), {})
            if "begin" in mk and "end" in mk and mk["end"] >= mk["begin"]:
                ext.append((mk["end"] - mk["begin"]) * 1000.0)
        shape = shape and len(ext) == reps
        integrity = integrity and shape
        state = state and bool(reps_seen) and all(
            r["apphash"] == exp["apphash"] and r["lthash"] == exp["lthash"] and r["digest"] == exp["digest"] for r in reps_seen)
        outcome = outcome and bool(reps_seen) and all(
            r["ok"] == exp["ok"] and r["failed"] == exp["failed"] and r["mainnet_match"] == exp["mainnet_match"]
            and r["receipts"] == exp["txs"] and r["receipts_ok"] == exp["ok"] for r in reps_seen)
        fins = [r["prepare_ms"] + r["finalize_ms"] for r in reps_seen]
        total_txs += sum(r["txs"] for r in reps_seen[:1])
        fm = statistics.median(fins) if fins else None
        em = statistics.median(ext) if ext else None
        fin_medians.append(fm)
        ext_medians.append(em)
        per_window[str(start)] = {
            "apply_ms": fins, "apply_ms_median": fm, "finalize_ms": [r["finalize_ms"] for r in reps_seen], "external_ms": [round(x, 3) for x in ext], "external_ms_median": em,
            "prepare_ms": [r["prepare_ms"] for r in reps_seen], "await_commit_ms": [r["await_commit_ms"] for r in reps_seen],
            "cpu_us_per_tx_finalize": [r["cpu_us_per_tx_finalize"] for r in reps_seen],
            "cpu_us_per_tx_block": [r["cpu_us_per_tx_block"] for r in reps_seen],
            "heap_inuse_mb": [r["heap_inuse_mb"] for r in reps_seen], "peak_rss_mb": [r["peak_rss_mb"] for r in reps_seen],
            "apphash": sorted({r["apphash"] for r in reps_seen}), "lthash": sorted({r["lthash"] for r in reps_seen}),
            "ok": sorted({r["ok"] for r in reps_seen}), "failed": sorted({r["failed"] for r in reps_seen}),
            "mainnet_match": sorted({r["mainnet_match"] for r in reps_seen}),
        }
    figures, witness = {"txps": None, "worst_window_ms": None}, {}
    if integrity and all(f for f in fin_medians):
        figures["txps"] = total_txs / (sum(fin_medians) / 1000.0)
        figures["worst_window_ms"] = max(fin_medians)
    if integrity and all(e for e in ext_medians):
        witness["txps"] = sum(exp["txs"] for exp in expected.values()) / (sum(ext_medians) / 1000.0)
        witness["worst_window_ms"] = max(ext_medians)
    gates = {"run_integrity": bool(integrity), "state_gate": bool(state), "outcome_gate": bool(outcome)}
    return figures, gates, witness, per_window


def observations(per_window):
    """Whole-run CPU and memory readings: per-window medians averaged or taken at their largest."""
    def med(key):
        return [statistics.median(w[key]) for w in per_window.values() if w[key]]
    cpu = med("cpu_us_per_tx_block")
    fin = med("cpu_us_per_tx_finalize")
    rss = [max(w["peak_rss_mb"]) for w in per_window.values() if w["peak_rss_mb"]]
    heap = [max(w["heap_inuse_mb"]) for w in per_window.values() if w["heap_inuse_mb"]]
    return {
        "cpu_us_per_tx_block": round(sum(cpu) / len(cpu), 1) if cpu else None,
        "cpu_us_per_tx_finalize": round(sum(fin) / len(fin), 1) if fin else None,
        "peak_rss_mb": max(rss) if rss else None,
        "heap_inuse_mb_max": max(heap) if heap else None,
        "prepare_ms_sum": round(sum(statistics.median(w["prepare_ms"]) for w in per_window.values() if w["prepare_ms"]), 3),
        "await_commit_ms_sum": round(sum(statistics.median(w["await_commit_ms"]) for w in per_window.values() if w["await_commit_ms"]), 3),
    }


# ------------------------------------------------------------------------------------------------
# one run

def workload_config(workload, data):
    """The data set, windows, per-window transaction counts, chain id and pins a workload runs."""
    ds = datasets(data)
    if workload == "sei2":
        return {"dir": ds["sei"]["cache"], "spec": ds["sei"], "starts": SEI_STARTS, "txs": [EXPECTED_SEI[s]["txs"] for s in SEI_STARTS],
                "chain_id": SEI_CHAIN_ID, "expected": EXPECTED_SEI}
    if workload in MIX_S:
        return {"dir": ds["sei"]["cache"], "spec": ds["sei"], "starts": (MIX_START,), "txs": [MIX_S[workload]], "chain_id": SEI_CHAIN_ID,
                "expected": EXPECTED_MIX[workload], "mix": MIX_N}
    return {"dir": ds["eth"]["cache"], "spec": ds["eth"], "starts": STARTS, "txs": [TXS] * len(STARTS), "chain_id": 1, "expected": EXPECTED}


def child_env(run_dir, n_procs, extra):
    env = {k: v for k, v in os.environ.items()
           if not (k.startswith("NR_") or k in ("GOGC", "GOMEMLIMIT", "GODEBUG", "GOMAXPROCS"))}
    env.update({"GOMAXPROCS": str(n_procs), "TMPDIR": run_dir})
    env.update(extra)
    return env


def harness_env(cfg, run_dir):
    env = {"NR_DIR": cfg["dir"], "NR_SCRATCH": run_dir, "NR_STARTS": ",".join(str(s) for s in cfg["starts"]),
           "NR_REPS": str(REPS), "NR_TXS": ",".join(str(t) for t in cfg["txs"]), "NR_CHAINID": str(cfg["chain_id"]),
           "NR_SENDERS": "foreign"}
    if cfg.get("mix"):
        env["NR_MIX_N"] = str(cfg["mix"])
    return env


def run_harness(binary, tree, run_dir, n_procs, cfg):
    """Runs the test binary once; returns (rc, [(stamp, line)], seconds)."""
    env = child_env(run_dir, n_procs, harness_env(cfg, run_dir))
    cmd = [binary, "-test.run", "^TestNodeRealWindows$", "-test.count=1", f"-test.timeout={PROCESS_TIMEOUT_SECONDS}s"]
    started = time.monotonic()
    proc = subprocess.Popen(cmd, cwd=os.path.join(tree, PKG), env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, start_new_session=True)
    killer = threading.Timer(PROCESS_TIMEOUT_SECONDS + 30, lambda: os.killpg(proc.pid, signal.SIGKILL))
    killer.start()
    lines, pending = [], b""
    fd = proc.stdout.fileno()
    try:
        while True:
            chunk = os.read(fd, 1 << 16)
            stamp = time.monotonic()
            if not chunk:
                break
            pending += chunk
            *complete, pending = pending.split(b"\n")
            lines.extend((stamp, c.decode("utf-8", "replace")) for c in complete)
        if pending:
            lines.append((time.monotonic(), pending.decode("utf-8", "replace")))
        rc = proc.wait()
    finally:
        killer.cancel()
    return rc, lines, time.monotonic() - started


def measure(tree, out, data, workload):
    n_procs = procs()
    cfg = workload_config(workload, data)
    binary = os.path.join(out, BIN_NAME)
    result = {"workload": workload, "procs": n_procs, "reps": REPS, "figure_spec": FIGURES, "primary": "txps"}
    problems = []
    if not os.access(binary, os.X_OK):
        problems.append(f"no test binary at {binary}: setup did not build it")
    else:
        problems.extend(verify_data(cfg["dir"], deep=False, spec=cfg["spec"]))
    if problems:
        result.update({"figures": {k: None for k in FIGURES}, "gates": {g: False for g in GATES}, "problems": problems[:10]})
        print("RESULT " + json.dumps(result, sort_keys=True))
        return 0
    run_dir = tempfile.mkdtemp(prefix=RUN_PREFIX)
    try:
        rc, lines, seconds = run_harness(binary, tree, run_dir, n_procs, cfg)
        parsed = parse_output(lines)
        figures, gates, witness, per_window = evaluate(parsed, rc, n_procs, expected=cfg["expected"])
        for _, text in lines:
            if text.startswith("NR ") or "FAIL" in text or "panic" in text:
                print(text[:520])
        # a figure whose second count on this wrapper's clock differs by more than 1% is left out
        disputes = common.apply_witness(figures, witness, WITNESS_TOLERANCE, ("txps", "worst_window_ms"))
        obs = observations(per_window)
        figures["cpu_us_per_tx"] = obs["cpu_us_per_tx_block"]
        figures["peak_rss_mb"] = obs["peak_rss_mb"]
        # every window's app hash, LtHash and outcome digest, which base and head must share
        digests = {w: {"apphash": o["apphash"], "lthash": o["lthash"]} for w, o in per_window.items()}
        result.update({"figures": figures, "gates": gates, "witness": witness, "observations": obs, "windows": per_window,
                       "digests": digests, "witness_disputes": disputes, "harness_rc": rc, "harness_seconds": round(seconds, 3),
                       "failures": parsed["failures"][:10], "loadavg_before": list(os.getloadavg())})
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)
    print("RESULT " + json.dumps(result, sort_keys=True))
    return 0


# ------------------------------------------------------------------------------------------------
# self-test: the parser and gates on synthetic output, each gate failing on a planted change

def self_test():
    def good_lines(p=DEFAULT_PROCS, mutate=None, expected=EXPECTED):
        out, t = [], 100.0
        for start, exp in expected.items():
            for rep in range(REPS):
                rec = {"rep": rep, "procs": p, "txs": exp["txs"], "blobs_dropped": exp["blobs_dropped"], "accts": exp["accts"],
                       "slots": exp["slots"], "prepare_ms": 2.0, "finalize_ms": 100.0 + rep, "await_commit_ms": 3.0,
                       "cpu_us_per_tx_finalize": 300.0, "cpu_us_per_tx_block": 310.0, "ok": exp["ok"], "failed": exp["failed"],
                       "mainnet_match": exp["mainnet_match"], "gas": 1, "receipts": exp["txs"], "receipts_ok": exp["ok"],
                       "heap_inuse_mb": 200.0, "peak_rss_mb": 500.0, "apphash": exp["apphash"] + "00" * 24,
                       "lthash": exp["lthash"] + "11" * 24, "digest": exp["digest"]}
                if mutate:
                    mutate(start, rep, rec)
                out.append((t, f"NRMARK begin eth-{start} rep={rep}"))
                t += (rec["prepare_ms"] + rec["finalize_ms"]) / 1000.0
                out.append((t, f"NRMARK end eth-{start} rep={rep}"))
                out.append((t, f"NR eth-{start} " + " ".join(f"{k}={v}" for k, v in rec.items())))
        out.append((t, "PASS"))
        return out

    figures, gates, witness, per_window = evaluate(parse_output(good_lines()), 0, DEFAULT_PROCS)
    assert all(gates.values()), gates
    assert abs(figures["txps"] - TXS * 8 / 0.824) < 1e-6, figures
    assert abs(witness["txps"] - figures["txps"]) / figures["txps"] < 1e-9, witness
    assert figures["worst_window_ms"] == 103.0 and abs(witness["worst_window_ms"] - 103.0) < 1e-6
    obs = observations(per_window)
    assert obs["cpu_us_per_tx_block"] == 310.0 and obs["peak_rss_mb"] == 500.0, obs

    def planted(mutate, lines=None, rc=0, want=DEFAULT_PROCS):
        return evaluate(parse_output(lines if lines is not None else good_lines(mutate=mutate)), rc, want)[1]

    first = STARTS[0]
    g = planted(lambda s, r, rec: rec.update(apphash="0" * 64) if (s, r) == (first, 2) else None)
    assert g == {"run_integrity": True, "state_gate": False, "outcome_gate": True}, g
    g = planted(lambda s, r, rec: rec.update(lthash="0" * 64) if s == STARTS[4] else None)
    assert g == {"run_integrity": True, "state_gate": False, "outcome_gate": True}, g
    g = planted(lambda s, r, rec: rec.update(digest="0" * 16) if s == STARTS[6] else None)
    assert not g["state_gate"], g
    g = planted(lambda s, r, rec: rec.update(ok=rec["ok"] - 1) if s == STARTS[-1] else None)
    assert g == {"run_integrity": True, "state_gate": True, "outcome_gate": False}, g
    g = planted(lambda s, r, rec: rec.update(mainnet_match=rec["mainnet_match"] + 1) if s == STARTS[3] else None)
    assert not g["outcome_gate"], g
    g = planted(lambda s, r, rec: rec.update(receipts=TXS - 1) if s == STARTS[2] else None)
    assert not g["outcome_gate"], g
    g = planted(lambda s, r, rec: rec.update(blobs_dropped=0) if s == STARTS[1] else None)
    assert not g["run_integrity"], g
    g = planted(None, rc=1)
    assert not any(g.values()), g
    g = planted(None, want=8)
    assert not g["run_integrity"], g
    lines = [l for l in good_lines() if not l[1].startswith(f"NR eth-{STARTS[5]} ")]
    assert not planted(None, lines=lines)["run_integrity"]
    lines = [l for l in good_lines() if l[1] != f"NRMARK end eth-{STARTS[2]} rep=1"]
    assert not planted(None, lines=lines)["run_integrity"]
    lines = [l for l in good_lines() if l[1] != "PASS"]
    assert not planted(None, lines=lines)["run_integrity"]
    lines = good_lines()[:-1] + [(0.0, "--- FAIL: TestNodeRealWindows"), (0.0, "PASS")]
    assert not planted(None, lines=lines)["run_integrity"]

    sei = EXPECTED_SEI
    figures, gates, witness, _ = evaluate(parse_output(good_lines(expected=sei)), 0, DEFAULT_PROCS, expected=sei)
    assert all(gates.values()), gates
    assert abs(figures["txps"] - (574 + 1131) / 0.206) < 1e-6 and abs(witness["txps"] - figures["txps"]) / figures["txps"] < 1e-9
    g = evaluate(parse_output(good_lines(expected=sei, mutate=lambda s, r, rec: rec.update(apphash="0" * 64) if s == 100000 else None)), 0, DEFAULT_PROCS, expected=sei)[1]
    assert g == {"run_integrity": True, "state_gate": False, "outcome_gate": True}, g
    g = evaluate(parse_output(good_lines(expected=sei, mutate=lambda s, r, rec: rec.update(txs=573) if s == 0 else None)), 0, DEFAULT_PROCS, expected=sei)[1]
    assert not g["run_integrity"], g
    g = evaluate(parse_output(good_lines(expected=sei, mutate=lambda s, r, rec: rec.update(receipts_ok=rec["ok"] - 1) if s == 0 else None)), 0, DEFAULT_PROCS, expected=sei)[1]
    assert not g["outcome_gate"], g
    assert not all(evaluate(parse_output(good_lines()), 0, DEFAULT_PROCS, expected=sei)[1].values())
    for m in MIX_S:
        mx = EXPECTED_MIX[m]
        figures, gates, _, _ = evaluate(parse_output(good_lines(expected=mx)), 0, DEFAULT_PROCS, expected=mx)
        assert all(gates.values()) and abs(figures["txps"] - 2000 / 0.103) < 1e-6, (m, gates, figures)
        g = evaluate(parse_output(good_lines(expected=mx, mutate=lambda s, r, rec: rec.update(lthash="0" * 64) if r == 1 else None)), 0, DEFAULT_PROCS, expected=mx)[1]
        assert g == {"run_integrity": True, "state_gate": False, "outcome_gate": True}, (m, g)
        g = evaluate(parse_output(good_lines(expected=mx, mutate=lambda s, r, rec: rec.update(ok=rec["ok"] - 1) if r == 2 else None)), 0, DEFAULT_PROCS, expected=mx)[1]
        assert g == {"run_integrity": True, "state_gate": True, "outcome_gate": False}, (m, g)
    assert not all(evaluate(parse_output(good_lines(expected=EXPECTED_MIX["mix125"])), 0, DEFAULT_PROCS, expected=EXPECTED_MIX["mix250"])[1].values())
    cfg = workload_config("mix250", "/x")
    assert harness_env(cfg, "/r")["NR_MIX_N"] == "2000" and harness_env(cfg, "/r")["NR_TXS"] == "250"
    assert "NR_MIX_N" not in harness_env(workload_config("sei2", "/x"), "/r")
    assert harness_env(workload_config("sei2", "/x"), "/r")["NR_TXS"] == "574,1131"
    assert workload_config("sei2", "/x")["chain_id"] == SEI_CHAIN_ID and workload_config("eth8", "/x")["chain_id"] == 1
    body = "func (a *evmOnlyApplication) newExecutor() *evmonly.Executor {\n\treturn evmonly.NewExecutor(evmonly.Config{\n" \
           "\t\tChainConfig:  a.chainConfig,\n\t\tMinGasPrice:  big.NewInt(evmOnlyMinGasPrice),\n\t\tOCCWorkers:   runtime.GOMAXPROCS(0),\n" \
           "\t\tParseWorkers: runtime.GOMAXPROCS(0),\n\t\t// a comment\n\t\tRejectUnappliableTxs: true,\n\t\tBlockResultPoolSize:  1,\n\t},\n" \
           "\t\tevmonly.WithStorageManager(a.storage, a.changeSetEncoder),\n\t\tevmonly.WithMissingAccountState(evmOnlyFundedState{}),\n" \
           "\t\tevmonly.WithStoreIndependentBlockChangeSetEncoder(a.encodeCursorChangeSet),\n\t)\n}\n"
    assert normalized(new_executor_body(body)) == normalized(NEW_EXECUTOR_BODY)
    assert normalized(new_executor_body(body.replace("BlockResultPoolSize:  1", "BlockResultPoolSize:  2"))) != normalized(NEW_EXECUTOR_BODY)
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
    if a.mode in ("check", "assets", "setup", "measure") and a.workload not in WORKLOADS:
        print(f"unknown replay workload {a.workload!r}; one of {', '.join(WORKLOADS)}", file=sys.stderr)
        return 2
    if a.mode == "check":
        return 0
    if a.mode == "assets":
        print(" ".join((ARCHIVE_NAME, MANIFEST_NAME) if a.workload == "eth8" else (SEI_ARCHIVE_NAME, SEI_MANIFEST_NAME)))
        return 0
    if a.mode in ("tools", "teardown"):
        return 0
    tree, out, data = os.path.abspath(a.tree), os.path.abspath(a.out), os.path.abspath(a.data)
    if a.mode == "setup":
        return setup(tree, out, data, a.workload)
    return measure(tree, out, data, a.workload)


if __name__ == "__main__":
    sys.exit(main())
