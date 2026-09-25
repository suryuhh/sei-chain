#!/usr/bin/env python3
"""Wallet wait benchmark: send -> receipt in hand through unmodified wallet libraries' default waits, at one seid
validator serving the EVM JSON-RPC (`evmrpc`), one fresh chain per reading.

Each reading builds a one-validator chain from the product's own `seid init` defaults, as
scripts/initialize_local_chain.sh does (admin key, gentx, the validator written into genesis, mode = "validator"),
funds the benchmark's EVM keys at their unassociated (cast) addresses, moves every listen port to 36xxx (EVM HTTP
36545, WS 36546), and runs `seid start`. The client is ethers 6.17.0 / viem 2.56.9 from the pinned lockfile, run by
node.js 20.17.0 in a separate process.

Workloads:
  wallet-wait:ethers          (default) genesis timeout.commit = 350 ms (about Sei mainnet's 0.4 s blocks); ethers'
                              default wait, one send and its wait at a time, 1,000 ms (+ up to 400 ms jitter) idle
                              before each send; one warm-up wait, then 12 counted waits
  wallet-wait:viem            the same with viem's waitForTransactionReceipt
  wallet-wait:ethers-default  ethers with the product's default genesis consensus params
  wallet-wait:viem-default    viem with the product's default genesis consensus params
  wallet-wait:load            350 ms commit; an open-loop viem client sends 100 presigned transfers/s for 30 s, one per
                              key from 3,000 keys, each followed by viem's default wait

Figures: wait_ms (median counted wait on the client's monotonic clock; load: the median over all sends),
node_cpu_ms_per_wait and node_cores (seid's user+sys CPU over the timed span, per wait and over its wall),
node_peak_rss_mb (seid's VmHWM; Linux only).

Gates: run_integrity (the node served and every wait and re-read was reported), waits_gate (every wait returned a
success receipt for the hash sent), answer_gate (each waited receipt equals a plain eth_getTransactionReceipt re-read),
load_offered_gate (load: every send started within 500 ms of schedule at p99 and all were issued within 31 s; true
elsewhere), pace_gate (the mean block interval over the timed span is 300-600 ms at the 350 ms commit, 300-900 ms on
load; true on the default-params workloads).

Requirements: curl and network access once, to fetch node.js 20.17.0 (sha256-checked against nodejs.org's
SHASUMS256) and run `npm ci` on the pinned lockfile; both are cached in BENCH_CLIENT_CACHE (default
~/.cache/sei-bench/wallet-client). Linux x86_64 or macOS arm64. Ports 36xxx must be free.

Usage (compare.sh drives it):
  run.py tools   --tree BASE --out DIR                 fetch node.js and the client libraries, derive the keys
  run.py setup   --tree CHECKOUT --out DIR --tools DIR  build seid from the checkout
  run.py measure --tree CHECKOUT --out DIR --tools DIR --workload W   one reading; prints RESULT {json}
  run.py self-test
"""
import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "lib"))
from common import apply_witness  # noqa: E402
CLIENT_DIR = os.path.join(HERE, "client")
NODE_VERSION = "v20.17.0"
N_COUNTED = 12
LOAD_RATE = 100
LOAD_SECONDS = 30
GAS_PRICE_WEI = 2_000_000_000
FUND_USEI = "1000000000000"
MAINNET_COMMIT_NS = "350000000"
PACE_MS = (300.0, 600.0)
LOAD_PACE_MS = (300.0, 900.0)
LOAD_LAG_P99_MS = 500.0
CHAIN = "sei-chain"
PORTS = {"26657": "36657", "26656": "36656", "26660": "36660", "9095": "36095", "1317": "36317", "8080": "36080",
         "9090": "36090", "9091": "36091"}
EVM_HTTP, EVM_WS = 36545, 36546
# wait_ms is counted twice: on the client's monotonic clock and on its wall clock; the two must agree within 10%.
WITNESS_TOLERANCE = 0.1
WITNESSED = ("wait_ms",)
GATES = ("run_integrity", "waits_gate", "answer_gate", "load_offered_gate", "pace_gate")
# variant -> (library, paced at the 350 ms commit)
SINGLE = {
    "ethers": ("ethers", True),
    "viem": ("viem", True),
    "ethers-default": ("ethers", False),
    "viem-default": ("viem", False),
}
VARIANTS = tuple(SINGLE) + ("load",)
DEFAULT_VARIANT = "ethers"
RUN_PREFIX = "bench-wallet-"
FIGURES = {
    "wait_ms": {"unit": "ms", "better": "lower", "effect": "relative"},
    "node_cpu_ms_per_wait": {"unit": "ms", "better": "lower", "effect": "relative"},
    "node_cores": {"unit": "cores", "better": "lower", "effect": "relative"},
    "node_peak_rss_mb": {"unit": "MB", "better": "lower", "effect": "relative"},
}


def variant_of(workload):
    if workload == "wallet-wait":
        return DEFAULT_VARIANT
    if workload and workload.startswith("wallet-wait:") and workload.split(":", 1)[1] in VARIANTS:
        return workload.split(":", 1)[1]
    return None


# ---------------------------------------------------------------- pure readers (self-tested)

def bech32_polymod(values):
    gen = (0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3)
    chk = 1
    for v in values:
        b = chk >> 25
        chk = (chk & 0x1ffffff) << 5 ^ v
        for i in range(5):
            chk ^= gen[i] if ((b >> i) & 1) else 0
    return chk


def bech32_encode(hrp, data20):
    acc, bits, five = 0, 0, []
    for b in data20:
        acc = (acc << 8) | b
        bits += 8
        while bits >= 5:
            bits -= 5
            five.append((acc >> bits) & 31)
    if bits:
        five.append((acc << (5 - bits)) & 31)
    hrpx = [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]
    pm = bech32_polymod(hrpx + five + [0] * 6) ^ 1
    chars = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
    return hrp + "1" + "".join(chars[d] for d in five + [(pm >> 5 * (5 - i)) & 31 for i in range(6)])


def cast_address(evm_hex):
    """The sei address an unassociated EVM address's balance is kept at (x/evm GetSeiAddressOrDefault)."""
    return bech32_encode("sei", bytes.fromhex(evm_hex[2:] if evm_hex.startswith("0x") else evm_hex))


def fund_genesis(gen, addrs, amount):
    """Adds a base account and a usei balance for each address, and sets the supply to the balances' total."""
    auth, bank = gen["app_state"]["auth"], gen["app_state"]["bank"]
    have = {a.get("address") for a in auth["accounts"]}
    for a in addrs:
        if a not in have:
            auth["accounts"].append({"@type": "/cosmos.auth.v1beta1.BaseAccount", "address": a, "pub_key": None,
                                     "account_number": "0", "sequence": "0"})
        bank["balances"].append({"address": a, "coins": [{"denom": "usei", "amount": amount}]})
    total = {}
    for b in bank["balances"]:
        for c in b["coins"]:
            total[c["denom"]] = total.get(c["denom"], 0) + int(c["amount"])
    bank["supply"] = [{"denom": d, "amount": str(v)} for d, v in sorted(total.items())]
    return gen


def move_ports(text, evm=False):
    for old, new in PORTS.items():
        text = re.sub(r"(?<=[:\"])%s(?=\")" % old, new, text)
    if evm:
        text = re.sub(r"(?m)^http_port = \d+", "http_port = %d" % EVM_HTTP, text)
        text = re.sub(r"(?m)^ws_port = \d+", "ws_port = %d" % EVM_WS, text)
    return text


def parse_client(lines):
    waits, checks, done = {}, {}, False
    for line in lines:
        f = line.split()
        if not f:
            continue
        if f[0] == "WAIT":
            kv = dict(x.split("=", 1) for x in f[1:] if "=" in x)
            waits[int(kv["i"])] = {"ms": float(kv["wait_ms"]), "wall": float(kv["wall_ms"]), "status": kv["status"],
                                   "hash": kv["hash"], "sent": kv["sent"]}
        elif f[0] == "CHECK":
            checks[int(f[1].split("=", 1)[1])] = f[2] == "ok"
        elif f[0] == "CLIENT_DONE":
            done = True
    return waits, checks, done


def pace_ok(block_ms, paced, bounds=PACE_MS):
    if not paced:
        return True
    return block_ms is not None and bounds[0] <= block_ms <= bounds[1]


def evaluate(waits, checks, done, n_total, served, block_ms, paced):
    counted = [waits[i] for i in range(1, n_total) if i in waits]
    complete = served and done and len(waits) == n_total and len(checks) == n_total
    figures, witness = {}, {}
    if len(counted) == n_total - 1:
        figures["wait_ms"] = statistics.median(w["ms"] for w in counted)
        witness["wait_ms"] = statistics.median(w["wall"] for w in counted)
    return figures, witness, {
        "run_integrity": bool(complete),
        "waits_gate": bool(complete and all(w["status"] == "0x1" and w["hash"] == w["sent"] for w in waits.values())),
        "answer_gate": bool(complete and all(checks.values())),
        "load_offered_gate": True,
        "pace_gate": pace_ok(block_ms, paced),
    }


def kv_line(line):
    return dict(x.split("=", 1) for x in line.split()[1:] if "=" in x)


def evaluate_load(load, n, served, block_ms):
    figures, witness = {}, {}
    gates = {g: False for g in GATES}
    if not (served and load is not None):
        return figures, witness, gates
    done = int(load["done"])
    gates["run_integrity"] = int(load["n"]) == n
    gates["waits_gate"] = (done == n and int(load["status_ok"]) == n and int(load["hash_ok"]) == n
                           and int(load["errors"]) == 0)
    gates["answer_gate"] = int(load["check_n"]) >= n // 10 and int(load["check_ok"]) == int(load["check_n"])
    gates["load_offered_gate"] = (gates["waits_gate"] and float(load["lag_p99_ms"]) <= LOAD_LAG_P99_MS
                                  and float(load["send_span_s"]) <= LOAD_SECONDS + 1.0)
    gates["pace_gate"] = pace_ok(block_ms, True, LOAD_PACE_MS)
    if done == n:
        figures["wait_ms"] = float(load["wait_p50_ms"])
        witness["wait_ms"] = float(load["wall_p50_ms"])
    return figures, witness, gates


# ---------------------------------------------------------------- process readers

def proc_cpu_ms(pid):
    try:
        with open(f"/proc/{pid}/stat") as fh:
            f = fh.read().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) * 1000.0 / os.sysconf("SC_CLK_TCK")
    except OSError:
        pass
    try:  # macOS: cumulative CPU as [[dd-]hh:]mm:ss.cc
        t = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
        parts = [float(x) for x in t.replace("-", ":").split(":")]
        secs = 0.0
        for p in parts:
            secs = secs * 60 + p
        return secs * 1000.0
    except (ValueError, OSError):
        return None


def proc_hwm_mb(pid):
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        return None
    return None


def rpc(url, method, params=None, timeout=5):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}).encode()
    req = urllib.request.Request(url, body, {"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())["result"]


def seid_run(seid, cmd, home, **kw):
    return subprocess.run([seid] + cmd + ["--home", home], capture_output=True, text=True, check=True, **kw)


def build_chain(seid, home, paced, evm_addrs):
    seid_run(seid, ["init", "demo", "--chain-id", CHAIN], home)
    out = seid_run(seid, ["keys", "add", "admin", "--keyring-backend", "test", "--output", "json"], home)
    admin = json.loads(out.stdout or out.stderr)["address"]
    seid_run(seid, ["add-genesis-account", admin, "100000000000000000000usei", "--keyring-backend", "test"], home)
    gpath = os.path.join(home, "config", "genesis.json")
    gen = json.load(open(gpath))
    fund_genesis(gen, [cast_address(a) for a in evm_addrs], FUND_USEI)
    json.dump(gen, open(gpath, "w"))
    seid_run(seid, ["gentx", "admin", "7000000000000000usei", "--chain-id", CHAIN, "--keyring-backend", "test"], home)
    seid_run(seid, ["collect-gentxs"], home)
    gen = json.load(open(gpath))
    key = json.load(open(os.path.join(home, "config", "priv_validator_key.json")))["pub_key"]
    gen["validators"] = [{"power": "7000000000", "pub_key": key}]
    if paced:
        gen["consensus_params"]["timeout"]["commit"] = MAINNET_COMMIT_NS
    json.dump(gen, open(gpath, "w"))
    cpath, apath = os.path.join(home, "config", "config.toml"), os.path.join(home, "config", "app.toml")
    c = move_ports(open(cpath).read()).replace('mode = "full"', 'mode = "validator"')
    a = move_ports(open(apath).read(), evm=True)
    open(cpath, "w").write(c)
    open(apath, "w").write(a)


def start_node(seid, work, paced, evm_addrs):
    home = os.path.join(work, "home")
    build_chain(seid, home, paced, evm_addrs)
    logpath = os.path.join(work, "seid.log")
    log = open(logpath, "w")
    node = subprocess.Popen([seid, "start", "--home", home], stdout=log, stderr=subprocess.STDOUT,
                            start_new_session=True)
    url = f"http://127.0.0.1:{EVM_HTTP}"
    deadline = time.time() + 180
    chain = None
    while time.time() < deadline and node.poll() is None:
        try:
            if int(rpc(url, "eth_blockNumber"), 16) >= 3:
                chain = int(rpc(url, "eth_chainId"), 16)
                break
        except Exception:
            pass
        time.sleep(0.5)
    return node, log, logpath, url, chain


def stop_node(node, log):
    try:
        os.killpg(node.pid, signal.SIGTERM)
        node.wait(timeout=30)
    except (subprocess.TimeoutExpired, ProcessLookupError):
        try:
            os.killpg(node.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    log.close()


def height(url):
    try:
        return int(rpc(url, "eth_blockNumber"), 16)
    except Exception:
        return None


def block_ms(h0, h1, secs):
    if h0 is None or h1 is None or h1 <= h0:
        return None
    return secs * 1000.0 / (h1 - h0)


def install_js(jsdir, name):
    js = os.path.join(jsdir, name)
    with open(os.path.join(CLIENT_DIR, name)) as src, open(js + ".tmp", "w") as dst:
        dst.write(src.read())
    os.replace(js + ".tmp", js)
    return js


def cpu_figures(figures, pid, cpu0, cpu1, secs, waits):
    if cpu0 is not None and cpu1 is not None:
        figures["node_cpu_ms_per_wait"] = (cpu1 - cpu0) / waits
        figures["node_cores"] = (cpu1 - cpu0) / 1000.0 / max(secs, 1e-3)
    hwm = proc_hwm_mb(pid)
    if hwm is not None:
        figures["node_peak_rss_mb"] = hwm


# ---------------------------------------------------------------- tools: the shared client, built once

def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def client_paths(tools):
    with open(os.path.join(tools, "wallet-client.json")) as f:
        return json.load(f)


def tools(out):
    """Fetches node.js (sha256-checked) and installs the pinned client libraries into a per-user cache, so both
    sides run the identical client; derives the benchmark's EVM addresses."""
    system = f"{platform.system()}-{platform.machine()}"
    if system == "Linux-x86_64":
        pkg, ext = f"node-{NODE_VERSION}-linux-x64", "tar.xz"
    elif system == "Darwin-arm64":
        pkg, ext = f"node-{NODE_VERSION}-darwin-arm64", "tar.gz"
    else:
        print(f"unsupported machine {system}: Linux x86_64 or macOS arm64", file=sys.stderr)
        return 2
    cache = os.environ.get("BENCH_CLIENT_CACHE") or os.path.join(os.path.expanduser("~"), ".cache", "sei-bench", "wallet-client")
    os.makedirs(cache, exist_ok=True)
    nodedir = os.path.join(cache, pkg)
    node = os.path.join(nodedir, "bin", "node")
    if not os.access(node, os.X_OK):
        base = f"https://nodejs.org/dist/{NODE_VERSION}"
        stage = tempfile.mkdtemp(prefix="dl.", dir=cache)
        try:
            archive = os.path.join(stage, "node.pkg")
            subprocess.run(["curl", "-sfL", "-o", archive, f"{base}/{pkg}.{ext}"], check=True)
            sums = subprocess.run(["curl", "-sfL", f"{base}/SHASUMS256.txt"], check=True, capture_output=True, text=True).stdout
            want = next((l.split()[0] for l in sums.splitlines() if l.endswith(f" {pkg}.{ext}")), None)
            if want is None or sha256_file(archive) != want:
                print(f"node.js archive {pkg}.{ext} does not match nodejs.org's SHASUMS256", file=sys.stderr)
                return 3
            subprocess.run(["tar", "xf", archive, "-C", cache], check=True)
        finally:
            shutil.rmtree(stage, ignore_errors=True)
    lock_sha = sha256_file(os.path.join(CLIENT_DIR, "package-lock.json"))[:16]
    jsdir = os.path.join(cache, "js-" + lock_sha)
    env = dict(os.environ, PATH=os.path.join(nodedir, "bin") + os.pathsep + os.environ.get("PATH", ""))
    if not os.path.isfile(os.path.join(jsdir, ".done")):
        shutil.rmtree(jsdir, ignore_errors=True)
        os.makedirs(jsdir)
        for name in ("package.json", "package-lock.json"):
            shutil.copyfile(os.path.join(CLIENT_DIR, name), os.path.join(jsdir, name))
        subprocess.run(["npm", "ci", "--no-audit", "--no-fund", "--omit=dev"], cwd=jsdir, env=env, check=True)
        open(os.path.join(jsdir, ".done"), "w").close()
    versions = subprocess.run([node, "-e", 'const v = (p) => JSON.parse(require("fs").readFileSync("node_modules/" + p + '
                               '"/package.json")).version; console.log("ethers", v("ethers"), "viem", v("viem"))'],
                              cwd=jsdir, env=env, check=True, capture_output=True, text=True).stdout.strip()
    print(f"node={NODE_VERSION} clients: {versions}")
    keys = install_js(jsdir, "keys.mjs")
    addrs = subprocess.run([node, keys, str(1 + LOAD_RATE * LOAD_SECONDS)], cwd=jsdir, env=env, check=True,
                           capture_output=True, text=True).stdout
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "wallet-addrs.txt"), "w") as f:
        f.write(addrs)
    with open(os.path.join(out, "wallet-client.json"), "w") as f:
        json.dump({"node": node, "jsdir": jsdir, "addrs": os.path.join(out, "wallet-addrs.txt")}, f)
    print(f"keys={len(addrs.splitlines())}")
    return 0


def addresses(tools_dir, n):
    rows = [l.split() for l in open(client_paths(tools_dir)["addrs"]).read().split("\n") if l.strip()]
    return [a for _, a in rows[:n]]


# ---------------------------------------------------------------- setup and one reading

def setup(tree, out):
    os.makedirs(out, exist_ok=True)
    seid = os.path.join(out, "seid")
    if os.path.exists(seid):
        os.remove(seid)
    subprocess.run(["go", "build", "-tags", "netgo", "-o", seid, "./cmd/seid"], cwd=tree, check=True)
    print(f"built {seid}")
    print("evmrpc_receipt_hold=" + ("present" if os.path.exists(os.path.join(tree, "evmrpc", "await.go")) else "absent"))
    return 0


def measure(tree, out, tools_dir, workload):
    variant = variant_of(workload)
    seid = os.path.join(out, "seid")
    result = {"workload": workload, "figure_spec": FIGURES, "primary": "wait_ms"}
    problems = []
    if not os.access(seid, os.X_OK):
        problems.append(f"no seid at {seid}: setup did not build it")
    try:
        paths = client_paths(tools_dir)
    except OSError:
        problems.append("no client: the tools step did not run")
    if problems:
        result.update({"figures": {}, "gates": {g: False for g in GATES}, "problems": problems})
        print("RESULT " + json.dumps(result, sort_keys=True))
        return 0
    node_bin, jsdir = paths["node"], paths["jsdir"]
    work = tempfile.mkdtemp(prefix=RUN_PREFIX)
    paced = variant == "load" or SINGLE[variant][1]
    n_keys = 1 + (LOAD_RATE * LOAD_SECONDS if variant == "load" else 0)
    obs = {"variant": variant, "paced": paced}
    figures, witness, gates = {}, {}, {g: False for g in GATES}
    node = log = None
    host = {"loadavg_before": list(os.getloadavg())}
    try:
        node, log, logpath, url, chain = start_node(seid, work, paced, addresses(tools_dir, n_keys))
        served = chain is not None
        obs["chain_id"] = chain
        cenv = dict(os.environ, NODE_PATH=os.path.join(jsdir, "node_modules"), GAS_PRICE_WEI=str(GAS_PRICE_WEI))
        if served and variant == "load":
            n = LOAD_RATE * LOAD_SECONDS
            wallet = os.path.join(work, "wallet.txt")
            subprocess.run([node_bin, install_js(jsdir, "sign.mjs"), str(chain), str(GAS_PRICE_WEI), "1", str(n + 1),
                            wallet], check=True, capture_output=True, text=True, env=cenv, cwd=jsdir, timeout=180)
            js = install_js(jsdir, "load.mjs")
            h0, cpu0, t0 = height(url), proc_cpu_ms(node.pid), time.time()
            client = subprocess.Popen([node_bin, js, url, str(chain), wallet, str(LOAD_RATE)], stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True, env=cenv, cwd=jsdir)
            time.sleep(max(0.0, t0 + LOAD_SECONDS - time.time()))
            h1, cpu1, t1 = height(url), proc_cpu_ms(node.pid), time.time()
            try:
                stdout, err = client.communicate(timeout=240)
            except subprocess.TimeoutExpired:
                client.kill()
                stdout, err = client.communicate()
            load = next((kv_line(l) for l in stdout.splitlines() if l.startswith("LOAD ")), None)
            first_err = next((l for l in stdout.splitlines() if l.startswith("LOAD_FIRST_ERROR")), None)
            obs.update(client_rc=client.returncode, load=load, block_ms=block_ms(h0, h1, t1 - t0))
            if first_err:
                obs["client_first_error"] = first_err[:300]
            if client.returncode != 0:
                obs["client_stderr_tail"] = err[-600:]
            figures, witness, gates = evaluate_load(load if client.returncode == 0 else None, n, served, obs["block_ms"])
            cpu_figures(figures, node.pid, cpu0, cpu1, t1 - t0, n)
        elif served:
            lib = SINGLE[variant][0]
            n_total = N_COUNTED + 1
            key = "0x" + subprocess.run([node_bin, "-e", 'const {ethers}=require("ethers");'
                                         'process.stdout.write(ethers.id("evawait-0").slice(2))'],
                                        capture_output=True, text=True, env=cenv, cwd=jsdir, check=True).stdout
            js = install_js(jsdir, "wait.mjs")
            h0, cpu0, t0 = height(url), proc_cpu_ms(node.pid), time.time()
            client = subprocess.run([node_bin, js, url, lib, str(n_total), key, str(chain)], capture_output=True,
                                    text=True, timeout=420, env=dict(cenv, PAUSE_MS="1000"), cwd=jsdir)
            h1, cpu1, t1 = height(url), proc_cpu_ms(node.pid), time.time()
            waits, checks, done = parse_client(client.stdout.splitlines())
            obs.update(client_rc=client.returncode, client_seconds=round(t1 - t0, 1),
                       waits_ms=[round(waits[i]["ms"], 1) for i in sorted(waits)], block_ms=block_ms(h0, h1, t1 - t0))
            if client.returncode != 0:
                obs["client_stderr_tail"] = client.stderr[-600:]
            figures, witness, gates = evaluate(waits, checks, done and client.returncode == 0, n_total, served,
                                               obs["block_ms"], paced)
            cpu_figures(figures, node.pid, cpu0, cpu1, t1 - t0, n_total)
        else:
            obs["node_log_tail"] = open(logpath).read()[-1200:]
            problems.append("the node never served eth_blockNumber >= 3 within 180 s")
    except subprocess.CalledProcessError as e:
        obs["setup_error"] = (" ".join(map(str, e.cmd))[-200:], (e.stderr or "")[-600:])
        problems.append("chain setup failed: " + " ".join(map(str, e.cmd))[-200:])
    finally:
        if node is not None:
            stop_node(node, log)
        shutil.rmtree(work, ignore_errors=True)
    disputes = apply_witness(figures, witness, WITNESS_TOLERANCE, [f for f in WITNESSED if f in figures])
    result.update({"figures": figures, "gates": gates, "witness": witness, "witness_disputes": disputes,
                   "observations": obs, "host": host})
    if problems:
        result["problems"] = problems
    print("RESULT " + json.dumps(result, sort_keys=True))
    return 0


def teardown(out):
    if out:
        subprocess.run(["pkill", "-f", os.path.join(out, "seid")], check=False)
    tmp = tempfile.gettempdir()
    for d in os.listdir(tmp):
        if d.startswith(RUN_PREFIX):
            shutil.rmtree(os.path.join(tmp, d), ignore_errors=True)
    return 0


def self_test():
    # bech32 against `seid debug addr`.
    assert bech32_encode("sei", bytes(20)) == "sei1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq703fpu"
    assert cast_address("0x9d8a62f656a8d1615c1294fd71e9cfb3e4855a4f") == "sei1nk9x9ajk4rgkzhqjjn7hr6w0k0jg2kj03kexq4"
    gen = {"app_state": {"auth": {"accounts": []}, "bank": {"balances": [
        {"address": "sei1a", "coins": [{"denom": "usei", "amount": "5"}, {"denom": "uatom", "amount": "1"}]}],
        "supply": []}}}
    fund_genesis(gen, ["sei1b", "sei1c"], "10")
    assert gen["app_state"]["bank"]["supply"] == [{"denom": "uatom", "amount": "1"}, {"denom": "usei", "amount": "25"}]
    assert len(gen["app_state"]["auth"]["accounts"]) == 2
    t = 'laddr = "tcp://0.0.0.0:26657"\nx = "tcp://0.0.0.0:1317"\naddress = "0.0.0.0:9090"\nhttp_port = 8545\nws_port = 8546\ny = 126657\n'
    m = move_ports(t, evm=True)
    assert ":36657\"" in m and ":36317\"" in m and ":36090\"" in m and "http_port = 36545" in m and "ws_port = 36546" in m
    assert "y = 126657" in m
    lines = [f"WAIT i={i} wait_ms={40 + i}.0 wall_ms={40 + i} status=0x1 hash=0x{i:02x} sent=0x{i:02x} block=0x5"
             for i in range(13)] + [f"CHECK i={i} ok waited={{}} plain={{}}" for i in range(13)] + ["CLIENT_DONE n=13"]
    v, wit, g = evaluate(*parse_client(lines), 13, True, 410.0, True)
    assert all(g.values()) and v["wait_ms"] == 46.5 and wit["wait_ms"] == 46.5, (v, g)
    assert not evaluate(*parse_client(lines), 13, True, 150.0, True)[2]["pace_gate"]
    assert evaluate(*parse_client(lines), 13, True, 150.0, False)[2]["pace_gate"]
    assert not evaluate(*parse_client(lines), 13, True, None, True)[2]["pace_gate"]
    bad = [l.replace("CHECK i=3 ok", "CHECK i=3 mismatch") for l in lines]
    assert not evaluate(*parse_client(bad), 13, True, 410.0, True)[2]["answer_gate"]
    bad = [l for l in lines if not l.startswith("WAIT i=7 ")]
    v2, _, g2 = evaluate(*parse_client(bad), 13, True, 410.0, True)
    assert not g2["run_integrity"] and "wait_ms" not in v2
    bad = [l.replace("status=0x1 hash=0x04", "status=0x0 hash=0x04") for l in lines]
    assert not evaluate(*parse_client(bad), 13, True, 410.0, True)[2]["waits_gate"]
    assert not evaluate(*parse_client(lines[:-1]), 13, True, 410.0, True)[2]["run_integrity"]
    load = kv_line("LOAD n=100 done=100 status_ok=100 hash_ok=100 errors=0 check_n=10 check_ok=10 wait_p50_ms=50.0 "
                   "wall_p50_ms=50 lag_p99_ms=3.0 send_span_s=29.99")
    v, wit, g = evaluate_load(load, 100, True, 400.0)
    assert all(g.values()) and v["wait_ms"] == 50.0, (v, g)
    assert not evaluate_load(dict(load, lag_p99_ms="600"), 100, True, 400.0)[2]["load_offered_gate"]
    assert not evaluate_load(dict(load, send_span_s="31.5"), 100, True, 400.0)[2]["load_offered_gate"]
    assert evaluate_load(dict(load, lag_p99_ms="200"), 100, True, 800.0)[2]["pace_gate"]
    assert not evaluate_load(dict(load, check_ok="9"), 100, True, 400.0)[2]["answer_gate"]
    assert not evaluate_load(dict(load, status_ok="99"), 100, True, 400.0)[2]["waits_gate"]
    assert not evaluate_load(dict(load, errors="1"), 100, True, 400.0)[2]["load_offered_gate"]
    assert not evaluate_load(load, 100, True, 950.0)[2]["pace_gate"]
    assert not any(evaluate_load(None, 100, True, 400.0)[2].values())
    assert block_ms(10, 20, 4.0) == 400.0 and block_ms(10, 10, 4.0) is None
    assert variant_of("wallet-wait") == "ethers" and variant_of("wallet-wait:load") == "load"
    assert variant_of("wallet-wait:viem-default") == "viem-default" and variant_of("wallet-wait:x") is None
    assert variant_of("eth8") is None
    f = {"wait_ms": 100.0}
    assert apply_witness(f, {"wait_ms": 105.0}, WITNESS_TOLERANCE, WITNESSED) == [] and f["wait_ms"] == 100.0
    f = {"wait_ms": 100.0}
    assert apply_witness(f, {"wait_ms": 120.0}, WITNESS_TOLERANCE, WITNESSED) and f["wait_ms"] is None
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
    if a.mode in ("check", "assets", "measure") and variant_of(a.workload) is None:
        print(f"unknown workload {a.workload!r}; wallet-wait[:{'|'.join(VARIANTS)}]", file=sys.stderr)
        return 2
    if a.mode in ("check", "assets"):
        return 0
    if a.mode == "tools":
        return tools(os.path.abspath(a.out))
    if a.mode == "teardown":
        return teardown(os.path.abspath(a.out) if a.out else None)
    if a.mode == "setup":
        return setup(os.path.abspath(a.tree), os.path.abspath(a.out))
    return measure(os.path.abspath(a.tree), os.path.abspath(a.out), os.path.abspath(a.tools), a.workload)


if __name__ == "__main__":
    sys.exit(main())
