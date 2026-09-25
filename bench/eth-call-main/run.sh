#!/usr/bin/env bash
# run.sh <checkout-dir> <runs> [label]
#
# Builds the load harness (harness/loadnode_test.go) against a sei-chain checkout and runs it <runs>
# times. Each run starts one in-process Autobahn EVM-only validator (the checkout's real evmonlyapp,
# Giga executor, FlatKV store and receipt store) that executes presigned 2,000-transaction ERC-20
# blocks back to back and serves its real EVM-only JSON-RPC on 127.0.0.1:8545, and, as a separate
# process, a client that sends one eth_call and one eth_estimateGas every CALL_EVERY_US from the first
# executed block to the last.
#
# Per run it prints one RESULT line:
#   call_answered / est_answered   share of offered requests answered while blocks executed
#   node_tx_s                      node throughput over the steady span (blocks after the first 10%)
#   cpu_us_per_tx                  node process CPU over the block window / transactions executed
#   call_p50_ms call_p99_ms        latency of answered eth_call requests (est_* for eth_estimateGas)
#   call_ok=M/A                    answered eth_call whose NUMBER and BALANCE(0x0) equal the coinbase
#                                  balance the node recorded at that height (M of A answered)
#   est_ok=M/A                     answered eth_estimateGas equal to sequential re-estimates made
#                                  after the last block
#   apphash                        final app hash (identical across checkouts for the same workload)
# and appends the same fields as a TSV row to $OUT/results.tsv.
#
# The checkout is read, never written: see the build overlay below.
#
# Environment (defaults in brackets): OUT [./out]  BLOCKS [120]  TXS [2000]  CALL_EVERY_US [500]
# CLIENT_PROCS [2]  GO [go]  KEEP_STORE [0]
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
checkout="$(cd "${1:?usage: run.sh <checkout-dir> <runs> [label]}" && pwd)"
runs="${2:-1}"
label="${3:-$(basename "$checkout")}"
OUT="${OUT:-$PWD/out}"
BLOCKS="${BLOCKS:-120}"
TXS="${TXS:-2000}"
CALL_EVERY_US="${CALL_EVERY_US:-500}"
CLIENT_PROCS="${CLIENT_PROCS:-2}"
GO="${GO:-go}"
mkdir -p "$OUT/bin" "$OUT/logs"

if git -C "$checkout" rev-parse HEAD >/dev/null 2>&1; then
  rev="$(git -C "$checkout" rev-parse --short=12 HEAD)"
elif [ -f "$checkout/REVISION" ]; then
  rev="$(head -c 12 "$checkout/REVISION")"
else
  rev="unknown"
fi

# The harness is added to sei-tendermint/internal/p2p through a build overlay, so the checkout is
# never written to. The same overlay relaxes one InitChain precondition, identically for every
# checkout: the harness commits the ERC-20 genesis state at height 1 before InitChain at height 2,
# and seedInitialStateVersion refuses a store that is already at any height; with the overlay it
# accepts a store at exactly initialHeight-1. Nothing on the
# FinalizeBlock, Commit or RPC paths is changed.
work="$OUT/overlay/$label-$rev"
mkdir -p "$work"
cp "$here/harness/loadnode_test.go" "$work/loadnode_test.go"
python3 - "$checkout/sei-tendermint/internal/evmonlyapp/app.go" "$work/app.go" <<'PY'
import sys
src = open(sys.argv[1]).read()
old = "\tif latest != 0 {\n"
if src.count(old) != 1:
    sys.exit("seedInitialStateVersion precondition not found exactly once in " + sys.argv[1])
open(sys.argv[2], "w").write(src.replace(old, "\tif latest == initialHeight-1 {\n\t\treturn nil\n\t}\n" + old))
PY
python3 - "$checkout" "$work" >"$work/overlay.json" <<'PY'
import json, sys
co, work = sys.argv[1], sys.argv[2]
print(json.dumps({"Replace": {
    co + "/sei-tendermint/internal/p2p/loadnode_test.go": work + "/loadnode_test.go",
    co + "/sei-tendermint/internal/evmonlyapp/app.go": work + "/app.go",
}}))
PY
bin="$OUT/bin/loadnode-$label-$rev.test"
echo "building harness against $checkout ($rev) ..." >&2
(cd "$checkout" && "$GO" test -c -overlay "$work/overlay.json" -o "$bin" ./sei-tendermint/internal/p2p/)

for i in $(seq 1 "$runs"); do
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  store="$(mktemp -d "${TMPDIR:-/tmp}/loadnode-XXXXXX")"
  log="$OUT/logs/$stamp-$label"
  env_common=(LOADNODE_ADMIT=self LOADNODE_WORKLOAD=erc20-transfer LOADNODE_NONCE=table LOADNODE_CALL=1
    LOADNODE_BLOCKS="$BLOCKS" LOADNODE_TXS="$TXS" LOADNODE_DIR="$store"
    LOADNODE_RPCQ_HASHFILE="$store/hashes" LOADNODE_RPCQ_DONEFILE="$store/client-done" LOADNODE_RPCQ_HOLD_S=360)
  env -u GOGC -u GOMEMLIMIT -u GOMAXPROCS -u GODEBUG "${env_common[@]}" LOADNODE_RPCQ_LIVE=serve \
    "$bin" -test.run '^TestLoadNode$' -test.count=1 -test.timeout=900s >"$log.node.txt" 2>&1 &
  node=$!
  env -u GOGC -u GOMEMLIMIT -u GODEBUG "${env_common[@]}" GOMAXPROCS="$CLIENT_PROCS" \
    LOADNODE_CALL_EVERY_US="$CALL_EVERY_US" LOADNODE_RPCQ_CLIENT_S=600 LOADNODE_RPCQ_FIRST=2 \
    "$bin" -test.run '^TestCallLoadClient$' -test.count=1 -test.timeout=900s >"$log.client.txt" 2>&1 &
  client=$!
  node_rc=0; client_rc=0
  wait "$node" || node_rc=$?
  # A node that fails never reaches the client's first block or last block; stop the client then.
  [ "$node_rc" = 0 ] || kill "$client" 2>/dev/null || true
  wait "$client" || client_rc=$?
  [ "${KEEP_STORE:-0}" = 1 ] || rm -rf "$store"
  python3 - "$label" "$rev" "$log" "$node_rc" "$client_rc" "$OUT/results.tsv" <<'PY'
import re, sys, os
label, rev, log, node_rc, client_rc, tsv = sys.argv[1:]
def kv(line):
    return dict(re.findall(r'(\w+)=("[^"]*"|\S+)', line))
node = open(log + ".node.txt", errors="replace").read().splitlines()
client = open(log + ".client.txt", errors="replace").read().splitlines()
steady = next((kv(l) for l in node if l.startswith("LOADNODE steady ")), {})
proc = next((kv(l) for l in node if l.startswith("LOADNODE proc ")), {})
succ = next((kv(l) for l in node if l.startswith("LOADNODE success ")), {})
state = next((kv(l) for l in node if l.startswith("LOADNODE state ")), {})
m = {kv(l).get("method"): kv(l) for l in client if l.startswith("CALLCLIENT method=")}
c, e = m.get("eth_call", {}), m.get("eth_estimateGas", {})
def ok(t, ref):
    a = int(t.get("answered_in_span", 0)) + int(t.get("answered_late", 0))
    return f'{t.get("matched","?")}/{a}' + ("" if ref else "")
row = {
    "label": label, "rev": rev,
    "call_answered": c.get("answered_share", "?"), "est_answered": e.get("answered_share", "?"),
    "node_tx_s": steady.get("tx_per_s", "?"), "cpu_us_per_tx": proc.get("cpu_us_per_tx", "?"),
    "call_p50_ms": c.get("lat_ms_p50", "?"), "call_p99_ms": c.get("lat_ms_p99", "?"),
    "est_p50_ms": e.get("lat_ms_p50", "?"), "est_p99_ms": e.get("lat_ms_p99", "?"),
    "call_ok": ok(c, 0), "call_mismatched": c.get("mismatched", "?"), "call_unrecorded": c.get("unrecorded", "?"),
    "est_ok": ok(e, 1), "est_mismatched": e.get("mismatched", "?"),
    "call_offered": c.get("offered", "?"), "est_offered": e.get("offered", "?"),
    "txs_ok": succ.get("ok", "?"), "txs_failed": succ.get("failed", "?"),
    "apphash": state.get("final_apphash", "?")[:16], "node_rc": node_rc, "client_rc": client_rc,
    "log": os.path.basename(log),
}
print("RESULT " + " ".join(f"{k}={v}" for k, v in row.items()), flush=True)
for l in client:
    if l.startswith("CALLCLIENT_ERR"):
        print("  " + l[:200])
new = not os.path.exists(tsv)
with open(tsv, "a") as f:
    if new:
        f.write("\t".join(row) + "\n")
    f.write("\t".join(str(v) for v in row.values()) + "\n")
PY
done
