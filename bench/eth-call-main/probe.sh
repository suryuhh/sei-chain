#!/usr/bin/env bash
# probe.sh [pairs]
#
# Runs the eth_call / eth_estimateGas load measurement on base and patched sei-chain trees in
# alternating order (base, patched, base, patched, ...), <pairs> times [default 3], and prints a
# summary per side. Run it from the directory payload.tar.xz was unpacked into:
#
#   mkdir m && tar -C m -xJf payload.tar.xz && cd m && ./probe.sh 3
#
# Inputs (all inside the payload, overridable by environment):
#   BASE_DIR     [./trees/base]     sei-chain at the base commit (see trees/base/REVISION)
#   PATCHED_DIR  [./trees/patched]  the same tree with the change applied (trees/patched/REVISION)
#   OUT          [./out]            where binaries, logs and results go
#   BLOCKS [120] TXS [2000] CALL_EVERY_US [500] CLIENT_PROCS [2]   passed through to run.sh
# Requirements: Go matching go.mod's toolchain line (go 1.27.x) on PATH or as $GO, python3, a free
# 127.0.0.1:8545, and a few GB of free disk under $TMPDIR for each run's store.
#
# Outputs:
#   $OUT/results.tsv     one row per run: side, revision, answered share per method, node tx/s,
#                        CPU per tx, p50/p99 of answered requests, correctness counts, app hash
#   $OUT/logs/*.txt      the node's and the client's full output per run
#   $OUT/host.txt        CPU model, core count, kernel and Go version of the machine
#   stdout               one RESULT line per run, then per-side medians
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pairs="${1:-3}"
BASE_DIR="${BASE_DIR:-$here/trees/base}"
PATCHED_DIR="${PATCHED_DIR:-$here/trees/patched}"
export OUT="${OUT:-$here/out}"
mkdir -p "$OUT"
GO="${GO:-go}"
{
  echo "date_utc=$(date -u +%FT%TZ)"
  echo "uname=$(uname -a)"
  echo "go=$(cd "$BASE_DIR" && "$GO" version)"
  if [ -r /proc/cpuinfo ]; then
    echo "cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
    echo "vcpus=$(nproc)"
    echo "mem_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')"
  else
    echo "cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
    echo "vcpus=$(sysctl -n hw.ncpu 2>/dev/null || echo unknown)"
  fi
} >"$OUT/host.txt"
cat "$OUT/host.txt"

for i in $(seq 1 "$pairs"); do
  echo "== pair $i of $pairs" >&2
  "$here/run.sh" "$BASE_DIR" 1 base
  "$here/run.sh" "$PATCHED_DIR" 1 patched
done

python3 "$here/summarize.py" "$OUT/results.tsv"
