#!/bin/sh
# Paired comparison of two sei-chain revisions on one benchmark workload.
#
#   bench/compare.sh <base-ref> <head-ref> <workload> [pairs]
#
# Checks out both refs into temporary git worktrees of the repository you run it from (or BENCH_REPO), copies the
# workload's harness into each (the harness files never become part of either ref), builds, then runs base and head
# alternately, `pairs` times each (default 3; pair i runs base first when i is even, head first when odd). It prints
# every reading, then for each figure the paired mean change of head against base with its 95% interval, and exits
# non-zero, loudly, if any output-equality gate fails on either side or any output digest differs between them.
#
# Workloads (see bench/README.md for what each measures):
#   eth8 sei2 mix250 mix0                           validator block replay
#   loop-erc20[:self|foreign|carried]               one-validator execute loop, conflict-free ERC-20 transfers
#   loop-native[:self|foreign|carried]              the same loop, native 21,000-gas transfers
#   loop-replay-conflicts[:eth|eth_touch|eth_touchcode|sei|hot1|chain1|eth_nochain|eth_chainonly]
#   loop-swaps[:swap|swap_hot|swap_native|swap_native_mixed]   (conflicts and swaps: see bench/loop/run.py)
#   rpc-receipt[:serve|serve10|off|latest10|wallet|wallet10|call20]  node with its JSON-RPC server and a client
#   rpc-call[:call|off]                             the same node answering eth_call every 500 us
#   wallet-wait[:ethers|viem|load|ethers-default|viem-default]  seid validator and unmodified wallet libraries
#   getlogs-cold[:cold|dense|addr|rare|cold-w8]     receipt-store log polls
#   fullsync[:committee16|committee8|committee40|paced50|rtt100|rtt250|serve10]  an EVM-only fullnode following
#                                                   the chain (rtt*: Linux, root, tc)
#
# Environment:
#   BENCH_REPO        repository holding both refs (default: the git repository of the current directory)
#   BENCH_DATA_DIR    where data files are kept (default ~/.cache/sei-bench/data); BENCH_DATA_URL: see fetch-data.sh
#   BENCH_OUT         where readings and logs are kept (default ~/.cache/sei-bench/results/<time>-<workload>)
#   BENCH_PROCS       GOMAXPROCS for the replay workloads (default 16)
#   BENCH_CONFIDENCE  interval level (default 0.95)
#   BENCH_MIN_FREE_GB refuse to start with less free disk than this in the work directory (default 20)
#   BENCH_KEEP=1      keep the worktrees and build outputs
set -eu

if [ $# -lt 3 ] || [ $# -gt 4 ]; then
  awk 'NR > 1 && /^set -eu/ {exit} NR > 1 {sub(/^# ?/, ""); print}' "$0" >&2
  exit 2
fi
BASE_REF=$1
HEAD_REF=$2
WORKLOAD=$3
PAIRS=${4:-3}
case "$PAIRS" in ''|*[!0-9]*|0) echo "pairs must be a positive integer" >&2; exit 2 ;; esac

KIT=$(cd "$(dirname "$0")" && pwd)
case "$WORKLOAD" in
  eth8|sei2|mix250|mix0) FAMILY=replay ;;
  loop-*) FAMILY=loop ;;
  rpc-receipt|rpc-receipt:*) FAMILY=rpc ;;
  rpc-call|rpc-call:*) FAMILY=call ;;
  wallet-wait|wallet-wait:*) FAMILY=wallet ;;
  getlogs-cold|getlogs-cold:*) FAMILY=getlogs ;;
  fullsync|fullsync:*) FAMILY=follow ;;
  *) echo "unknown workload: $WORKLOAD (see bench/compare.sh --help)" >&2; exit 2 ;;
esac
RUN="$KIT/$FAMILY/run.py"
python3 "$RUN" check --workload "$WORKLOAD"

REPO=${BENCH_REPO:-$(git rev-parse --show-toplevel)}
BASE_SHA=$(git -C "$REPO" rev-parse --verify "$BASE_REF^{commit}")
HEAD_SHA=$(git -C "$REPO" rev-parse --verify "$HEAD_REF^{commit}")
DATA=${BENCH_DATA_DIR:-$HOME/.cache/sei-bench/data}
export BENCH_DATA_DIR="$DATA"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=${BENCH_OUT:-$HOME/.cache/sei-bench/results/$STAMP-$(echo "$WORKLOAD" | tr ':' '-')}
mkdir -p "$OUT/runs"
WORK=$(mktemp -d "${TMPDIR:-/tmp}/sei-bench.XXXXXX")
WORK=$(cd "$WORK" && pwd -P)

free_gb=$(df -Pk "$WORK" | awk 'NR==2 {printf "%d", $4 / 1048576}')
if [ "$free_gb" -lt "${BENCH_MIN_FREE_GB:-20}" ]; then
  echo "only ${free_gb} GB free under $WORK; need ${BENCH_MIN_FREE_GB:-20} (BENCH_MIN_FREE_GB)" >&2
  rm -rf "$WORK"
  exit 1
fi

cleanup() {
  if [ "${BENCH_KEEP:-0}" = 1 ]; then
    echo "kept $WORK"
    return
  fi
  for side in base head; do
    if [ -d "$WORK/$side" ]; then
      python3 "$RUN" teardown --tree "$WORK/$side" --out "$WORK/out-$side" >/dev/null 2>&1 || true
      git -C "$REPO" worktree remove --force "$WORK/$side" >/dev/null 2>&1 || true
    fi
  done
  git -C "$REPO" worktree prune >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

echo "workload $WORKLOAD, $PAIRS pair(s)"
echo "base $BASE_REF = $BASE_SHA"
echo "head $HEAD_REF = $HEAD_SHA"
echo "machine: $(uname -srm), $(python3 -c 'import os; print(os.cpu_count())') logical CPUs, $(go version 2>/dev/null | awk '{print $3}') on PATH"
echo "readings and logs: $OUT"
{
  echo "workload=$WORKLOAD pairs=$PAIRS"
  echo "base=$BASE_REF $BASE_SHA"
  echo "head=$HEAD_REF $HEAD_SHA"
  echo "started=$STAMP"
  uname -a
} > "$OUT/about.txt"

ASSETS=$(python3 "$RUN" assets --workload "$WORKLOAD")
if [ -n "$ASSETS" ]; then
  # shellcheck disable=SC2086
  sh "$KIT/fetch-data.sh" $ASSETS
fi

for side in base head; do
  if [ "$side" = base ]; then sha=$BASE_SHA; else sha=$HEAD_SHA; fi
  git -C "$REPO" worktree add --detach "$WORK/$side" "$sha" >/dev/null 2>&1 || {
    echo "could not check out $sha into $WORK/$side" >&2; exit 1; }
done

echo "building shared tools from base"
if ! python3 "$RUN" tools --tree "$WORK/base" --out "$WORK/tools" --workload "$WORKLOAD" > "$OUT/tools.log" 2>&1; then
  tail -30 "$OUT/tools.log" >&2
  echo "building the tools failed; full log: $OUT/tools.log" >&2
  exit 1
fi
for side in base head; do
  echo "building $side"
  if ! python3 "$RUN" setup --tree "$WORK/$side" --out "$WORK/out-$side" --data "$DATA" --tools "$WORK/tools" \
      --workload "$WORKLOAD" > "$OUT/setup-$side.log" 2>&1; then
    tail -30 "$OUT/setup-$side.log" >&2
    echo "building $side failed; full log: $OUT/setup-$side.log" >&2
    exit 1
  fi
done

reading() { # side pair
  log="$OUT/runs/$2-$1.log"
  python3 "$RUN" measure --tree "$WORK/$1" --out "$WORK/out-$1" --data "$DATA" --tools "$WORK/tools" \
    --workload "$WORKLOAD" > "$log" 2>&1 || true
  if ! grep '^RESULT ' "$log" | tail -1 | sed 's/^RESULT //' > "$OUT/runs/$2-$1.json" || [ ! -s "$OUT/runs/$2-$1.json" ]; then
    tail -30 "$log" >&2
    echo "pair $2 $1 printed no result; full log: $log" >&2
    exit 1
  fi
  echo "pair $2 $1: $(python3 "$KIT/lib/stats.py" summarize "$OUT/runs/$2-$1.json")"
}

i=0
while [ "$i" -lt "$PAIRS" ]; do
  if [ $((i % 2)) -eq 0 ]; then reading base "$i"; reading head "$i"; else reading head "$i"; reading base "$i"; fi
  i=$((i + 1))
done

set +e
python3 "$KIT/lib/stats.py" compare "$OUT/runs" "$WORKLOAD: $HEAD_REF vs $BASE_REF" | tee "$OUT/summary.txt"
status=$(python3 "$KIT/lib/stats.py" compare "$OUT/runs" "$WORKLOAD" > /dev/null 2>&1; echo $?)
set -e
if [ "$status" != 0 ]; then
  echo "FAILED: see $OUT" >&2
  exit 1
fi
