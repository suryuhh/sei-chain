# Benchmarks for the Giga EVM-only execution and RPC changes

This directory reproduces the performance figures quoted in the executor, node and RPC pull requests. One command
builds two revisions side by side, runs them alternately on the same machine, prints both sides' figures and the paired
change with its 95% interval, and fails if the two sides' outputs differ in any way the benchmark checks.

```sh
bench/compare.sh <base-ref> <head-ref> <workload> [pairs]
```

Run it from a clone of sei-chain that has both refs (or set `BENCH_REPO`). It checks each ref out into a temporary
git worktree, copies the workload's harness into it, builds, and removes the worktrees when it finishes. The harness
files are Go test files carrying the `benchharness` build tag, so they only compile when a benchmark asks for them,
and they never become part of either ref. Readings, logs and the summary are kept under
`~/.cache/sei-bench/results/` (or `BENCH_OUT`).

## Reproducing a pull request's figures

Each row compares the pull request's head with the branch it is opened against. Where a figure in a pull request was
measured over a different base, the pull request says so.

| pull request | command |
|---|---|
| executor/dependent-blocks-sequential | `bench/compare.sh sei/4273-head executor/dependent-blocks-sequential eth8`, then `sei2`, `mix250`, `mix0` |
| executor/block-stm | `bench/compare.sh sei/4273-head executor/block-stm eth8`, then `sei2`, `mix250`, `mix0` |
| rpc/eth-call-while-executing | `BASE_DIR=<checkout of sei/main-3a61e67> PATCHED_DIR=<checkout of rpc/eth-call-while-executing> bench/eth-call-main/probe.sh` |
| rpc/receipt-hash-from-commitqc | `bench/compare.sh sei/giga-1-9bfc0d6 rpc/receipt-hash-from-commitqc rpc-receipt:serve`, then `rpc-receipt:serve10` |
| evmrpc/receipt-hold | `bench/compare.sh sei/main-3a61e67 evmrpc/receipt-hold wallet-wait:ethers`, then `wallet-wait:viem`, `wallet-wait:load` |

## Machine behind the published figures

A 16-vCPU AMD EPYC Genoa virtual machine (8 cores × 2 threads, local disk) for every figure in the five pull requests.
All ran Linux with the Go toolchain `go.mod` names (Go 1.27.1) and `GOMAXPROCS` at the machine's 16 CPUs. The
benchmarks run on macOS too (the kit was rehearsed on an 8-core arm64 laptop), but a laptop's timings are not
comparable to these, and several validity gates (below) are tuned for a quiet 16-CPU machine.

## What each benchmark measures

**Validator replay: `eth8`, `sei2`, `mix250`, `mix0`** (`replay/`). Recorded transactions applied as one block each
through `NewEVMOnlyApplication` in `sei-tendermint/internal/evmonlyapp`, over a fresh FlatKV, LtHash and receipt
store whose genesis is the window's recorded start state. `PrepareBlock`, `FinalizeBlock` and `Commit` are timed,
then `AwaitCommits`; three repeats per window at `GOMAXPROCS` 16 (`BENCH_PROCS`). Every block takes the path a
validator takes for blocks it did not admit: the CheckTx sender cache is empty, and a tree with the sender-key sidecar
receives the producer's keys. `eth8` is eight Ethereum mainnet windows of 2,000 transactions (starting at block
26037338, every 875 blocks); `sei2` is two windows of Sei mainnet EVM traffic (574 and 1,131 transactions) each pooled
into one block; `mix250` spreads 250 of those Sei transactions through a 2,000-transaction block of ERC-20 transfers;
`mix0` is 2,000 conflict-free ERC-20 transfers. Figures: transactions per second over the sum of per-window median
wall times (primary), the slowest window, CPU per transaction and peak RSS. Gates: per window and repeat, the app
hash, the FlatKV LtHash, the per-transaction gas and error digest, the success and failure counts, agreement with the
source chain's receipts and the stored receipts all equal pinned values. The boundary is block application at one
validator, not network throughput.

**Execute loop: `loop-erc20`, `loop-native`, `loop-replay-conflicts`, `loop-swaps`** (`loop/`). One Autobahn
EVM-only validator runs in process in `sei-tendermint/internal/p2p`: the real `evmonlyapp`, Giga executor, FlatKV,
littidx receipt store, fsynced block store and hash vault, and `runExecute`. It presigns 120 blocks of 2,000
transactions, admits them, feeds them through the producer's mempool and times its committed blocks; each reading is
two fresh processes. Variants choose the admission path (`self`: this validator's CheckTx admitted every transaction;
`foreign`: `PrepareBlock` recovers every sender; `carried`: the producer's sender keys arrive with the block) and the
block content: conflict-free ERC-20 transfers, native 21,000-gas transfers, ERC-20 blocks whose conflicts replay
eight Ethereum mainnet windows or pooled Sei traffic, contracts touching every storage key those windows read and
wrote, and SushiSwap V2 router swaps over 64 pools, with WSEI paid in the native coin in the `swap_native*`
variants. The conflict and swap schedules are data (`loop-replay.tar.xz`). Figures: steady-span transactions per
second (primary), transactions per CPU-second, the executor's share of the main loop, per-block execution, storage
and parse time, and peak RSS. Gates: every transaction succeeds with its receipt and the pinned total gas; the final
app hash and a digest over all 120 app hashes, the LtHash recomputed from the store, and digests over every receipt
and log all equal pinned values (the store is read after the node exits by `seidb dump-flatkv` and a receipt reader
built from the base ref, `lib/receiptreader`); the admission path the variant names ran on every transaction; and the
node, not the feeder, set the rate.

**RPC under execution: `rpc-receipt`, `rpc-call`** (`rpc/`, `call/`). The same one-validator node executing its own
ERC-20 blocks with its EVM-only JSON-RPC server up, and a separate client process. `rpc-receipt` requests each
executed transaction's receipt 1 s after its block is first reported (`serve`; every tenth with `serve10`), or runs
`eth_getBlockByNumber("latest", false)` or a four-call wallet flow; `rpc-call` sends one `eth_call` every 500 µs.
Figures: node transactions per second, CPU per transaction, peak RSS, answer latency and the share of requests
answered. Gates: the loop gates above, every request answered, every receipt's `blockHash` equal to the node's own
`BlockByNumber` hash for its height, answer digests equal to the shipped code's, every `eth_call` answer equal to the
state recorded at its height, and the client below 60% of its processors.

**Wallet wait: `wallet-wait`** (`wallet/`). One `seid` validator from the `seid init` defaults at about mainnet's
0.36 s blocks, with unmodified ethers 6.17.0 and viem 2.56.9 on their default waits in a separate node.js process
(one send at a time, or 100 sends per second over 3,000 keys with `load`). Figures: median wait from send to receipt
(primary), node CPU per wait, node cores and peak RSS. Gates: every wait returns the sent transaction's successful
receipt, equal to a plain `eth_getTransactionReceipt` re-read. Needs network once to fetch node.js (sha256-checked)
and the pinned npm packages, and free ports 36xxx.

**Log polls: `getlogs-cold`** (`getlogs/`). CPU per `FilterLogs` call at the littidx receipt store
(`sei-db/ledger_db/receipt`), over 2,000 blocks of 2,000 receipts each (70% transfers of one hot token, 10% over 63
cold tokens, one rare-token transfer, 20% router swaps over 64 pools), each side filling its own store. `cold` polls a
cold token's Transfer events over 2,000 blocks; `dense` the hot token over 8 blocks; `addr` a cold pool by address.
Gates: log counts and the SHA-256 over every log's JSON equal pinned values on both sides. This is the store, not an
HTTP node.

**Fullnode follow: `fullsync`** (`follow/`). A 16-validator Autobahn committee in one process at the shipped 400 ms
seal interval and light load, and one EVM-only fullnode dialing it over TCP; the figure is the share of the chain's
blocks the fullnode executed. `paced50` and `rtt100`/`rtt250` pace one validator at 50 or 100 blocks per second, the
latter through a relay delayed by `tc netem` (Linux, root). Gates: the fullnode's app hash and transaction digest
equal the validator's at every checked height.

## How the numbers are computed

Each side is read `pairs` times, alternating which side goes first. For each figure, each pair gives one effect: the
relative change 100 × (head − base) / base, or, for memory and shares, the absolute change in the figure's unit. The
printed change is the mean of the per-pair effects with a two-sided Student-t interval on n − 1 degrees of freedom
(`BENCH_CONFIDENCE`, default 0.95); one pair gives no interval. A figure whose independent second count (the wrapper's
own clock or a second counter) disagrees with it beyond the benchmark's tolerance is left out of that pair. The run
fails if any gate fails on either side or if the output digests differ between any two readings.

## Requirements and data

Git, `make`, Python 3.9 or newer, and the Go toolchain `go.mod` asks for (with `GOTOOLCHAIN=auto` the Go command
fetches it). A build of both sides needs about 20 GB free (`BENCH_MIN_FREE_GB`); each workload's run takes from a few
minutes (replay) to about 15 minutes per pair (wallet load, fullnode).

The recorded windows ship with the kit in `bench/data/files` (about 21 MB). `bench/data/manifest.json` lists each data file with its size and sha256; `compare.sh` calls `bench/fetch-data.sh`, which copies the files a workload needs into `~/.cache/sei-bench/data` (or `BENCH_DATA_DIR`) and refuses any file whose sha256 differs; `BENCH_DATA_URL` points it at another copy instead. The replay workloads also check every block file inside the archives against the per-file manifests.

## Layout

```
compare.sh          the entry point
fetch-data.sh       data download and verification
data/manifest.json  data files and their sha256
lib/stats.py        paired effects, interval, gate and digest verdict
lib/common.py       GOEXPERIMENT, harness placement, readers built from the base ref, second-count check
lib/receiptreader/  the receipt-store reader
replay/ loop/ rpc/ call/ wallet/ getlogs/ follow/
                    run.py (setup, one reading, self-test) and the harness each workload copies into a checkout
```

`python3 bench/<dir>/run.py self-test` checks each runner's parsing and gates against planted failures.
