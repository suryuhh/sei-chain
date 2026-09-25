eth_call / eth_estimateGas under back-to-back blocks: base vs patched

What it measures
  One in-process Autobahn EVM-only validator (the tree's real evmonlyapp, Giga executor, FlatKV
  state store and receipt store, fed through its real mempool and consensus) executes presigned
  2,000-transaction ERC-20 transfer blocks back to back (120 blocks by default) and serves its real
  EVM-only JSON-RPC on 127.0.0.1:8545. A separate client process (GOMAXPROCS=2) sends one eth_call
  and one eth_estimateGas every 500 us each, open loop, never retried, from the first executed block
  to the last. Both requests carry creation code that returns NUMBER and BALANCE(0x0); the coinbase
  0x0 receives every block's fees, so its balance identifies the state a call ran against.

Per run (RESULT line and a row of results.tsv)
  call_answered, est_answered   share of offered requests answered while blocks executed
  node_tx_s                     node throughput over the steady span (after the first 10% of blocks)
  cpu_us_per_tx                 node process CPU over the block window / transactions executed
                                (includes serving the answered requests)
  call_p50_ms .. est_p99_ms     latency of answered requests
  call_ok = M/A                 answered eth_call whose (NUMBER, BALANCE) equals the coinbase balance
                                the node recorded when that height's FinalizeBlock returned
  est_ok = M/A                  answered eth_estimateGas equal to 20 sequential re-estimates made
                                after the last block (which must all agree)
  apphash                       final app hash; identical for both trees on the same workload
  txs_ok / txs_failed           executed transactions and failures (expected 240000 / 0)

Files
  probe.sh [pairs]              alternates base, patched, base, patched ... <pairs> times [3], then
                                prints per-side medians (summarize.py) and correctness checks
  run.sh <checkout> <runs> [label]
                                builds the harness against one checkout and runs it <runs> times
  harness/loadnode_test.go      the harness; added to sei-tendermint/internal/p2p by a go build
                                overlay, so the trees are never written to (see run.sh for the one
                                InitChain precondition the overlay relaxes, identically for both)
  trees/base, trees/patched     sei-chain source at the two commits (REVISION in each); the
                                prebuilt wasmvm libraries are left out, this build does not use them

Inputs (environment, defaults in brackets)
  BASE_DIR [trees/base]  PATCHED_DIR [trees/patched]  OUT [./out]  BLOCKS [120]  TXS [2000]
  CALL_EVERY_US [500]  CLIENT_PROCS [2]  GO [go]  KEEP_STORE [0]

Requirements
  Linux or macOS; Go >= 1.21 on PATH with GOTOOLCHAIN=auto (the default), which fetches the go 1.27.1
  toolchain go.mod asks for; network access to proxy.golang.org for modules on the first build (or a
  populated GOMODCACHE); python3; port 8545 free on 127.0.0.1; about 3 GB free under $TMPDIR per run.

Usage on a 16-vCPU host
  mkdir m && tar -C m -xJf payload.tar.xz && cd m
  ./probe.sh 3 2>&1 | tee probe.log

Outputs
  out/results.tsv   one row per run
  out/logs/         node and client output per run
  out/host.txt      CPU model, vCPUs, memory, kernel, Go version
  stdout            RESULT lines, then MEDIAN, CHECK and APPHASH lines
