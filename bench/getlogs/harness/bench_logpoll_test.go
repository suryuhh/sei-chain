//go:build benchharness

// Log-poll benchmark at the littidx receipt store. TestLittPollFill writes LP_BLOCKS blocks of LP_TXS receipts with
// a fixed mix of token transfers and router swaps; TestLittPollSettle reopens the store and runs every poll shape for
// LP_SETTLE_S seconds; TestLittPollQuery times FilterLogs for the shape LP_ONLY names and prints one LITTPOLL line
// with CPU and wall per call and a digest of the answer.

package receipt_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth/filters"
	storetypes "github.com/sei-protocol/sei-chain/sei-cosmos/store/types"
	"github.com/sei-protocol/sei-chain/sei-cosmos/testutil"
	sdk "github.com/sei-protocol/sei-chain/sei-cosmos/types"
	dbconfig "github.com/sei-protocol/sei-chain/sei-db/config"
	"github.com/sei-protocol/sei-chain/sei-db/ledger_db/receipt"
	"github.com/sei-protocol/sei-chain/x/evm/types"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func ts2Env(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func ts2CPU() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

func ts2Addr(kind byte, i int) common.Address {
	var a common.Address
	a[0] = kind
	binary.BigEndian.PutUint64(a[12:], uint64(i)) //nolint:gosec // test indices are non-negative
	return a
}

var (
	ts2Transfer = crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	ts2Approval = crypto.Keccak256Hash([]byte("Approval(address,address,uint256)"))
	ts2Swap     = crypto.Keccak256Hash([]byte("Swap(address,uint256,uint256,uint256,uint256,address)"))
	ts2Sync     = crypto.Keccak256Hash([]byte("Sync(uint112,uint112)"))
)

func ts2Open(t *testing.T, dir string) (receipt.ReceiptStore, sdk.Context) {
	storeKey := storetypes.NewKVStoreKey("evm")
	ctx := testutil.DefaultContext(storeKey, storetypes.NewTransientStoreKey("evm_transient")).WithBlockHeight(1)
	cfg := dbconfig.DefaultReceiptStoreConfig()
	cfg.Backend = "littidx"
	cfg.DBDirectory = dir
	cfg.AsyncWriteBuffer = 0
	store, err := receipt.NewReceiptStore(cfg, storeKey)
	require.NoError(t, err)
	return store, ctx
}

// Block mix per 2,000 transactions: 70% transfers of one hot token, 10% transfers spread over 63 cold tokens,
// one transfer of a rare token, and 20% router swaps over 64 pools (half on one hot pool), each swap writing
// Transfer(in), Transfer(out), Sync and Swap on its pool; a tenth of transfers also carry an Approval.
func TestLittPollFill(t *testing.T) {
	dir := os.Getenv("LP_DIR")
	if dir == "" {
		t.Skip("LP_DIR unset")
	}
	blocks, perBlock, recipients := ts2Env("LP_BLOCKS", 2000), ts2Env("LP_TXS", 2000), 3000
	store, ctx := ts2Open(t, dir)
	word := func(a common.Address) string { return common.BytesToHash(a[:]).Hex() }
	amount := make([]byte, 32)
	t0 := time.Now()
	for b := 1; b <= blocks; b++ {
		records := make([]receipt.ReceiptRecord, perBlock)
		logIndex := uint32(0)
		for i := range perBlock {
			n := (b-1)*perBlock + i
			from, to := ts2Addr(1, n%50000), ts2Addr(2, n%recipients)
			var h common.Hash
			binary.BigEndian.PutUint64(h[:], uint64(n)) //nolint:gosec // test indices are non-negative
			h = crypto.Keccak256Hash(h[:])
			logs := []*types.Log{}
			add := func(addr common.Address, data []byte, topics ...string) {
				logs = append(logs, &types.Log{Address: addr.Hex(), Topics: topics, Data: data, Index: logIndex})
				logIndex++
			}
			callee := common.Address{}
			switch r := i % 10; {
			case i == 17:
				callee = ts2Addr(3, 999)
				add(callee, amount, ts2Transfer.Hex(), word(from), word(to))
			case r < 7:
				callee = ts2Addr(3, 0)
				if n%10 == 3 {
					add(callee, amount, ts2Approval.Hex(), word(from), word(ts2Addr(4, 0)))
				}
				add(callee, amount, ts2Transfer.Hex(), word(from), word(to))
			case r == 7:
				callee = ts2Addr(3, 1+n%63)
				add(callee, amount, ts2Transfer.Hex(), word(from), word(to))
			default:
				pool := 0
				if n%2 == 1 {
					pool = 1 + (n/2)%63
				}
				pa := ts2Addr(5, pool)
				callee = ts2Addr(6, 0)
				add(ts2Addr(3, 100+pool), amount, ts2Transfer.Hex(), word(from), word(pa))
				add(ts2Addr(3, 200+pool), amount, ts2Transfer.Hex(), word(pa), word(from))
				add(pa, append(amount, amount...), ts2Sync.Hex())
				add(pa, append(append(amount, amount...), append(amount, amount...)...), ts2Swap.Hex(), word(callee), word(from))
			}
			records[i] = receipt.ReceiptRecord{TxHash: h, Receipt: &types.Receipt{
				TxHashHex: h.Hex(), BlockNumber: uint64(b), TransactionIndex: uint32(i), GasUsed: 51000, //nolint:gosec // small test values
				Status: 1, From: from.Hex(), To: callee.Hex(), Logs: logs,
			}}
		}
		require.NoError(t, store.SetReceipts(ctx.WithBlockHeight(int64(b)), records))
	}
	require.Eventually(t, func() bool {
		logs, err := store.FilterLogs(ctx, uint64(blocks), uint64(blocks), filters.FilterCriteria{}, nil) //nolint:gosec // small
		return err == nil && len(logs) >= perBlock
	}, 5*time.Minute, 20*time.Millisecond)
	require.NoError(t, store.Close())
	fmt.Printf("LITTPOLL fill blocks=%d txs=%d fill_s=%.1f\n", blocks, perBlock, time.Since(t0).Seconds())
}

func lpCPU2() float64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &ts); err != nil {
		return -1
	}
	return float64(ts.Sec) + float64(ts.Nsec)/1e9
}

func lpMono() float64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		return -1
	}
	return float64(ts.Sec) + float64(ts.Nsec)/1e9
}

type lpQuery struct {
	name     string
	from, to uint64
	crit     filters.FilterCriteria
}

// lpQueries are the poll shapes: contract-events polls over 2,000-block ranges (as #4308's getContractEvents caller
// makes them) and 8-block windows (as the Cosmos evmrpc walks a range).
func lpQueries(all uint64) []lpQuery {
	cold3, rare := ts2Addr(3, 5), ts2Addr(3, 999)
	coldPool := ts2Addr(5, 7)
	w8 := all - 7
	return []lpQuery{
		{"ct_cold", 1, all, filters.FilterCriteria{Addresses: []common.Address{cold3}, Topics: [][]common.Hash{{ts2Transfer}}}},
		{"ct_rare", 1, all, filters.FilterCriteria{Addresses: []common.Address{rare}, Topics: [][]common.Hash{{ts2Transfer}}}},
		{"ct_cold_w8", w8, all, filters.FilterCriteria{Addresses: []common.Address{cold3}, Topics: [][]common.Hash{{ts2Transfer}}}},
		{"dense_w8", w8, all, filters.FilterCriteria{Addresses: []common.Address{ts2Addr(3, 0)}, Topics: [][]common.Hash{{ts2Transfer}}}},
		{"ce_all", 1, all, filters.FilterCriteria{Addresses: []common.Address{coldPool}}},
	}
}

func lpDigest(t *testing.T, logs []*ethtypes.Log) string {
	d := sha256.New()
	for _, lg := range logs {
		j, err := lg.MarshalJSON()
		require.NoError(t, err)
		d.Write(j)
	}
	return hex.EncodeToString(d.Sum(nil))[:16]
}

// TestLittPollSettle reopens the filled store and runs every shape for LP_SETTLE_S seconds, so the store's own
// background work after the fill lands in setup rather than in a timed reading.
func TestLittPollSettle(t *testing.T) {
	dir := os.Getenv("LP_DIR")
	if dir == "" {
		t.Skip("LP_DIR unset")
	}
	store, ctx := ts2Open(t, dir)
	all := uint64(store.LatestVersion()) //nolint:gosec // non-negative
	t0 := time.Now()
	rounds := 0
	for time.Since(t0).Seconds() < float64(ts2Env("LP_SETTLE_S", 20)) {
		for _, q := range lpQueries(all) {
			_, err := store.FilterLogs(ctx, q.from, q.to, q.crit, nil)
			require.NoError(t, err)
		}
		rounds++
	}
	require.NoError(t, store.Close())
	fmt.Printf("LITTPOLL settle latest=%d rounds=%d seconds=%.1f\n", all, rounds, time.Since(t0).Seconds())
}

// TestLittPollQuery opens the filled store and times FilterLogs for the shape LP_ONLY names, over at least LP_MS
// milliseconds and 3 calls after one untimed call, printing CPU and wall per call on two clocks each and a digest of
// the answer.
func TestLittPollQuery(t *testing.T) {
	dir, only := os.Getenv("LP_DIR"), os.Getenv("LP_ONLY")
	if dir == "" || only == "" {
		t.Skip("LP_DIR or LP_ONLY unset")
	}
	store, ctx := ts2Open(t, dir)
	t.Cleanup(func() { _ = store.Close() })
	all := uint64(store.LatestVersion()) //nolint:gosec // non-negative
	var q *lpQuery
	for _, c := range lpQueries(all) {
		if c.name == only {
			q = &c
		}
	}
	require.NotNil(t, q, "unknown shape %q", only)
	logs, err := store.FilterLogs(ctx, q.from, q.to, q.crit, nil)
	require.NoError(t, err)
	digest := lpDigest(t, logs)
	seconds := float64(ts2Env("LP_MS", 4000)) / 1000
	calls, consistent := 0, true
	c0, k0, t0, m0 := ts2CPU(), lpCPU2(), time.Now(), lpMono()
	for calls < 3 || time.Since(t0).Seconds() < seconds {
		got, err := store.FilterLogs(ctx, q.from, q.to, q.crit, nil)
		require.NoError(t, err)
		if len(got) != len(logs) {
			consistent = false
		}
		calls++
	}
	cpu, cpu2, wall, wall2 := ts2CPU()-c0, lpCPU2()-k0, time.Since(t0).Seconds(), lpMono()-m0
	n := float64(calls)
	fmt.Printf("LITTPOLL query=%s latest=%d blocks=%d logs=%d calls=%d consistent=%t cpu_us_per_call=%.1f cpu2_us_per_call=%.1f wall_us_per_call=%.1f wall2_us_per_call=%.1f digest=%s\n",
		q.name, all, q.to-q.from+1, len(logs), calls, consistent, 1e6*cpu/n, 1e6*cpu2/n, 1e6*wall/n, 1e6*wall2/n, digest)
}
