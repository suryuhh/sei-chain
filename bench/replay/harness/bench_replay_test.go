//go:build benchharness

package evmonlyapp

// Replay benchmark: recorded transaction windows, each executed as one block through the EVM-only application a
// validator runs (PrepareBlock, FinalizeBlock and Commit over FlatKV, the LtHash and the receipt store), on a fresh
// store whose genesis is the window's start state (the first prestateTracer value of every account field and slot
// the window touches). Each transaction's own signature names its sender. NR_SENDERS=own places senders in the
// application's CheckTx cache as a validator does for transactions it admitted itself; NR_SENDERS=foreign leaves
// that cache empty and, where the tree supports it, attaches the producer's sender-key sidecar
// (bench_replay_keys_test.go). The application's minimum gas price is not applied (its base fee is 0, so a
// mainnet transaction's effective price is its tip). Prints NR lines per window and repeat, and NRMARK lines
// around the timed PrepareBlock, FinalizeBlock and Commit. With NR_MIX_N set, each window's transactions are
// spread evenly through a block of NR_MIX_N transactions whose others are ERC-20 transfers from fresh senders
// (nrMix).

import (
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/sei-protocol/sei-chain/giga/evmonly"
	sdk "github.com/sei-protocol/sei-chain/sei-cosmos/types"
	"github.com/sei-protocol/sei-chain/sei-db/bootstrap"
	abci "github.com/sei-protocol/sei-chain/sei-tendermint/abci/types"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/require"
	tmproto "github.com/sei-protocol/sei-chain/sei-tendermint/proto/tendermint/types"
	loadoffline "github.com/sei-protocol/sei-load/generator/offline"
)

type nrAcct struct {
	Balance *hexutil.Big           `json:"balance"`
	Nonce   uint64                 `json:"nonce"`
	Code    hexutil.Bytes          `json:"code"`
	Storage map[common.Hash]string `json:"storage"`
}

type nrBlockFile struct {
	Block struct {
		Number       hexutil.Uint64    `json:"number"`
		Timestamp    hexutil.Uint64    `json:"timestamp"`
		Transactions []json.RawMessage `json:"transactions"`
	} `json:"block"`
	Receipts []struct {
		TxHash  common.Hash    `json:"transactionHash"`
		Status  hexutil.Uint64 `json:"status"`
		GasUsed hexutil.Uint64 `json:"gasUsed"`
	} `json:"receipts"`
	Pre []struct {
		TxHash common.Hash                `json:"txHash"`
		Result map[common.Address]*nrAcct `json:"result"`
	} `json:"pre"`
}

type nrWindow struct {
	name    string
	time    uint64
	raw     [][]byte
	hashes  []common.Hash
	senders []common.Address
	status  []uint64
	gas     []uint64
	genesis evmonly.StateChangeSet
	accts   int
	slots   int
	blobs   int
}

// nrLoad reads the first n non-blob transactions from block start on, as the executor's real-window harness does,
// and builds the genesis change set from each account's and slot's first prestate value.
func nrLoad(t *testing.T, dir string, start uint64, n int) *nrWindow {
	w := &nrWindow{name: fmt.Sprintf("eth-%d", start)}
	balances := map[common.Address]*big.Int{}
	nonces := map[common.Address]uint64{}
	code := map[common.Address][]byte{}
	storage := map[common.Address]map[common.Hash]common.Hash{}
	seenSlot := map[common.Address]map[common.Hash]bool{}
	dropped := map[common.Address]bool{}
	for blk := start; len(w.raw) < n; blk++ {
		var bf nrBlockFile
		if f, err := os.Open(filepath.Join(dir, fmt.Sprintf("%d.json.gz", blk))); err == nil {
			zr, err := gzip.NewReader(f)
			require.NoError(t, err)
			require.NoError(t, json.NewDecoder(zr).Decode(&bf))
			_ = f.Close()
		} else {
			b, err := os.ReadFile(filepath.Join(dir, fmt.Sprintf("%d.json", blk)))
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(b, &bf))
		}
		if blk == start {
			w.time = uint64(bf.Block.Timestamp)
		}
		pre := map[common.Hash]map[common.Address]*nrAcct{}
		for _, p := range bf.Pre {
			pre[p.TxHash] = p.Result
		}
		rc := map[common.Hash]int{}
		for i, r := range bf.Receipts {
			rc[r.TxHash] = i
		}
		for _, rawTx := range bf.Block.Transactions {
			if len(w.raw) >= n {
				break
			}
			var tx ethtypes.Transaction
			require.NoError(t, tx.UnmarshalJSON(rawTx))
			var from struct {
				From common.Address `json:"from"`
			}
			_ = json.Unmarshal(rawTx, &from)
			if tx.Type() == ethtypes.BlobTxType || dropped[from.From] {
				dropped[from.From] = true
				w.blobs++
				continue
			}
			b, err := tx.MarshalBinary()
			require.NoError(t, err)
			w.raw = append(w.raw, b)
			w.hashes = append(w.hashes, tx.Hash())
			w.senders = append(w.senders, from.From)
			r := bf.Receipts[rc[tx.Hash()]]
			w.status = append(w.status, uint64(r.Status))
			w.gas = append(w.gas, uint64(r.GasUsed))
			for addr, a := range pre[tx.Hash()] {
				if _, seen := balances[addr]; !seen {
					w.accts++
					balances[addr] = new(big.Int)
					if a.Balance != nil {
						balances[addr] = a.Balance.ToInt()
					}
					if a.Nonce > 0 {
						nonces[addr] = a.Nonce
					}
					if len(a.Code) > 0 {
						code[addr] = a.Code
					}
				}
				for k, v := range a.Storage {
					m := seenSlot[addr]
					if m == nil {
						m = map[common.Hash]bool{}
						seenSlot[addr] = m
					}
					if m[k] {
						continue
					}
					m[k] = true
					w.slots++
					if val := common.HexToHash(v); val != (common.Hash{}) {
						s := storage[addr]
						if s == nil {
							s = map[common.Hash]common.Hash{}
							storage[addr] = s
						}
						s[k] = val
					}
				}
			}
		}
	}
	addrs := make([]common.Address, 0, len(balances))
	for a := range balances {
		addrs = append(addrs, a)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].Cmp(addrs[j]) < 0 })
	for _, a := range addrs {
		w.genesis.Balances = append(w.genesis.Balances, evmonly.BalanceChange{Address: a, Balance: balances[a]})
		if nonce, ok := nonces[a]; ok {
			w.genesis.Nonces = append(w.genesis.Nonces, evmonly.NonceChange{Address: a, Nonce: nonce})
		}
		if c, ok := code[a]; ok {
			w.genesis.Code = append(w.genesis.Code, evmonly.CodeChange{Address: a, Code: c})
		}
		keys := make([]common.Hash, 0, len(storage[a]))
		for k := range storage[a] {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].Cmp(keys[j]) < 0 })
		for _, k := range keys {
			w.genesis.Storage = append(w.genesis.Storage, evmonly.StorageChange{Address: a, Key: k, Value: storage[a][k]})
		}
	}
	return w
}

// nrGenesis collects a genesis state as sei-load's offline scenarios write it.
type nrGenesis struct {
	balances map[common.Address]*big.Int
	nonces   map[common.Address]uint64
	code     map[common.Address][]byte
	storage  map[common.Address]map[common.Hash]common.Hash
}

func (g *nrGenesis) SetBalance(a common.Address, b *big.Int) { g.balances[a] = new(big.Int).Set(b) }

func (g *nrGenesis) SetCode(a common.Address, c []byte) {
	if _, ok := g.balances[a]; !ok {
		g.balances[a] = new(big.Int)
	}
	g.code[a] = c
}

func (g *nrGenesis) SetState(a common.Address, k common.Hash, v common.Hash) {
	if _, ok := g.balances[a]; !ok {
		g.balances[a] = new(big.Int)
	}
	if g.storage[a] == nil {
		g.storage[a] = map[common.Hash]common.Hash{}
	}
	g.storage[a][k] = v
}

// changeSet returns the genesis in nrLoad's order: accounts by address, each with its nonce, code and slots by key.
func (g *nrGenesis) changeSet() evmonly.StateChangeSet {
	var cs evmonly.StateChangeSet
	addrs := make([]common.Address, 0, len(g.balances))
	for a := range g.balances {
		addrs = append(addrs, a)
	}
	sort.Slice(addrs, func(i, j int) bool { return addrs[i].Cmp(addrs[j]) < 0 })
	for _, a := range addrs {
		cs.Balances = append(cs.Balances, evmonly.BalanceChange{Address: a, Balance: g.balances[a]})
		if nonce, ok := g.nonces[a]; ok && nonce > 0 {
			cs.Nonces = append(cs.Nonces, evmonly.NonceChange{Address: a, Nonce: nonce})
		}
		if c, ok := g.code[a]; ok && len(c) > 0 {
			cs.Code = append(cs.Code, evmonly.CodeChange{Address: a, Code: c})
		}
		keys := make([]common.Hash, 0, len(g.storage[a]))
		for k, v := range g.storage[a] {
			if v != (common.Hash{}) {
				keys = append(keys, k)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i].Cmp(keys[j]) < 0 })
		for _, k := range keys {
			cs.Storage = append(cs.Storage, evmonly.StorageChange{Address: a, Key: k, Value: g.storage[a][k]})
		}
	}
	return cs
}

// nrMixToken is the ERC-20 contract the mix's transfers call, sei-load's offline token as the executor's loadtest
// deploys it.
var nrMixToken = common.HexToAddress("0x000000000000000000000000000000000000e20c")

// nrMix spreads w's transactions evenly through a block of n: the window's j-th transaction lands at position
// j*n/len(w.raw), and every other position is an ERC-20 transfer of one token unit from a fresh sender (nonce 0,
// seeded with 1 SEI and one token unit) to a fresh recipient, signed for chainID at 1 gwei and 100,000 gas. The
// token's code and the senders' seeds join the window's genesis.
func nrMix(t *testing.T, w *nrWindow, n int, chainID uint64) {
	s := len(w.raw)
	require.True(t, s <= n, "mix of %d transactions into %d", s, n)
	scenario, err := loadoffline.NewScenario(loadoffline.ERC20Transfer, loadoffline.Config{
		ChainID:       new(big.Int).SetUint64(chainID),
		GasPrice:      big.NewInt(1_000_000_000),
		SenderBalance: big.NewInt(1_000_000_000_000_000_000),
		TransferValue: big.NewInt(1),
		GasLimit:      100_000,
		ERC20Contract: nrMixToken,
	})
	require.NoError(t, err)
	g := &nrGenesis{balances: map[common.Address]*big.Int{}, nonces: map[common.Address]uint64{},
		code: map[common.Address][]byte{}, storage: map[common.Address]map[common.Hash]common.Hash{}}
	for _, b := range w.genesis.Balances {
		g.balances[b.Address] = b.Balance
	}
	for _, x := range w.genesis.Nonces {
		g.nonces[x.Address] = x.Nonce
	}
	for _, c := range w.genesis.Code {
		g.code[c.Address] = c.Code
	}
	for _, x := range w.genesis.Storage {
		g.SetState(x.Address, x.Key, x.Value)
	}
	require.NoError(t, scenario.SetupGenesis(g))
	raw := make([][]byte, 0, n)
	hashes := make([]common.Hash, 0, n)
	senders := make([]common.Address, 0, n)
	status := make([]uint64, 0, n)
	gas := make([]uint64, 0, n)
	next, filler := 0, 0
	for pos := range n {
		if next < s && pos == next*n/s {
			raw, hashes, senders = append(raw, w.raw[next]), append(hashes, w.hashes[next]), append(senders, w.senders[next])
			status, gas = append(status, w.status[next]), append(gas, w.gas[next])
			next++
			continue
		}
		var key *ecdsa.PrivateKey
		for attempt := uint64(0); key == nil; attempt++ {
			var buf [16]byte
			binary.BigEndian.PutUint64(buf[:8], uint64(filler)) //nolint:gosec // filler is non-negative
			binary.BigEndian.PutUint64(buf[8:], attempt)
			key, _ = crypto.ToECDSA(crypto.Keccak256([]byte("nodemix-sender"), buf[:]))
		}
		sender := crypto.PubkeyToAddress(key.PublicKey)
		recipient := common.BytesToAddress(crypto.Keccak256([]byte("nodemix-recipient"), sender.Bytes()))
		scenario.SeedSender(g, sender)
		tx, err := scenario.BuildTransaction(key, 0, recipient)
		require.NoError(t, err)
		b, err := tx.MarshalBinary()
		require.NoError(t, err)
		raw, hashes, senders = append(raw, b), append(hashes, tx.Hash()), append(senders, sender)
		// A transfer has no source-chain receipt, so it never counts toward mainnet_match.
		status, gas = append(status, 1), append(gas, 0)
		filler++
	}
	w.raw, w.hashes, w.senders, w.status, w.gas = raw, hashes, senders, status, gas
	w.genesis = g.changeSet()
}

// nrInstallUncheckedExecutor replaces the application's executor with one built as newExecutor builds it, less the
// minimum gas price check.
func nrInstallUncheckedExecutor(t *testing.T, a *evmOnlyApplication) {
	for slot := range a.executor.Lock() {
		old, ok := slot.Get()
		require.True(t, ok)
		old.Close()
		executor := evmonly.NewExecutor(evmonly.Config{
			ChainConfig:          a.chainConfig,
			MinGasPrice:          big.NewInt(evmOnlyMinGasPrice),
			DisableGasPriceCheck: true,
			OCCWorkers:           runtime.GOMAXPROCS(0),
			ParseWorkers:         runtime.GOMAXPROCS(0),
			RejectUnappliableTxs: true,
			BlockResultPoolSize:  1,
		},
			evmonly.WithStorageManager(a.storage, a.changeSetEncoder),
			evmonly.WithMissingAccountState(evmOnlyFundedState{}),
			evmonly.WithStoreIndependentBlockChangeSetEncoder(a.encodeCursorChangeSet),
		)
		*slot = utils.Some(executor)
		a.settler.Store(utils.Some(executor))
	}
}

func nrCPU() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Sec+ru.Stime.Sec) + float64(ru.Utime.Usec+ru.Stime.Usec)/1e6
}

func nrMaxRSSMB() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	if runtime.GOOS == "darwin" {
		return float64(ru.Maxrss) / (1 << 20)
	}
	return float64(ru.Maxrss) / (1 << 10)
}

func nrPhases(reader *sdkmetric.ManualReader) map[string]float64 {
	var rm metricdata.ResourceMetrics
	_ = reader.Collect(context.Background(), &rm)
	out := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			d, ok := m.Data.(metricdata.Sum[float64])
			if !ok {
				continue
			}
			for _, dp := range d.DataPoints {
				var labels []string
				for _, kv := range dp.Attributes.ToSlice() {
					labels = append(labels, string(kv.Key)+"="+kv.Value.Emit())
				}
				out[m.Name+"{"+strings.Join(labels, ",")+"}"] += dp.Value
			}
		}
	}
	return out
}

func nrEnvInt(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func TestNodeRealWindows(t *testing.T) {
	dir := os.Getenv("NR_DIR")
	if dir == "" {
		t.Skip("NR_DIR unset")
	}
	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	var starts []uint64
	for _, s := range strings.Split(os.Getenv("NR_STARTS"), ",") {
		v, err := strconv.ParseUint(strings.TrimSpace(s), 10, 64)
		require.NoError(t, err)
		starts = append(starts, v)
	}
	// NR_TXS is one count for every window, or one per window in NR_STARTS's order.
	counts := strings.Split(os.Getenv("NR_TXS"), ",")
	txsFor := func(i int) int {
		if len(counts) == len(starts) {
			v, err := strconv.Atoi(strings.TrimSpace(counts[i]))
			require.NoError(t, err)
			return v
		}
		return nrEnvInt("NR_TXS", 2000)
	}
	reps := nrEnvInt("NR_REPS", 3)
	scratch := os.Getenv("NR_SCRATCH")
	if scratch == "" {
		scratch = t.TempDir()
	}
	mixN := nrEnvInt("NR_MIX_N", 0)
	for i, start := range starts {
		var w *nrWindow
		if mixN > 0 {
			// A mix of 0 still loads one transaction for the window's timestamp and start state, then drops it.
			s := txsFor(i)
			w = nrLoad(t, dir, start, max(s, 1))
			w.raw, w.hashes, w.senders, w.status, w.gas = w.raw[:s], w.hashes[:s], w.senders[:s], w.status[:s], w.gas[:s]
			nrMix(t, w, mixN, uint64(nrEnvInt("NR_CHAINID", 1))) //nolint:gosec // chain ids are positive
		} else {
			w = nrLoad(t, dir, start, txsFor(i))
		}
		for rep := range reps {
			home, err := os.MkdirTemp(scratch, "nr-")
			require.NoError(t, err)
			storageCfg, err := evmonly.NewValidatorStorageConfig(home, true)
			require.NoError(t, err)
			storageCtx, cancel := context.WithCancel(context.Background())
			storage, err := bootstrap.NewGigaStorageManager(storageCtx, storageCfg)
			require.NoError(t, err)
			encoder := evmonly.NewFlatKVChangeSetEncoder(storage.SC())
			genesis, err := encoder(w.genesis)
			require.NoError(t, err)
			require.NoError(t, storage.StateDB().CommitStateChanges(1, genesis))
			inner, err := NewEVMOnlyApplication(uint64(nrEnvInt("NR_CHAINID", 1)), nil, storage, encoder)
			require.NoError(t, err)
			app := inner.(*evmOnlyApplication)
			_, err = app.InitChain(&abci.RequestInitChain{
				InitialHeight:   2,
				ConsensusParams: &tmproto.ConsensusParams{Block: &tmproto.BlockParams{MaxGas: 10_000_000_000}},
			})
			require.NoError(t, err)
			nrInstallUncheckedExecutor(t, app)
			senders := os.Getenv("NR_SENDERS")
			var senderKeys [][]byte
			switch senders {
			case "own":
				for i, h := range w.hashes {
					app.rememberSender(h, w.senders[i])
				}
			case "foreign":
				senderKeys = nrForeignSenderKeys(t, app, w)
			default:
				t.Fatalf("NR_SENDERS must be own or foreign, got %q", senders)
			}
			req := &abci.RequestFinalizeBlock{
				Txs:    w.raw,
				Hash:   crypto.Keccak256([]byte("nodereal-" + w.name)),
				Header: &tmproto.Header{Height: 2, Time: time.Unix(int64(w.time), 0).UTC()}, //nolint:gosec // mainnet timestamps fit int64
			}
			if senders == "foreign" {
				nrCarrySenderKeys(req, senderKeys)
			}
			runtime.GC()
			ph0 := nrPhases(reader)
			ctx := context.Background()
			// NRMARK lines bracket the whole measured application path so the wrapper independently witnesses it.
			fmt.Printf("NRMARK begin %s rep=%d\n", w.name, rep)
			c0, t0 := nrCPU(), time.Now()
			require.NoError(t, app.PrepareBlock(ctx, req))
			c1, t1 := nrCPU(), time.Now()
			resp, err := app.FinalizeBlock(ctx, req)
			require.NoError(t, err)
			_, err = app.Commit(ctx)
			require.NoError(t, err)
			t2 := time.Now()
			fmt.Printf("NRMARK end %s rep=%d\n", w.name, rep)
			c2 := nrCPU()
			require.NoError(t, app.AwaitCommits())
			t3, c3 := time.Now(), nrCPU()
			ph1 := nrPhases(reader)

			ok, failed, match := 0, 0, 0
			var gasTotal int64
			digest := sha256.New()
			digest.Write(resp.AppHash)
			for i, r := range resp.TxResults {
				gasTotal += r.GasUsed
				fmt.Fprintf(digest, "%d:%s;", r.GasUsed, r.Log)
				st := uint64(0)
				if r.Log == "" {
					ok++
					st = 1
				} else {
					failed++
				}
				if st == w.status[i] && uint64(r.GasUsed) == w.gas[i] { //nolint:gosec // gas is non-negative
					match++
				}
			}
			ltHash := "unreachable"
			if sc := storage.SC(); sc != nil {
				if err := sc.FlushHashes(); err == nil {
					if h, err := sc.RegisterHashListener(nil); err == nil {
						sum := h.Global.Checksum()
						ltHash = hex.EncodeToString(sum[:])
					}
				}
			}
			rdb := storage.ReceiptDB()
			deadline := time.Now().Add(2 * time.Minute)
			for rdb.LatestVersion() < 2 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			rctx := sdk.NewContext(nil, tmproto.Header{Height: 2}, false).WithContext(context.Background())
			rcptFound, rcptOK := 0, 0
			for _, h := range w.hashes {
				if r, err := rdb.GetReceipt(rctx, h); err == nil && r != nil {
					rcptFound++
					if r.Status == uint32(ethtypes.ReceiptStatusSuccessful) {
						rcptOK++
					}
				}
			}
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			fmt.Printf("NR %s rep=%d procs=%d txs=%d blobs_dropped=%d accts=%d slots=%d prepare_ms=%.3f finalize_ms=%.3f await_commit_ms=%.3f cpu_us_per_tx_finalize=%.1f cpu_us_per_tx_block=%.1f ok=%d failed=%d mainnet_match=%d gas=%d receipts=%d receipts_ok=%d heap_inuse_mb=%.0f peak_rss_mb=%.0f apphash=%x lthash=%s digest=%s\n",
				w.name, rep, runtime.GOMAXPROCS(0), len(w.raw), w.blobs, w.accts, w.slots,
				t1.Sub(t0).Seconds()*1000, t2.Sub(t1).Seconds()*1000, t3.Sub(t2).Seconds()*1000,
				1e6*(c2-c1)/float64(len(w.raw)), 1e6*(c3-c0)/float64(len(w.raw)),
				ok, failed, match, gasTotal, rcptFound, rcptOK, float64(ms.HeapInuse)/(1<<20), nrMaxRSSMB(),
				resp.AppHash, ltHash, hex.EncodeToString(digest.Sum(nil))[:16])
			names := make([]string, 0, len(ph1))
			for k := range ph1 {
				names = append(names, k)
			}
			sort.Strings(names)
			for _, k := range names {
				if d := ph1[k] - ph0[k]; d > 0 && strings.Contains(k, "phase") {
					fmt.Printf("NRPHASE %s rep=%d %s ms=%.3f\n", w.name, rep, k, d*1000)
				}
			}
			require.NoError(t, storage.Close())
			cancel()
			require.NoError(t, os.RemoveAll(home))
		}
	}
}
