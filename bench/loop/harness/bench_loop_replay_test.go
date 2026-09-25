//go:build benchharness

package p2p_test

// NODE1: one validator's Autobahn EVM-only node in process with the real evmonlyapp (Giga executor,
// FlatKV store, receipt store), fed presigned ERC-20 transfers, read at its execute loop.
//
// Benchmark harness for sei-tendermint/internal/p2p, compiled only with -tags benchharness. It is an external test package (p2p_test)
// because evmonlyapp transitively imports p2p, so package p2p itself cannot import it.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	rtmetrics "runtime/metrics"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/common"
	ethcore "github.com/ethereum/go-ethereum/core"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/prometheus/client_golang/prometheus"
	dbm "github.com/tendermint/tm-db"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"golang.org/x/time/rate"

	loadoffline "github.com/sei-protocol/sei-load/generator/offline"

	"github.com/sei-protocol/sei-chain/giga/evmonly"
	sdk "github.com/sei-protocol/sei-chain/sei-cosmos/types"
	"github.com/sei-protocol/sei-chain/sei-db/bootstrap"
	seidbproto "github.com/sei-protocol/sei-chain/sei-db/proto"
	abci "github.com/sei-protocol/sei-chain/sei-tendermint/abci/types"
	atypes "github.com/sei-protocol/sei-chain/sei-tendermint/autobahn/types"
	tmconfig "github.com/sei-protocol/sei-chain/sei-tendermint/config"
	tmcrypto "github.com/sei-protocol/sei-chain/sei-tendermint/crypto"
	"github.com/sei-protocol/sei-chain/sei-tendermint/crypto/ed25519"
	"github.com/sei-protocol/sei-chain/sei-tendermint/internal/autobahn/producer"
	"github.com/sei-protocol/sei-chain/sei-tendermint/internal/evmonlyapp"
	"github.com/sei-protocol/sei-chain/sei-tendermint/internal/p2p"
	"github.com/sei-protocol/sei-chain/sei-tendermint/internal/p2p/conn"
	"github.com/sei-protocol/sei-chain/sei-tendermint/internal/proxy"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/require"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/scope"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/tcp"
	tmproto "github.com/sei-protocol/sei-chain/sei-tendermint/proto/tendermint/types"
	"github.com/sei-protocol/sei-chain/sei-tendermint/types"
)

// ---------------------------------------------------------------------------------------------
// configuration

type node1Config struct {
	txsPerBlock     int
	blocks          int
	accounts        int
	recipients      string // pooled | fresh
	admit           string // self: the real CheckTx admits every tx | foreign: admission data without CheckTx, so PrepareBlock recovers every sender | carried: the response of the tree's real CheckTx on a second application (the admitting validator), so the node carries whatever that validator reports and recovers nothing itself
	receipts        bool
	canonical       bool // canonical block hash + time in the requests the app sees
	blockIntervalMs int
	precheckAhead   int
	precheckWorkers int
	maxGas          int64
	workload        string // erc20-transfer | transfer (the loadtest's scenarios)
	gaugeMs         int    // the gauge sampler's period, run over the window only
	dir             string
	keepDir         bool
}

func node1EnvInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

func node1EnvStr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func readNode1Config(t *testing.T) node1Config {
	cfg := node1Config{
		txsPerBlock:     node1EnvInt("NODE1_TXS", 2000),
		blocks:          node1EnvInt("NODE1_BLOCKS", 200),
		accounts:        node1EnvInt("NODE1_ACCOUNTS", 4000),
		recipients:      node1EnvStr("NODE1_RECIPIENTS", "pooled"),
		admit:           node1EnvStr("NODE1_ADMIT", "self"),
		receipts:        node1EnvStr("NODE1_RECEIPTS", "on") == "on",
		canonical:       node1EnvStr("NODE1_CANONICAL", "on") == "on",
		blockIntervalMs: node1EnvInt("NODE1_BLOCK_INTERVAL_MS", 3_600_000),
		precheckAhead:   node1EnvInt("NODE1_PRECHECK_AHEAD", 250_000),
		precheckWorkers: node1EnvInt("NODE1_PRECHECK_WORKERS", runtime.GOMAXPROCS(0)),
		maxGas:          int64(node1EnvInt("NODE1_MAX_GAS", 10_000_000_000)),
		workload:        node1EnvStr("NODE1_WORKLOAD", loadoffline.ERC20Transfer),
		gaugeMs:         node1EnvInt("NODE1_GAUGE_MS", 20),
	}
	if cfg.gaugeMs <= 0 {
		t.Fatalf("NODE1_GAUGE_MS must be positive")
	}
	if cfg.txsPerBlock <= 0 || uint64(cfg.txsPerBlock) > atypes.MaxTxsPerBlock {
		t.Fatalf("NODE1_TXS must be in 1..%d", atypes.MaxTxsPerBlock)
	}
	if cfg.recipients != "pooled" && cfg.recipients != "fresh" {
		t.Fatalf("NODE1_RECIPIENTS must be pooled or fresh")
	}
	switch cfg.workload {
	case loadoffline.ERC20Transfer:
		node1TxGasLimit = 100_000 // loadtest defaultERC20TxGasLimit
	case loadoffline.Transfer:
		node1TxGasLimit = 21_000 // loadtest defaultTxGasLimit
	default:
		t.Fatalf("NODE1_WORKLOAD must be erc20-transfer or transfer")
	}
	if os.Getenv("NODE1_REPLAY_TOUCH") != "" {
		if os.Getenv("NODE1_REPLAY") != "" {
			t.Fatalf("NODE1_REPLAY and NODE1_REPLAY_TOUCH are exclusive")
		}
		node1TxGasLimit = node1TouchGasLimit // every tx, the sentinel included, and its admission's GasWanted
	} else if os.Getenv("NODE1_REPLAY_TOUCH_CODE") != "" {
		t.Fatalf("NODE1_REPLAY_TOUCH_CODE needs NODE1_REPLAY_TOUCH")
	}
	if os.Getenv("NODE1_SWAP") != "" {
		if os.Getenv("NODE1_REPLAY") != "" || os.Getenv("NODE1_REPLAY_TOUCH") != "" || node1EnvInt("NODE1_HOT_PERMILLE", 0) != 0 {
			t.Fatalf("NODE1_SWAP excludes NODE1_REPLAY, NODE1_REPLAY_TOUCH and NODE1_HOT_PERMILLE")
		}
		node1TxGasLimit = node1SwapGasLimit // every tx, the sentinel included, and its admission's GasWanted
	} else if os.Getenv("NODE1_SWAP_POOLS") != "" || os.Getenv("NODE1_SWAP_HOT_PERMILLE") != "" || os.Getenv("NODE1_SWAP_NATIVE") != "" || os.Getenv("NODE1_SWAP_NATIVE_OUT_PERMILLE") != "" {
		t.Fatalf("NODE1_SWAP_POOLS, NODE1_SWAP_HOT_PERMILLE, NODE1_SWAP_NATIVE and NODE1_SWAP_NATIVE_OUT_PERMILLE need NODE1_SWAP")
	}
	if cfg.admit != "self" && cfg.admit != "foreign" && cfg.admit != "carried" {
		t.Fatalf("NODE1_ADMIT must be self, foreign or carried")
	}
	if cfg.accounts < 2*cfg.txsPerBlock {
		// A block never draws one sender twice, and a pooled recipient half a pool away falls
		// outside the block that paid it (the loadtest's rule).
		t.Fatalf("NODE1_ACCOUNTS must be at least 2*NODE1_TXS")
	}
	// The app's CheckTx sender cache clears itself at 1<<18 entries.
	cfg.precheckAhead = min(cfg.precheckAhead, 250_000)
	cfg.dir = os.Getenv("NODE1_DIR")
	cfg.keepDir = cfg.dir != ""
	if cfg.dir == "" {
		cfg.dir = t.TempDir()
	}
	abs, err := filepath.Abs(cfg.dir)
	require.NoError(t, err)
	cfg.dir = abs
	return cfg
}

// ---------------------------------------------------------------------------------------------
// workload: the evmonly-loadtest's erc20-transfer or transfer scenario, presigned

const node1GasPrice = 1_000_000_000 // the app's minimum effective gas price; base fee is 0

// node1TxGasLimit is the loadtest's default gas limit for the configured workload.
var node1TxGasLimit uint64 = 100_000

var (
	node1ERC20         = common.HexToAddress("0x000000000000000000000000000000000000e20c") // loadtest default
	node1SenderBalance = big.NewInt(1_000_000_000_000_000_000)                             // loadtest default, wei
	node1TransferValue = big.NewInt(1)                                                     // loadtest default, token units
	node1TokenBalance  = new(big.Int).Lsh(big.NewInt(1), 64)                               // token units per sender
)

type node1Workload struct {
	txs      [][]byte
	sentinel []byte
	addrs    []common.Address // pool accounts, then the sentinel's sender
	pubs     [][]byte         // compressed public keys, aligned with addrs
	genesis  evmonly.StateChangeSet
	presignS float64
	account  []int32  // replay: each tx's pool account; nil when account is i mod the pool
	nonce    []uint64 // replay: each tx's nonce
}

// node1Key is the loadtest's DeterministicPrivateKey.
func node1Key(index uint64) *ecdsa.PrivateKey {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], index)
	for attempt := uint64(0); ; attempt++ {
		binary.BigEndian.PutUint64(buf[8:], attempt)
		key, err := crypto.ToECDSA(crypto.Keccak256([]byte("sei-evmonly-loadtest-sender"), buf[:]))
		if err == nil {
			return key
		}
	}
}

func node1FreshRecipient(i uint64) common.Address {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	h := crypto.Keccak256Hash([]byte("node1-fresh-recipient"), buf[:])
	return common.BytesToAddress(h[12:])
}

func buildNode1Workload(t *testing.T, cfg node1Config) *node1Workload {
	start := time.Now()
	scenario, err := loadoffline.NewScenario(cfg.workload, loadoffline.Config{
		ChainID:       new(big.Int).SetUint64(tmconfig.AutobahnEVMOnlyChainID),
		GasPrice:      big.NewInt(node1GasPrice),
		SenderBalance: node1SenderBalance,
		TransferValue: node1TransferValue,
		GasLimit:      node1TxGasLimit,
		ERC20Contract: node1ERC20,
	})
	require.NoError(t, err)
	// Accounts 0..A-1 are the pool; account A sends the sentinel.
	nAcc := cfg.accounts + 1
	keys := make([]*ecdsa.PrivateKey, nAcc)
	addrs := make([]common.Address, nAcc)
	pubs := make([][]byte, nAcc)
	parallelFor(nAcc, func(i int) {
		keys[i] = node1Key(uint64(i))
		addrs[i] = crypto.PubkeyToAddress(keys[i].PublicKey)
		pubs[i] = crypto.CompressPubkey(&keys[i].PublicKey)
	})
	gw := &node1GenesisWriter{balances: map[common.Address]*big.Int{}, code: map[common.Address][]byte{}, storage: map[common.Address]map[common.Hash]common.Hash{}}
	require.NoError(t, scenario.SetupGenesis(gw))
	n := cfg.txsPerBlock * cfg.blocks
	touch := node1LoadTouch(t, cfg.txsPerBlock, n, cfg.accounts)
	if touch != nil {
		touch.seedGenesis(gw)
	}
	swap := node1LoadSwap(t, cfg, keys, addrs)
	if swap != nil {
		swap.seedGenesis(gw)
	}
	senderBalance := node1SenderBalance
	if os.Getenv("NODE1_REPLAY") != "" || touch != nil {
		senderBalance = new(big.Int).Mul(node1SenderBalance, big.NewInt(1000))
	}
	for _, a := range addrs {
		gw.SetBalance(a, senderBalance)
		if cfg.workload == loadoffline.ERC20Transfer {
			gw.SetState(node1ERC20, loadoffline.ERC20BalanceSlot(a), common.BigToHash(node1TokenBalance))
		}
	}
	txs := make([][]byte, n)
	A := uint64(cfg.accounts)
	var signErr atomic.Pointer[error]
	hotPermille := uint64(node1EnvInt("NODE1_HOT_PERMILLE", 0))
	hotRecipients := make([]common.Address, max(1, node1EnvInt("NODE1_HOT_KEYS", 1)))
	for k := range hotRecipients {
		seed := []byte("node1-hot-recipient")
		if k > 0 {
			seed = append(seed, byte(k))
		}
		hotRecipients[k] = common.BytesToAddress(crypto.Keccak256(seed))
	}
	fmt.Printf("NODE1 hot permille=%d keys=%d first=%s\n", hotPermille, len(hotRecipients), hotRecipients[0].Hex())
	replay := node1LoadReplay(t, cfg.txsPerBlock, n, cfg.accounts)
	if touch != nil {
		replay = touch.plan
	}
	parallelFor(n, func(i int) {
		idx := uint64(i)
		account, nonce := idx%A, idx/A // the loadtest's senderFor over a bounded pool
		var recipient common.Address
		if cfg.recipients == "pooled" {
			recipient = addrs[(account+A/2)%A] // the loadtest's pooled recipient, half a pool away
		} else {
			recipient = node1FreshRecipient(idx)
		}
		if hotPermille > 0 && (idx*2654435761)%1000 < hotPermille {
			recipient = hotRecipients[(idx*40503/7)%uint64(len(hotRecipients))]
		}
		if replay != nil {
			account, nonce = uint64(replay.account[i]), replay.nonce[i]
			recipient = addrs[(account+A/2)%A]
			if h := replay.holder[i]; h >= 0 {
				recipient = node1ReplayHolder(h)
			}
		}
		var signed *ethtypes.Transaction
		var err error
		if touch != nil {
			signed, err = touch.buildTransaction(keys[account], nonce, i)
		} else if swap != nil {
			signed, err = swap.buildTransaction(keys[account], addrs[account], nonce, i)
		} else {
			signed, err = scenario.BuildTransaction(keys[account], nonce, recipient)
		}
		if err == nil {
			txs[i], err = signed.MarshalBinary()
		}
		if err != nil {
			signErr.Store(&err)
		}
	})
	if e := signErr.Load(); e != nil {
		t.Fatalf("presign: %v", *e)
	}
	signed, err := scenario.BuildTransaction(keys[cfg.accounts], 0, addrs[0])
	require.NoError(t, err)
	sentinel, err := signed.MarshalBinary()
	require.NoError(t, err)
	wl := &node1Workload{txs: txs, sentinel: sentinel, addrs: addrs, pubs: pubs, genesis: gw.changeSet(), presignS: time.Since(start).Seconds()}
	if replay != nil {
		wl.account, wl.nonce = replay.account, replay.nonce
	}
	return wl
}

func parallelFor(n int, f func(i int)) {
	workers := runtime.GOMAXPROCS(0)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				f(i)
			}
		}()
	}
	wg.Wait()
}

// node1GenesisWriter collects the scenario's genesis state: balances, code, and each contract's storage.
type node1GenesisWriter struct {
	balances map[common.Address]*big.Int
	code     map[common.Address][]byte
	storage  map[common.Address]map[common.Hash]common.Hash
	nonces   map[common.Address]uint64 // only the swap workload's contracts set a nonce
}

func (w *node1GenesisWriter) SetBalance(a common.Address, b *big.Int) {
	w.balances[a] = new(big.Int).Set(b)
}
func (w *node1GenesisWriter) SetCode(a common.Address, c []byte) { w.code[a] = bytes.Clone(c) }
func (w *node1GenesisWriter) SetNonce(a common.Address, n uint64) {
	if w.nonces == nil {
		w.nonces = map[common.Address]uint64{}
	}
	w.nonces[a] = n
}
func (w *node1GenesisWriter) SetState(a common.Address, k, v common.Hash) {
	s, ok := w.storage[a]
	if !ok {
		s = map[common.Hash]common.Hash{}
		w.storage[a] = s
	}
	if v == (common.Hash{}) {
		delete(s, k) // the loadtest's generatedState drops zero values the same way
		return
	}
	s[k] = v
}

// changeSet orders the genesis state the way the loadtest's generatedState does: addresses ascending, and
// each address's storage slots ascending.
func (w *node1GenesisWriter) changeSet() evmonly.StateChangeSet {
	var cs evmonly.StateChangeSet
	seen := make(map[common.Address]bool, len(w.balances)+len(w.code)+len(w.storage))
	addrs := make([]common.Address, 0, len(w.balances)+len(w.code)+len(w.storage))
	add := func(a common.Address) {
		if !seen[a] {
			seen[a] = true
			addrs = append(addrs, a)
		}
	}
	for a := range w.balances {
		add(a)
	}
	for a := range w.code {
		add(a)
	}
	for a := range w.storage {
		add(a)
	}
	for a := range w.nonces {
		add(a)
	}
	sort.Slice(addrs, func(i, j int) bool { return bytes.Compare(addrs[i][:], addrs[j][:]) < 0 })
	for _, a := range addrs {
		if b, ok := w.balances[a]; ok {
			cs.Balances = append(cs.Balances, evmonly.BalanceChange{Address: a, Balance: b})
		}
		if n, ok := w.nonces[a]; ok {
			cs.Nonces = append(cs.Nonces, evmonly.NonceChange{Address: a, Nonce: n})
		}
		if c, ok := w.code[a]; ok {
			cs.Code = append(cs.Code, evmonly.CodeChange{Address: a, Code: c})
		}
		slots := make([]common.Hash, 0, len(w.storage[a]))
		for k := range w.storage[a] {
			slots = append(slots, k)
		}
		sort.Slice(slots, func(i, j int) bool { return bytes.Compare(slots[i][:], slots[j][:]) < 0 })
		for _, k := range slots {
			cs.Storage = append(cs.Storage, evmonly.StorageChange{Address: a, Key: k, Value: w.storage[a][k]})
		}
	}
	return cs
}

// ---------------------------------------------------------------------------------------------
// application wrapper: records every block, serves prechecked CheckTx responses

const node1Chunk = 256

type node1Block struct {
	height  int64
	appHash []byte
	txs     int
	ok      int
	failed  int
	gasUsed int64
	done    time.Time
	// feedAhead is how many transactions the inserter had handed the producer beyond this block when
	// its FinalizeBlock began; feedDone reports that the inserter had already handed over every one.
	feedAhead int64
	feedDone  bool
}

type node1App struct {
	abci.Application // the real evmOnlyApplication
	cfg              node1Config
	wl               *node1Workload

	// precheck
	index        map[string]int32
	resp         []*abci.ResponseCheckTxV2
	chunkDone    []chan struct{}
	nextChunk    atomic.Int64
	prechecked   atomic.Int64
	precheckErr  atomic.Pointer[string]
	mismatches   atomic.Int64
	keyChecks    atomic.Int64 // self: admission responses whose public-key field was compared
	keyMismatch  atomic.Int64 // self: ...whose key was not the sender's
	innerChecks  atomic.Int64
	admitter     abci.Application // carried: a second application at genesis whose CheckTx admits every tx
	closeAdmit   func()
	admitChecks  atomic.Int64 // carried: admitter CheckTx calls
	carriedBytes atomic.Int64 // carried: bytes in the admitter's responses' []byte fields other than SeiSenderAddress
	reader       *sdkmetric.ManualReader
	startMetrics metricdata.ResourceMetrics
	cacheWaits   atomic.Int64
	cacheWaitNs  atomic.Int64
	uncached     atomic.Int64
	finalizedTxs utils.AtomicSend[int64]

	// feed: the inserter's progress, and the fill that runs before the first block executes
	inserted   atomic.Int64
	insertDone atomic.Bool
	fillS      float64
	fillTxs    int64
	windowOpen chan struct{}

	// feed diagnostics: app nonce reads on the insert path, and the inserter's own InsertTx wall
	nonceCalls atomic.Int64
	nonceNs    atomic.Int64
	nonceMaxNs atomic.Int64
	insCalls   atomic.Int64
	insNs      atomic.Int64
	insSlow    atomic.Int64 // calls above 50 us
	insSlowNs  atomic.Int64
	insMaxNs   atomic.Int64
	insGapNs   atomic.Int64 // inserter wall between InsertTx calls

	// nonce table: each pool account's executed nonce, advanced as FinalizeBlock returns
	nonceTable   bool
	addrIdx      map[common.Address]int
	execNonce    []atomic.Uint64
	nonceChecked atomic.Int64 // table answers compared with the app's own read
	nonceBehind  atomic.Int64 // ...where the table was below the app (a block in flight)
	nonceAhead   atomic.Int64 // ...where the table was above the app: never valid

	mu            sync.Mutex
	blocks        []node1Block
	firstFailure  string
	sentinelSeen  bool
	windowStarted atomic.Bool
	startRusage   syscall.Rusage
	startGC       uint64
	precheckAtWin int64
}

func newNode1App(inner abci.Application, cfg node1Config, wl *node1Workload) *node1App {
	n := len(wl.txs) + 1
	a := &node1App{
		Application:  inner,
		cfg:          cfg,
		wl:           wl,
		index:        make(map[string]int32, n),
		resp:         make([]*abci.ResponseCheckTxV2, n),
		chunkDone:    make([]chan struct{}, (n+node1Chunk-1)/node1Chunk),
		finalizedTxs: utils.NewAtomicSend[int64](0),
		windowOpen:   make(chan struct{}),
	}
	for i := range a.chunkDone {
		a.chunkDone[i] = make(chan struct{})
	}
	for i := range n {
		tx := a.tx(i)
		a.index[unsafe.String(unsafe.SliceData(tx), len(tx))] = int32(i)
	}
	a.nonceTable = os.Getenv("NODE1_NONCE") == "table"
	a.addrIdx = make(map[common.Address]int, len(wl.addrs))
	for i, addr := range wl.addrs {
		a.addrIdx[addr] = i
	}
	a.execNonce = make([]atomic.Uint64, len(wl.addrs))
	return a
}

func (a *node1App) tx(i int) []byte {
	if i == len(a.wl.txs) {
		return a.wl.sentinel
	}
	return a.wl.txs[i]
}

// runPrecheck runs the real CheckTx on every tx in order-claimed chunks, never more than
// precheckAhead ahead of the executed txs, so the app's sender cache never overflows.
func (a *node1App) runPrecheck(ctx context.Context) error {
	n := len(a.wl.txs) + 1
	for {
		c := int(a.nextChunk.Add(1) - 1)
		if c >= len(a.chunkDone) {
			return nil
		}
		lo, hi := c*node1Chunk, min((c+1)*node1Chunk, n)
		ahead := int64(a.cfg.precheckAhead)
		if _, err := a.finalizedTxs.Wait(ctx, func(f int64) bool { return int64(hi) <= f+ahead }); err != nil {
			return nil
		}
		for i := lo; i < hi; i++ {
			synth := a.admission(i)
			if a.cfg.admit == "carried" {
				a.admitChecks.Add(1)
				r := a.admitter.CheckTx(ctx, &abci.RequestCheckTxV2{Tx: a.tx(i)})
				if !r.IsOK() {
					msg := fmt.Sprintf("admitter tx %d: code=%d log=%s", i, r.Code, r.Log)
					a.precheckErr.CompareAndSwap(nil, &msg)
				}
				if !sameAdmission(r, synth) {
					a.mismatches.Add(1)
				}
				a.carriedBytes.Add(int64(carriedLen(r)))
				a.resp[i] = r
				continue
			}
			if a.cfg.admit != "self" {
				a.resp[i] = synth
				continue
			}
			a.innerChecks.Add(1)
			r := a.Application.CheckTx(ctx, &abci.RequestCheckTxV2{Tx: a.tx(i)})
			if !r.IsOK() {
				msg := fmt.Sprintf("tx %d: code=%d log=%s", i, r.Code, r.Log)
				a.precheckErr.CompareAndSwap(nil, &msg)
			}
			if !sameAdmission(r, synth) {
				a.mismatches.Add(1)
			}
			if carriedFieldPresent() {
				a.keyChecks.Add(1)
				if !sameSenderKey(carriedKey(r), a.wl.pubs[a.accountOf(i)]) {
					a.keyMismatch.Add(1)
				}
			}
			a.resp[i] = r
		}
		a.prechecked.Add(int64(hi - lo))
		close(a.chunkDone[c])
	}
}

// admission is what CheckTx returns for tx i, built from the workload without recovering its
// sender: the data a validator holds for a transaction another validator admitted.
func (a *node1App) admission(i int) *abci.ResponseCheckTxV2 {
	A := a.cfg.accounts
	account, nonce := a.accountOf(i), uint64(0)
	if i < len(a.wl.txs) {
		nonce = uint64(i / A)
		if a.wl.nonce != nil {
			nonce = a.wl.nonce[i]
		}
	}
	sender := a.wl.addrs[account]
	r := &abci.ResponseCheckTxV2{
		ResponseCheckTx: &abci.ResponseCheckTx{
			Code:         abci.CodeTypeOK,
			GasWanted:    int64(node1TxGasLimit),
			GasEstimated: int64(node1TxGasLimit),
		},
		IsEVM:            true,
		EVMNonce:         nonce,
		EVMHash:          crypto.Keccak256Hash(a.tx(i)),
		EVMSenderAddress: sender,
		SeiSenderAddress: append([]byte(nil), sender[:]...),
	}
	return r
}

// carriedLen is the number of bytes in r's []byte fields other than SeiSenderAddress: what the
// admitting validator's CheckTx reports beyond the fields every tree carries.
func carriedLen(r *abci.ResponseCheckTxV2) int {
	v := reflect.ValueOf(r).Elem()
	n := 0
	for i := range v.NumField() {
		f := v.Type().Field(i)
		if f.Name == "SeiSenderAddress" || f.Type != reflect.TypeOf([]byte(nil)) {
			continue
		}
		n += v.Field(i).Len()
	}
	return n
}

// accountOf is the pool index of tx i's sender; the sentinel's sender follows the pool.
func (a *node1App) accountOf(i int) int {
	if i < len(a.wl.txs) {
		if a.wl.account != nil {
			return int(a.wl.account[i])
		}
		return i % a.cfg.accounts
	}
	return a.cfg.accounts
}

// carriedKey returns r's public-key field, or nil on a tree without it.
func carriedKey(r *abci.ResponseCheckTxV2) []byte {
	if !carriedFieldPresent() {
		return nil
	}
	return reflect.ValueOf(r).Elem().FieldByName(carriedKeyField).Bytes()
}

// carriedKeyField is the admission response's public-key field, when the tree under test has one.
const carriedKeyField = "EVMSenderPubKey"

// carriedFieldPresent reports whether this tree's admission response can carry a sender's public key.
func carriedFieldPresent() bool {
	f, ok := reflect.TypeOf(abci.ResponseCheckTxV2{}).FieldByName(carriedKeyField)
	return ok && f.Type == reflect.TypeOf([]byte(nil))
}

// sameSenderKey reports whether key, a SEC1 secp256k1 public key in compressed (33-byte) or uncompressed (65-byte)
// form, is the key whose compressed form is compressed.
func sameSenderKey(key, compressed []byte) bool {
	switch len(key) {
	case 33:
		return bytes.Equal(key, compressed)
	case 65:
		k, err := crypto.UnmarshalPubkey(key)
		return err == nil && bytes.Equal(crypto.CompressPubkey(k), compressed)
	}
	return false
}

func sameAdmission(r, s *abci.ResponseCheckTxV2) bool {
	return r.Code == s.Code && r.GasWanted == s.GasWanted && r.GasEstimated == s.GasEstimated &&
		r.IsEVM == s.IsEVM && r.EVMNonce == s.EVMNonce && r.EVMHash == s.EVMHash &&
		r.EVMSenderAddress == s.EVMSenderAddress && bytes.Equal(r.SeiSenderAddress, s.SeiSenderAddress)
}

func (a *node1App) CheckTx(ctx context.Context, req *abci.RequestCheckTxV2) *abci.ResponseCheckTxV2 {
	i, ok := a.index[string(req.Tx)]
	if !ok {
		a.uncached.Add(1)
		a.innerChecks.Add(1)
		return a.Application.CheckTx(ctx, req)
	}
	done := a.chunkDone[int(i)/node1Chunk]
	select {
	case <-done:
	default:
		t0 := time.Now()
		a.cacheWaits.Add(1)
		select {
		case <-done:
		case <-ctx.Done():
			return &abci.ResponseCheckTxV2{ResponseCheckTx: &abci.ResponseCheckTx{Code: 1, Log: ctx.Err().Error()}}
		}
		a.cacheWaitNs.Add(int64(time.Since(t0)))
	}
	return a.resp[i]
}

// canonical replaces the block hash and time, which carry the producer's wall clock, with values
// derived from the height, so that a block's app hash depends only on its transactions.
func (a *node1App) canonical(req *abci.RequestFinalizeBlock) *abci.RequestFinalizeBlock {
	if !a.cfg.canonical {
		return req
	}
	c := *req
	h := *req.Header
	h.Time = time.Unix(1_700_000_000+h.Height, 0).UTC()
	c.Header = &h
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(h.Height))
	c.Hash = crypto.Keccak256([]byte("node1-block"), buf[:])
	return &c
}

func (a *node1App) PrepareBlock(ctx context.Context, req *abci.RequestFinalizeBlock) error {
	p, ok := a.Application.(interface {
		PrepareBlock(context.Context, *abci.RequestFinalizeBlock) error
	})
	if !ok {
		return nil
	}
	return p.PrepareBlock(ctx, a.canonical(req))
}

func (a *node1App) FinalizeBlock(ctx context.Context, req *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	if !a.windowStarted.Load() {
		a.fillFeed(ctx)
	}
	feedAhead := a.inserted.Load() - a.finalizedTxs.Load() - int64(len(req.Txs))
	feedDone := a.insertDone.Load()
	resp, err := a.Application.FinalizeBlock(ctx, a.canonical(req))
	if err != nil {
		return resp, err
	}
	done := time.Now()
	if !a.windowStarted.Swap(true) {
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &a.startRusage)
		node1ProfStart()
		a.startGC = node1GCCycles()
		a.precheckAtWin = a.prechecked.Load()
		// The window's phase totals are read against this snapshot; its cost falls before the steady span.
		_ = a.reader.Collect(context.Background(), &a.startMetrics)
		close(a.windowOpen)
	}
	b := node1Block{height: req.Header.Height, appHash: bytes.Clone(resp.AppHash), done: done, feedAhead: feedAhead, feedDone: feedDone}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i, r := range resp.TxResults {
		if bytes.Equal(req.Txs[i], a.wl.sentinel) {
			a.sentinelSeen = true
			continue
		}
		b.txs++
		b.gasUsed += r.GasUsed
		// Every result is CodeTypeOK; the EVM failure reason, when there is one, is the log.
		if r.Code == abci.CodeTypeOK && r.Log == "" {
			b.ok++
		} else {
			b.failed++
			if a.firstFailure == "" {
				a.firstFailure = fmt.Sprintf("height=%d idx=%d code=%d log=%q", b.height, i, r.Code, r.Log)
				fmt.Printf("NODE1 FAIL first tx failure %s\n", a.firstFailure)
			}
		}
	}
	a.blocks = append(a.blocks, b)
	for _, tx := range req.Txs {
		if i, ok := a.index[unsafe.String(unsafe.SliceData(tx), len(tx))]; ok {
			a.execNonce[a.accountOf(int(i))].Add(1)
		}
	}
	a.finalizedTxs.Store(a.finalizedTxs.Load() + int64(b.txs))
	return resp, nil
}

// fillFeed holds the first block until the inserter stops advancing, which happens once the producer is
// at capacity, so every later block starts with that backlog already handed over.
func (a *node1App) fillFeed(ctx context.Context) {
	t0 := time.Now()
	last, still := int64(-1), 0
	for still < 5 && !a.insertDone.Load() && ctx.Err() == nil && time.Since(t0) < time.Minute {
		time.Sleep(2 * time.Millisecond)
		n := a.inserted.Load()
		if n > 0 && n == last {
			still++
		} else {
			still = 0
		}
		last = n
	}
	a.fillS = time.Since(t0).Seconds()
	a.fillTxs = a.inserted.Load()
}

// EvmNonce times the app nonce reads the producer makes for senders its mempool does not track.
func (a *node1App) EvmNonce(addr common.Address) uint64 {
	t0 := time.Now()
	var n uint64
	if i, ok := a.addrIdx[addr]; ok && a.nonceTable {
		n = a.execNonce[i].Load()
		if c := a.nonceChecked.Add(1); c%64 == 0 {
			switch real := a.Application.EvmNonce(addr); {
			case n < real:
				a.nonceBehind.Add(1)
			case n > real:
				a.nonceAhead.Add(1)
			}
		}
	} else {
		n = a.Application.EvmNonce(addr)
	}
	d := time.Since(t0).Nanoseconds()
	a.nonceCalls.Add(1)
	a.nonceNs.Add(d)
	for m := a.nonceMaxNs.Load(); d > m && !a.nonceMaxNs.CompareAndSwap(m, d); m = a.nonceMaxNs.Load() {
	}
	return n
}

func (a *node1App) AwaitCommits() error {
	if s, ok := a.Application.(interface{ AwaitCommits() error }); ok {
		return s.AwaitCommits()
	}
	return nil
}

func (a *node1App) EvmCall(ctx context.Context, msg *ethcore.Message) (*ethcore.ExecutionResult, error) {
	return a.Application.(interface {
		EvmCall(context.Context, *ethcore.Message) (*ethcore.ExecutionResult, error)
	}).EvmCall(ctx, msg)
}

func (a *node1App) EvmChainConfig() *params.ChainConfig {
	return a.Application.(interface{ EvmChainConfig() *params.ChainConfig }).EvmChainConfig()
}

func (a *node1App) EvmBaseFee() *big.Int {
	return a.Application.(interface{ EvmBaseFee() *big.Int }).EvmBaseFee()
}

func (a *node1App) EvmGasLimit() uint64 {
	return a.Application.(interface{ EvmGasLimit() uint64 }).EvmGasLimit()
}

func (a *node1App) EvmMinGasPrice() *big.Int {
	return a.Application.(interface{ EvmMinGasPrice() *big.Int }).EvmMinGasPrice()
}

func node1GCCycles() uint64 {
	s := []rtmetrics.Sample{{Name: "/gc/cycles/total:gc-cycles"}}
	rtmetrics.Read(s)
	if s[0].Value.Kind() != rtmetrics.KindUint64 {
		return 0
	}
	return s[0].Value.Uint64()
}

func rusageCPU(r *syscall.Rusage) float64 {
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}

// ---------------------------------------------------------------------------------------------
// the test

// TestNode1 runs one validator's Autobahn EVM-only node (producer, consensus, fsynced LittDB block
// store, pebble hash vault, runExecute) over the real EVM-only application and reports the
// committed throughput at its execute loop.
func TestNode1(t *testing.T) {
	cfg := readNode1Config(t)

	reader := sdkmetric.NewManualReader()
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	ctx := t.Context()
	rng := utils.TestRng()
	_, valKeys := atypes.GenCommittee(rng, 1)
	valKey := valKeys[0]
	nodeKey := p2p.NodeSecretKey(ed25519.TestSecretKey(utils.GenBytes(rng, 32)))
	addr := tcp.TestReserveAddr()
	evmRPC, err := url.Parse(fmt.Sprintf("http://%s:8545", addr.Addr().String()))
	require.NoError(t, err)
	addrs := map[atypes.PublicKey]p2p.GigaNodeAddr{valKey.Public(): {
		Key:      nodeKey.Public(),
		HostPort: tcp.HostPort{Hostname: addr.Addr().String(), Port: addr.Port()},
		EVMRPC:   *evmRPC,
	}}
	consensusParams := types.DefaultConsensusParams()
	consensusParams.Block.MaxGas = cfg.maxGas
	consensusParams.Block.MaxGasWanted = cfg.maxGas
	// Genesis state is committed as version 1 (as the loadtest does), so the chain starts at 2.
	genDoc := &types.GenesisDoc{
		ChainID:         "node1-evmonly",
		InitialHeight:   2,
		ConsensusParams: consensusParams,
		AppState:        json.RawMessage(`{}`),
	}
	require.NoError(t, genDoc.ValidateAndComplete())

	dir := filepath.Join(cfg.dir, "node0")
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.MkdirAll(dir, 0o700))

	wl := buildNode1Workload(t, cfg)
	nTxs := len(wl.txs)

	// Storage as node/setup.go openEVMOnlyStorageManager builds it: validator config rooted at the
	// Autobahn persistent-state dir, the block DB at <dir>/blockdb with fsync forced on.
	storageCfg, err := evmonly.NewValidatorStorageConfig(dir, cfg.receipts)
	require.NoError(t, err)
	blockCfg, err := tmconfig.AutobahnBlockDBConfig{}.LittBlockConfig(filepath.Join(dir, "blockdb"))
	require.NoError(t, err)
	storageCfg.BlockDBConfig = &blockCfg
	storageCtx, cancelStorage := context.WithCancel(context.Background())
	defer cancelStorage()
	manager, err := bootstrap.NewGigaStorageManager(storageCtx, storageCfg)
	require.NoError(t, err)
	managerClosed := false
	defer func() {
		if !managerClosed {
			_ = manager.Close()
		}
	}()
	encoder := evmonly.NewFlatKVChangeSetEncoder(manager.SC())
	genesisChanges, err := encoder(wl.genesis)
	require.NoError(t, err)
	require.NoError(t, manager.StateDB().CommitStateChanges(genDoc.InitialHeight-1, genesisChanges))

	// Validators as node/public.go evmOnlyValidatorUpdates builds them.
	edKey, err := ed25519.PublicKeyFromBytes(valKey.Public().Bytes())
	require.NoError(t, err)
	validators := []abci.ValidatorUpdate{{PubKey: tmcrypto.PubKeyToProto(edKey), Power: 1}}
	inner, err := evmonlyapp.NewEVMOnlyApplication(tmconfig.AutobahnEVMOnlyChainID, validators, manager, encoder)
	require.NoError(t, err)
	app := newNode1App(inner, cfg, wl)
	app.reader = reader
	if cfg.admit == "carried" {
		admitter, closeAdmitter := newNode1Admitter(t, cfg, wl, genDoc, validators)
		defer closeAdmitter()
		app.admitter, app.closeAdmit = admitter, closeAdmitter
	}

	commonCfg := p2p.GigaRouterCommonConfig{
		DialInterval:            100 * time.Millisecond,
		ValidatorAddrs:          addrs,
		PersistentStateDir:      utils.Some(dir),
		App:                     proxy.New(app),
		GenDoc:                  genDoc,
		MaxInboundFullnodePeers: tmconfig.DefaultMaxInboundFullnodePeers,
		EnableEvmProxy:          true,
	}
	dataState, err := p2p.BuildDataState(&commonCfg, manager.BlockStore())
	require.NoError(t, err)
	giga, err := p2p.NewGigaValidatorRouter(&p2p.GigaValidatorConfig{
		GigaRouterCommonConfig: commonCfg,
		ValidatorKey:           valKey,
		ViewTimeout:            func(atypes.View) time.Duration { return time.Hour },
		Producer: &producer.Config{
			MaxGasWantedPerBlock:    genDoc.ConsensusParams.Block.MaxGasWantedUint64(),
			MaxGasEstimatedPerBlock: genDoc.ConsensusParams.Block.MaxGasUint64(),
			MaxTxsPerBlock:          uint64(cfg.txsPerBlock),
			MaxTxsPerSecond:         utils.None[uint64](),
			BlockInterval:           time.Duration(cfg.blockIntervalMs) * time.Millisecond,
			AllowEmptyBlocks:        false,
			MaxPendingInserts:       producer.DefaultMaxPendingInserts,
		},
	}, nodeKey, dataState)
	require.NoError(t, err)
	nodeInfo := types.NodeInfo{
		NodeID:          nodeKey.Public().NodeID(),
		ListenAddr:      addr.String(),
		Network:         genDoc.ChainID,
		Moniker:         "node1",
		Channels:        []byte{},
		ProtocolVersion: types.ProtocolVersion{P2P: 1, Block: 2, App: 3},
		Version:         "1.2.3",
		Other:           types.NodeInfoOther{TxIndex: "on", RPCAddress: "rpc.domain.com"},
	}
	e := p2p.Endpoint{AddrPort: addr}
	router, err := p2p.NewRouter(nodeKey, func() *types.NodeInfo { return &nodeInfo }, dbm.NewMemDB(), &p2p.RouterOptions{
		SelfAddress:              utils.Some(e.NodeAddress(nodeKey.Public().NodeID())),
		Endpoint:                 e,
		Connection:               conn.DefaultMConnConfig(),
		IncomingConnectionWindow: utils.Some(time.Duration(0)),
		MaxAcceptRate:            rate.Inf,
		MaxDialRate:              rate.Limit(30),
		Giga:                     utils.Some[p2p.GigaRouter](giga),
	})
	require.NoError(t, err)

	if x := node1EnvInt("NODE1_EXTRA_P", 0); x > 0 {
		runtime.GOMAXPROCS(runtime.GOMAXPROCS(0) + x)
	}
	var precheckInitialS, insertS float64
	var insertEnd time.Time
	err = scope.Run(ctx, func(ctx context.Context, s scope.Scope) error {
		s.SpawnBgNamed("router", func() error { return utils.IgnoreCancel(router.Run(ctx)) })
		s.SpawnBgNamed("giga", func() error { return utils.IgnoreCancel(giga.Run(ctx)) })

		// Admission, part 1: the real CheckTx on the first precheckAhead txs before any is inserted.
		t0 := time.Now()
		for range max(1, cfg.precheckWorkers) {
			s.SpawnBg(func() error { return app.runPrecheck(ctx) })
		}
		initialChunks := min(len(app.chunkDone), cfg.precheckAhead/node1Chunk)
		for c := range initialChunks {
			// utils.Recv waits for ctx on a closed channel, so wait on the close directly.
			select {
			case <-app.chunkDone[c]:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		precheckInitialS = time.Since(t0).Seconds()
		if app.closeAdmit != nil && initialChunks == len(app.chunkDone) {
			// Every tx is admitted: the admitter's store stops before the window opens.
			app.closeAdmit()
		}
		if msg := app.precheckErr.Load(); msg != nil {
			return fmt.Errorf("CheckTx rejected a presigned tx: %s", *msg)
		}

		sampleCtx, stopSample := context.WithCancel(ctx)
		s.SpawnBg(func() error {
			select {
			case <-app.windowOpen:
				node1SampleGauges(sampleCtx, time.Duration(cfg.gaugeMs)*time.Millisecond)
			case <-sampleCtx.Done():
			}
			return nil
		})

		// Admission, part 2: one inserter in a fixed order, then the sentinel that seals the last block.
		s.SpawnNamed("inserter", func() error {
			mp := giga.Mempool().OrPanic("validator giga must have a mempool")
			if os.Getenv("NODE1_LOCK_INSERTER") == "1" {
				runtime.LockOSThread()
				defer runtime.UnlockOSThread()
			}
			t0 := time.Now()
			prev := t0
			for i := range nTxs + 1 {
				c0 := time.Now()
				resp, err := mp.InsertTx(ctx, app.tx(i))
				c1 := time.Now()
				if app.windowStarted.Load() {
					d := c1.Sub(c0).Nanoseconds()
					app.insCalls.Add(1)
					app.insNs.Add(d)
					app.insGapNs.Add(c0.Sub(prev).Nanoseconds())
					if d > 50_000 {
						app.insSlow.Add(1)
						app.insSlowNs.Add(d)
					}
					if d > app.insMaxNs.Load() {
						app.insMaxNs.Store(d)
					}
				}
				prev = c1
				if err != nil {
					return fmt.Errorf("InsertTx(%d): %w", i, err)
				}
				if resp.Code != abci.CodeTypeOK {
					return fmt.Errorf("InsertTx(%d): code=%d log=%s", i, resp.Code, resp.Log)
				}
				app.inserted.Add(1)
			}
			app.insertDone.Store(true)
			insertEnd = time.Now()
			insertS = insertEnd.Sub(t0).Seconds()
			return nil
		})
		if _, err := app.finalizedTxs.Wait(ctx, func(f int64) bool { return f >= int64(nTxs) }); err != nil {
			return err
		}
		stopSample()
		return nil
	})
	require.NoError(t, err)
	var endRusage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &endRusage)
	node1ProfStop()
	endGC := node1GCCycles()

	require.NoError(t, app.AwaitCommits())
	app.mu.Lock()
	blocks := slices_clone(app.blocks)
	firstFailure := app.firstFailure
	app.mu.Unlock()
	lastHeight := blocks[len(blocks)-1].height

	// Receipts: the executor writes them behind the block; wait for the last height to land.
	rcptOK, rcptFailed, rcptMissing := -1, -1, -1
	if cfg.receipts {
		rdb := manager.ReceiptDB()
		deadline := time.Now().Add(2 * time.Minute)
		for rdb.LatestVersion() < lastHeight && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		var ok, failed, missing atomic.Int64
		sctx := sdk.NewContext(nil, tmproto.Header{Height: lastHeight}, false).WithContext(context.Background())
		parallelFor(nTxs, func(i int) {
			h := crypto.Keccak256Hash(wl.txs[i])
			r, err := rdb.GetReceipt(sctx, h)
			switch {
			case err != nil || r == nil:
				missing.Add(1)
			case r.Status == uint32(ethtypes.ReceiptStatusSuccessful):
				ok.Add(1)
			default:
				failed.Add(1)
			}
		})
		rcptOK, rcptFailed, rcptMissing = int(ok.Load()), int(failed.Load()), int(missing.Load())
	}

	// Committed FlatKV LtHash, as the store publishes it.
	flatkvLtHash := "unreachable"
	if sc := manager.SC(); sc != nil {
		if err := sc.FlushHashes(); err == nil {
			if h, err := sc.RegisterHashListener(nil); err == nil {
				sum := h.Global.Checksum()
				flatkvLtHash = hex.EncodeToString(sum[:])
			}
		}
	}
	flatkvVersion := manager.SC().Version()
	flatkvDir := storageCfg.FlatKVConfig.DataDir
	receiptDir := storageCfg.ReceiptDBConfig.DBDirectory
	managerClosed = true
	require.NoError(t, manager.Close())

	// ---- report
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	B := len(blocks)
	okTotal, failedTotal, minTxs, maxTxs := 0, 0, blocks[0].txs, blocks[0].txs
	var gasTotal int64
	digest := sha256.New()
	for _, b := range blocks {
		okTotal += b.ok
		failedTotal += b.failed
		gasTotal += b.gasUsed
		minTxs, maxTxs = min(minTxs, b.txs), max(maxTxs, b.txs)
		digest.Write(b.appHash)
	}
	span := func(from int) (int, float64, int) {
		wall := blocks[B-1].done.Sub(blocks[from].done).Seconds()
		txs := 0
		for _, b := range blocks[from+1:] {
			txs += b.txs
		}
		return B - 1 - from, wall, txs
	}
	gomax := runtime.GOMAXPROCS(0)
	fmt.Printf("NODE1 cfg workload=%s admit=%s txs=%d blocks=%d accounts=%d recipients=%s receipts=%v canonical=%v block_interval_ms=%d max_gas=%d gomaxprocs=%d occ_workers=%d parse_workers=%d precheck_ahead=%d precheck_workers=%d chain_id=%d initial_height=%d dir=%s\n",
		cfg.workload, cfg.admit, cfg.txsPerBlock, cfg.blocks, cfg.accounts, cfg.recipients, cfg.receipts, cfg.canonical, cfg.blockIntervalMs, cfg.maxGas,
		gomax, gomax, gomax, cfg.precheckAhead, cfg.precheckWorkers, tmconfig.AutobahnEVMOnlyChainID, genDoc.InitialHeight, cfg.dir)
	fmt.Printf("NODE1 admission presign_s=%.3f precheck_initial=%d precheck_initial_s=%.3f precheck_us_per_tx_core=%.1f precheck_before_window=%d precheck_in_window=%d cache_waits=%d cache_wait_s=%.3f uncached=%d admission_mismatch=%d inner_checktx=%d insert_s=%.3f insert_ended_before_last_block=%v key_field=%v key_checks=%d key_mismatch=%d admitter_checktx=%d carried_bytes=%d\n",
		wl.presignS, initialChunksTxs(cfg, nTxs), precheckInitialS,
		1e6*precheckInitialS*float64(max(1, cfg.precheckWorkers))/float64(max(1, initialChunksTxs(cfg, nTxs))),
		app.precheckAtWin, app.prechecked.Load()-app.precheckAtWin, app.cacheWaits.Load(), float64(app.cacheWaitNs.Load())/1e9, app.uncached.Load(), app.mismatches.Load(), app.innerChecks.Load(),
		insertS, insertEnd.Before(blocks[B-1].done), carriedFieldPresent(), app.keyChecks.Load(), app.keyMismatch.Load(), app.admitChecks.Load(), app.carriedBytes.Load())
	nb, wall, wtx := span(0)
	fmt.Printf("NODE1 window blocks=%d txs=%d wall_s=%.6f blocks_per_s=%.2f tx_per_s=%.0f ms_per_block=%.3f\n",
		nb, wtx, wall, float64(nb)/wall, float64(wtx)/wall, 1000*wall/float64(nb))
	from := max(1, B/10)
	nb2, wall2, wtx2 := span(from)
	fmt.Printf("NODE1 steady from_height=%d blocks=%d txs=%d wall_s=%.6f blocks_per_s=%.2f tx_per_s=%.0f ms_per_block=%.6f\n",
		blocks[from].height, nb2, wtx2, wall2, float64(nb2)/wall2, float64(wtx2)/wall2, 1000*wall2/float64(nb2))
	var starved, doneBlocks int
	aheadMin, aheadSum := int64(-1), int64(0)
	for _, b := range blocks[from+1:] {
		if b.feedDone {
			doneBlocks++
			continue
		}
		if b.feedAhead < int64(2*cfg.txsPerBlock) {
			starved++
		}
		if aheadMin < 0 || b.feedAhead < aheadMin {
			aheadMin = b.feedAhead
		}
		aheadSum += b.feedAhead
	}
	fed := max(1, nb2-doneBlocks)
	fmt.Printf("NODE1 feed fill_s=%.3f fill_txs=%d gauge_ms=%d steady_blocks=%d steady_fed_blocks=%d steady_after_insert_blocks=%d steady_starved_blocks=%d steady_min_ahead_blocks=%.2f steady_mean_ahead_blocks=%.2f\n",
		app.fillS, app.fillTxs, cfg.gaugeMs, nb2, nb2-doneBlocks, doneBlocks, starved,
		float64(aheadMin)/float64(cfg.txsPerBlock), float64(aheadSum)/float64(fed)/float64(cfg.txsPerBlock))
	fmt.Printf("NODE1 feeddiag win_insert_calls=%d win_insert_s=%.4f win_gap_s=%.4f slow_calls=%d slow_s=%.4f max_insert_ms=%.3f nonce_calls=%d nonce_s=%.4f nonce_max_ms=%.3f extra_p=%d nonce_table=%v nonce_checked=%d nonce_behind=%d nonce_ahead=%d\n",
		app.insCalls.Load(), float64(app.insNs.Load())/1e9, float64(app.insGapNs.Load())/1e9, app.insSlow.Load(), float64(app.insSlowNs.Load())/1e9,
		float64(app.insMaxNs.Load())/1e6, app.nonceCalls.Load(), float64(app.nonceNs.Load())/1e9, float64(app.nonceMaxNs.Load())/1e6, node1EnvInt("NODE1_EXTRA_P", 0),
		app.nonceTable, app.nonceChecked.Load()/64, app.nonceBehind.Load(), app.nonceAhead.Load())
	fmt.Printf("NODE1 success ok=%d failed=%d expected=%d receipts_ok=%d receipts_failed=%d receipts_missing=%d gas_used=%d first_failure=%q\n",
		okTotal, failedTotal, nTxs, rcptOK, rcptFailed, rcptMissing, gasTotal, firstFailure)
	fmt.Printf("NODE1 state final_apphash=%x apphash_digest=%x blocks=%d first_height=%d last_height=%d block_txs_min=%d block_txs_max=%d flatkv_version=%d flatkv_lthash=%s\n",
		blocks[B-1].appHash, digest.Sum(nil), B, blocks[0].height, lastHeight, minTxs, maxTxs, flatkvVersion, flatkvLtHash)
	cpuWin := rusageCPU(&endRusage) - rusageCPU(&app.startRusage)
	maxrss := float64(endRusage.Maxrss)
	if runtime.GOOS == "darwin" {
		maxrss /= 1 << 20
	} else {
		maxrss /= 1 << 10
	}
	fmt.Printf("NODE1 proc cpu_window_s=%.6f cpu_us_per_tx=%.4f cores_busy=%.2f peak_rss_mb=%.0f gc_window=%d gc_total=%d\n",
		cpuWin, 1e6*cpuWin/float64(wtx), cpuWin/wall, maxrss, endGC-app.startGC, endGC)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[float64]:
				for _, dp := range d.DataPoints {
					fmt.Printf("NODE1 phase %s %s total_s=%.4f ms_per_block=%.4f\n", m.Name, node1Attrs(dp.Attributes), dp.Value, 1000*dp.Value/float64(B))
				}
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					fmt.Printf("NODE1 count %s %s total=%d\n", m.Name, node1Attrs(dp.Attributes), dp.Value)
				}
			}
		}
	}
	start := map[string]float64{}
	for _, sm := range app.startMetrics.ScopeMetrics {
		for _, m := range sm.Metrics {
			if d, ok := m.Data.(metricdata.Sum[float64]); ok {
				for _, dp := range d.DataPoints {
					start[m.Name+" "+node1Attrs(dp.Attributes)] = dp.Value
				}
			}
		}
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if d, ok := m.Data.(metricdata.Sum[float64]); ok {
				for _, dp := range d.DataPoints {
					k := m.Name + " " + node1Attrs(dp.Attributes)
					fmt.Printf("NODE1 wphase %s total_s=%.6f ms_per_block=%.6f\n", k, dp.Value-start[k], 1000*(dp.Value-start[k])/float64(nb))
				}
			}
		}
	}
	node1PrintGauges()
	fmt.Printf("NODE1 paths flatkv=%s receipts=%s latest_height=%d\n", flatkvDir, receiptDir, lastHeight)

	if cfg.admit != "self" && app.innerChecks.Load() != 0 {
		t.Fatalf("NODE1 FAIL: %s admission called the application's CheckTx %d times", cfg.admit, app.innerChecks.Load())
	}
	if n := app.mismatches.Load(); n != 0 {
		t.Fatalf("NODE1 FAIL: the workload's admission data differs from CheckTx's on %d txs", n)
	}
	if failedTotal != 0 || okTotal != nTxs {
		t.Fatalf("NODE1 FAIL: %d of %d txs succeeded (%d failed): %s", okTotal, nTxs, failedTotal, firstFailure)
	}
	if cfg.receipts && rcptOK != nTxs {
		t.Fatalf("NODE1 FAIL: receipts ok=%d failed=%d missing=%d of %d", rcptOK, rcptFailed, rcptMissing, nTxs)
	}
	if minTxs != cfg.txsPerBlock || maxTxs != cfg.txsPerBlock || B != cfg.blocks {
		t.Fatalf("NODE1 FAIL: block composition not deterministic: blocks=%d txs min=%d max=%d", B, minTxs, maxTxs)
	}
}

// initialChunksTxs is the number of txs prechecked before the first insert.
func initialChunksTxs(cfg node1Config, nTxs int) int {
	return min(nTxs+1, (cfg.precheckAhead/node1Chunk)*node1Chunk)
}

func slices_clone[T any](s []T) []T { return append([]T(nil), s...) }

func node1Attrs(set attribute.Set) string {
	var parts []string
	for _, kv := range set.ToSlice() {
		parts = append(parts, string(kv.Key)+"="+kv.Value.Emit())
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ",")
}

// ---------------------------------------------------------------------------------------------
// autobahn gauges, sampled as nodetail does

type node1GaugeStat struct {
	n             int
	sum, min, max float64
}

var node1Gauges = map[string]*node1GaugeStat{}

func node1PromKey(name string, labels []string) string {
	sort.Strings(labels)
	return name + "{" + strings.Join(labels, ",") + "}"
}

func node1SampleGauges(ctx context.Context, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		mfs, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			continue
		}
		stage := map[string]float64{}
		for _, mf := range mfs {
			if !strings.Contains(mf.GetName(), "autobahn") {
				continue
			}
			for _, m := range mf.GetMetric() {
				if m.GetGauge() == nil {
					continue
				}
				var labels []string
				for _, lp := range m.GetLabel() {
					labels = append(labels, lp.GetName()+"="+lp.GetValue())
					if strings.HasSuffix(mf.GetName(), "data_next_block") && lp.GetName() == "stage" {
						stage[lp.GetValue()] = m.GetGauge().GetValue()
					}
				}
				node1RecordGauge(node1PromKey(mf.GetName(), labels), m.GetGauge().GetValue())
			}
		}
		if len(stage) == 4 {
			node1RecordGauge("depth receive-execute", stage["receive"]-stage["execute"])
			node1RecordGauge("depth execute-certify", stage["execute"]-stage["certify"])
			node1RecordGauge("depth certify-evict", stage["certify"]-stage["evict"])
			node1RecordGauge("depth receive-evict", stage["receive"]-stage["evict"])
		}
	}
}

func node1RecordGauge(k string, v float64) {
	g, ok := node1Gauges[k]
	if !ok {
		g = &node1GaugeStat{min: v, max: v}
		node1Gauges[k] = g
	}
	g.n++
	g.sum += v
	g.min = min(g.min, v)
	g.max = max(g.max, v)
}

func node1PrintGauges() {
	keys := make([]string, 0, len(node1Gauges))
	for k := range node1Gauges {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		g := node1Gauges[k]
		fmt.Printf("NODE1 gauge %s n=%d mean=%.2f min=%.0f max=%.0f\n", k, g.n, g.sum/float64(g.n), g.min, g.max)
	}
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return
	}
	for _, mf := range mfs {
		if !strings.Contains(mf.GetName(), "autobahn") {
			continue
		}
		for _, m := range mf.GetMetric() {
			h := m.GetHistogram()
			if h == nil || h.GetSampleCount() == 0 {
				continue
			}
			var labels []string
			for _, lp := range m.GetLabel() {
				labels = append(labels, lp.GetName()+"="+lp.GetValue())
			}
			fmt.Printf("NODE1 hist %s count=%d mean_ms=%.3f\n", node1PromKey(mf.GetName(), labels), h.GetSampleCount(), 1000*h.GetSampleSum()/float64(h.GetSampleCount()))
		}
	}
}

// newNode1Admitter opens a second application over its own store at the same genesis: the validator that
// admitted the carried workload's transactions. It returns the application and a func that closes its store.
func newNode1Admitter(t *testing.T, cfg node1Config, wl *node1Workload, genDoc *types.GenesisDoc, validators []abci.ValidatorUpdate) (abci.Application, func()) {
	dir := filepath.Join(cfg.dir, "admitter")
	require.NoError(t, os.RemoveAll(dir))
	require.NoError(t, os.MkdirAll(dir, 0o700))
	storageCfg, err := evmonly.NewValidatorStorageConfig(dir, false)
	require.NoError(t, err)
	blockCfg, err := tmconfig.AutobahnBlockDBConfig{}.LittBlockConfig(filepath.Join(dir, "blockdb"))
	require.NoError(t, err)
	storageCfg.BlockDBConfig = &blockCfg
	storageCtx, cancel := context.WithCancel(context.Background())
	manager, err := bootstrap.NewGigaStorageManager(storageCtx, storageCfg)
	require.NoError(t, err)
	encoder := evmonly.NewFlatKVChangeSetEncoder(manager.SC())
	genesisChanges, err := encoder(wl.genesis)
	require.NoError(t, err)
	require.NoError(t, manager.StateDB().CommitStateChanges(genDoc.InitialHeight-1, genesisChanges))
	admitter, err := evmonlyapp.NewEVMOnlyApplication(tmconfig.AutobahnEVMOnlyChainID, validators, manager, encoder)
	require.NoError(t, err)
	var once sync.Once
	return admitter, func() {
		once.Do(func() {
			_ = manager.Close()
			cancel()
			_ = os.RemoveAll(dir)
		})
	}
}

// node1ReplayPlan is each transaction's pool account, nonce and replayed holder (-1 for none).
type node1ReplayPlan struct {
	account []int32
	nonce   []uint64
	holder  []int32
}

// node1LoadReplay reads NODE1_REPLAY, a JSON list of windows of [sender, holder] pairs, and lays window
// b mod len(windows) on block b: sender s is pool account s, which must lie in the pool's first half so no
// sender is another's pooled recipient, and nonces run on per account across blocks. It returns nil when
// NODE1_REPLAY is unset.
func node1LoadReplay(t *testing.T, perBlock, n, accounts int) *node1ReplayPlan {
	path := os.Getenv("NODE1_REPLAY")
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var windows [][][2]int32
	require.NoError(t, json.Unmarshal(raw, &windows))
	plan, holders, maxRun := node1PlanReplay(t, windows, perBlock, n, accounts)
	fmt.Printf("NODE1 replay path=%s windows=%d holders=%d max_sender_txs=%d\n", filepath.Base(path), len(windows), holders, maxRun)
	return plan
}

// node1PlanReplay lays window b mod len(windows) of [sender, holder] pairs on block b, as node1LoadReplay
// describes. It returns the plan, the number of distinct holders and the most transactions any sender sends.
func node1PlanReplay(t *testing.T, windows [][][2]int32, perBlock, n, accounts int) (*node1ReplayPlan, int, int) {
	require.NotEmpty(t, windows)
	plan := &node1ReplayPlan{account: make([]int32, n), nonce: make([]uint64, n), holder: make([]int32, n)}
	next := make([]uint64, accounts)
	holders, maxRun := map[int32]bool{}, 0
	for i := range n {
		w := windows[(i/perBlock)%len(windows)]
		require.Len(t, w, perBlock, "replay window size")
		p := w[i%perBlock]
		require.True(t, p[0] >= 0 && int(p[0]) < accounts/2, "replay sender %d outside the pool's first half", p[0])
		plan.account[i], plan.holder[i] = p[0], p[1]
		plan.nonce[i] = next[p[0]]
		next[p[0]]++
		if p[1] >= 0 {
			holders[p[1]] = true
		}
	}
	for _, c := range next {
		maxRun = max(maxRun, int(c))
	}
	return plan, len(holders), maxRun
}

// node1ReplayHolder is the address that stands for replayed hot key h.
func node1ReplayHolder(h int32) common.Address {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(h))
	return common.BytesToAddress(crypto.Keccak256([]byte("node1-replay-holder"), buf[:]))
}

// ---------------------------------------------------------------------------------------------
// touch replay: each real transaction's storage keys as reads and increments on hand-assembled contracts

const (
	node1TouchContracts = 64
	node1TouchGasLimit  = 3_000_000
	// node1TouchCodeHex is the touch contract's runtime, hand-assembled. Its calldata is a
	// sequence of records: 0x00 slot(32) loads slot; 0x01 slot(32) stores slot+1; 0x02 addr(20) len(2) data
	// calls addr with data and reverts if that call fails. Any other op byte reverts.
	node1TouchCodeHex = "60005b36811061000b57005b803560f81c801561002c578060011461003c57600214610051575b60006000fd5b5080600101355450602101610002565b50806001013580546001019055602101610002565b806001013560601c816015013560f01c8083601701600037600060008260006000865af1156100265790500160170161000256"
)

// node1TouchPlan is the touch replay: each transaction's sender plan, target contract and calldata.
type node1TouchPlan struct {
	plan   *node1ReplayPlan
	window [][]node1TouchTx
	slots  map[int32]int32 // slot index -> contract index
	codes  [node1TouchContracts][]byte
	signer ethtypes.Signer
}

// node1TouchTx is one replayed transaction: the contract it is sent to and its encoded calldata.
type node1TouchTx struct {
	to   int32
	data []byte
}

// node1TouchAddr is the address of touch contract i.
func node1TouchAddr(i int32) common.Address {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(i))
	return common.BytesToAddress(crypto.Keccak256([]byte("node1-touch"), buf[:])[12:])
}

// node1TouchSlot is the storage key that stands for replayed slot s.
func node1TouchSlot(s int32) common.Hash {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(s))
	return crypto.Keccak256Hash([]byte("node1-touch-slot"), buf[:])
}

// node1TouchCalldata encodes records [contract, slot, op] for a call to contract to: to's records inline, and
// each run of another contract's records as one nested call to it.
func node1TouchCalldata(to int32, recs [][3]int32) []byte {
	var out []byte
	for i := 0; i < len(recs); {
		j := i
		for j < len(recs) && recs[j][0] == recs[i][0] {
			j++
		}
		var run []byte
		for _, r := range recs[i:j] {
			run = append(run, byte(r[2]))
			run = append(run, node1TouchSlot(r[1]).Bytes()...)
		}
		if recs[i][0] == to {
			out = append(out, run...)
		} else {
			addr := node1TouchAddr(recs[i][0])
			out = append(out, 0x02)
			out = append(out, addr[:]...)
			out = binary.BigEndian.AppendUint16(out, uint16(len(run)))
			out = append(out, run...)
		}
		i = j
	}
	return out
}

// node1LoadTouch reads NODE1_REPLAY_TOUCH, a JSON list of windows of [sender, to, [[contract, slot, op], ...]]
// entries, and lays window b mod len(windows) on block b with node1PlanReplay's sender plan. It returns nil
// when NODE1_REPLAY_TOUCH is unset.
func node1LoadTouch(t *testing.T, perBlock, n, accounts int) *node1TouchPlan {
	path := os.Getenv("NODE1_REPLAY_TOUCH")
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var entries [][][]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &entries))
	tp := &node1TouchPlan{
		window: make([][]node1TouchTx, len(entries)),
		slots:  map[int32]int32{},
		signer: ethtypes.LatestSignerForChainID(new(big.Int).SetUint64(tmconfig.AutobahnEVMOnlyChainID)),
	}
	senders := make([][][2]int32, len(entries))
	maxRecs, sumRecs, nTx := 0, 0, 0
	for w, win := range entries {
		require.Len(t, win, perBlock, "touch window size")
		senders[w] = make([][2]int32, len(win))
		tp.window[w] = make([]node1TouchTx, len(win))
		for j, e := range win {
			require.Len(t, e, 3, "touch entry")
			var sender, to int32
			var recs [][3]int32
			require.NoError(t, json.Unmarshal(e[0], &sender))
			require.NoError(t, json.Unmarshal(e[1], &to))
			require.NoError(t, json.Unmarshal(e[2], &recs))
			require.True(t, to >= 0 && to < node1TouchContracts, "touch contract %d", to)
			for _, r := range recs {
				require.True(t, r[0] >= 0 && r[0] < node1TouchContracts && r[1] >= 0 && (r[2] == 0 || r[2] == 1), "touch record %v", r)
				if c, ok := tp.slots[r[1]]; ok {
					require.Equal(t, c, r[0], "touch slot %d in two contracts", r[1])
				}
				tp.slots[r[1]] = r[0]
			}
			senders[w][j] = [2]int32{sender, -1}
			tp.window[w][j] = node1TouchTx{to: to, data: node1TouchCalldata(to, recs)}
			maxRecs, sumRecs, nTx = max(maxRecs, len(recs)), sumRecs+len(recs), nTx+1
		}
	}
	plan, _, maxRun := node1PlanReplay(t, senders, perBlock, n, accounts)
	tp.plan = plan
	fmt.Printf("NODE1 touch path=%s windows=%d contracts=%d slots=%d max_records=%d mean_records=%.3f max_sender_txs=%d gas_limit=%d\n",
		filepath.Base(path), len(entries), node1TouchContracts, len(tp.slots), maxRecs, float64(sumRecs)/float64(nTx), maxRun, node1TxGasLimit)
	tp.codes = node1LoadTouchCodes(t)
	return tp
}

// node1LoadTouchCodes returns the runtime code deployed at each touch contract: the touch code everywhere
// when NODE1_REPLAY_TOUCH_CODE is unset, and otherwise the touch code, a zero byte and contract i's entry
// of that file, a JSON list of 64 hex strings of real contract code. The appended bytes are never executed.
func node1LoadTouchCodes(t *testing.T) [node1TouchContracts][]byte {
	var codes [node1TouchContracts][]byte
	touch := common.FromHex(node1TouchCodeHex)
	path := os.Getenv("NODE1_REPLAY_TOUCH_CODE")
	if path == "" {
		for c := range codes {
			codes[c] = touch
		}
		return codes
	}
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var tails []string
	require.NoError(t, json.Unmarshal(raw, &tails))
	require.Len(t, tails, node1TouchContracts, "touch code count")
	total, minLen, maxLen := 0, 0, 0
	deployed := sha256.New()
	for c, tail := range tails {
		body, err := hex.DecodeString(tail)
		require.NoError(t, err, "touch code %d", c)
		require.NotEmpty(t, body, "touch code %d", c)
		code := append(append(bytes.Clone(touch), 0x00), body...)
		codes[c] = code
		deployed.Write(code)
		total += len(code)
		if c == 0 || len(code) < minLen {
			minLen = len(code)
		}
		maxLen = max(maxLen, len(code))
	}
	fileSum := sha256.Sum256(raw)
	fmt.Printf("NODE1 touchcode path=%s sha256=%x codes=%d total_bytes=%d mean_bytes=%.1f min_bytes=%d max_bytes=%d deployed_sha256=%x\n",
		filepath.Base(path), fileSum, len(tails), total, float64(total)/float64(len(tails)), minLen, maxLen, deployed.Sum(nil))
	return codes
}

// seedGenesis deploys each touch contract's code and sets every replayed slot to 1, so each
// increment rewrites a nonzero value.
func (tp *node1TouchPlan) seedGenesis(gw *node1GenesisWriter) {
	for c := range int32(node1TouchContracts) {
		gw.SetCode(node1TouchAddr(c), tp.codes[c])
	}
	one := common.BigToHash(big.NewInt(1))
	for s, c := range tp.slots {
		gw.SetState(node1TouchAddr(c), node1TouchSlot(s), one)
	}
}

// buildTransaction signs transaction i of the touch replay: a legacy call from key to its window entry's
// contract, priced and limited as the loadtest's scenarios are.
func (tp *node1TouchPlan) buildTransaction(key *ecdsa.PrivateKey, nonce uint64, i int) (*ethtypes.Transaction, error) {
	perBlock := len(tp.window[0])
	e := tp.window[(i/perBlock)%len(tp.window)][i%perBlock]
	to := node1TouchAddr(e.to)
	return ethtypes.SignTx(ethtypes.NewTx(&ethtypes.LegacyTx{
		Nonce:    nonce,
		GasPrice: big.NewInt(node1GasPrice),
		Gas:      node1TxGasLimit,
		To:       &to,
		Value:    new(big.Int),
		Data:     e.data,
	}), tp.signer, key)
}

// ---------------------------------------------------------------------------------------------
// swap: SushiSwap V2 router swaps over N pools, the makers' vendored production bytecode

const (
	// node1SwapGasLimit is each swap's gas limit; a swap uses about 105.6k.
	node1SwapGasLimit = 400_000
	// node1SwapPairInitCodeHash is SushiSwap V2's pair init code hash, which its factory's CREATE2 uses.
	node1SwapPairInitCodeHash = "0xe18a34eb0e04b04f7a0ac29a6e80748dca96319b42c54d679cb821dca90c6303"
)

var (
	node1SwapAmountIn      = big.NewInt(1_000_000_000_000_000)                                 // 1e15 token units per swap
	node1SwapSenderToken   = big.NewInt(1_000_000_000_000_000_000)                             // 1e18 token units per sender
	node1SwapReserve       = new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)             // each side of each pool
	node1SwapDeadline      = uint64(1) << 40                                                   // far past any block time
	node1SwapWETH          = common.HexToAddress("0x000000000000000000000000000000000000eeee") // router's WETH, never called
	node1SwapNativeReserve = new(big.Int).Exp(big.NewInt(10), big.NewInt(24), nil)             // each pool's WSEI side, in wei
)

// node1SwapPlan is the swap workload: the deployed addresses, each pool's output token, and the pool of each
// transaction.
type node1SwapPlan struct {
	tokenIn  common.Address
	factory  common.Address
	router   common.Address
	tokenOut []common.Address
	pairs    []common.Address
	pool     []uint16 // transaction i swaps on pool pool[i]
	signer   ethtypes.Signer
	genesis  *node1GenesisWriter
	// native is set by NODE1_SWAP_NATIVE=1: the router's WETH is the product's WSEI (x/evm/artifacts/wsei), every
	// pool pairs WSEI with an output token, and every swap is swapExactETHForTokens paid in the native coin.
	native bool
	wsei   common.Address
	// outPermille, set by NODE1_SWAP_NATIVE_OUT_PERMILLE on a native plan, is the share of swaps that sell a pool's
	// output token for the native coin (swapExactTokensForETH, ending in WSEI.withdraw). Each sender then holds and
	// approves its own pool's output token, and senderPool names that pool.
	outPermille int
	senderPool  map[common.Address]uint16
}

func node1SwapWord(a any) []byte {
	var w [32]byte
	switch v := a.(type) {
	case common.Address:
		copy(w[12:], v[:])
	case *big.Int:
		v.FillBytes(w[:])
	case uint64:
		binary.BigEndian.PutUint64(w[24:], v)
	}
	return w[:]
}

func node1SwapCall(sig string, args ...any) []byte {
	out := bytes.Clone(crypto.Keccak256([]byte(sig))[:4])
	for _, a := range args {
		out = append(out, node1SwapWord(a)...)
	}
	return out
}

// node1SwapPool is the pool transaction i swaps on, fixed by i: pool 0 for hotPermille per mille of the
// transactions, and otherwise uniform over pools.
func node1SwapPool(i uint64, pools, hotPermille int) uint16 {
	x := i + 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	if int((x>>32)%1000) < hotPermille {
		return 0
	}
	return uint16(x % uint64(pools))
}

// node1SwapSellsNative reports whether transaction i sells for the native coin, fixed by i: permille per mille of
// the transactions.
func node1SwapSellsNative(i uint64, permille int) bool {
	x := i + 0x632be59bd9b4e019
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	x ^= x >> 31
	return int((x>>32)%1000) < permille
}

// node1LoadSwap reads NODE1_SWAP, a JSON object of the TestERC20, UniswapV2Factory and UniswapV2Router02 creation
// bytecode, deploys them with NODE1_SWAP_POOLS pools through the product's sequential executor on an in-memory
// state, mints every pool account its input tokens and has it approve the router, and keeps the resulting code,
// storage and contract nonces as genesis. It returns nil when NODE1_SWAP is unset.
func node1LoadSwap(t *testing.T, cfg node1Config, keys []*ecdsa.PrivateKey, addrs []common.Address) *node1SwapPlan {
	path := os.Getenv("NODE1_SWAP")
	if path == "" {
		return nil
	}
	pools := node1EnvInt("NODE1_SWAP_POOLS", 64)
	require.True(t, pools > 0 && pools <= 1<<16, "NODE1_SWAP_POOLS %d", pools)
	hotPermille := node1EnvInt("NODE1_SWAP_HOT_PERMILLE", 0)
	require.True(t, hotPermille >= 0 && hotPermille <= 1000, "NODE1_SWAP_HOT_PERMILLE %d", hotPermille)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var hexes map[string]string
	require.NoError(t, json.Unmarshal(raw, &hexes))
	bin := func(name string) []byte {
		b, err := hex.DecodeString(strings.TrimPrefix(strings.TrimSpace(hexes[name]), "0x"))
		require.NoError(t, err, "swap bytecode %s", name)
		require.NotEmpty(t, b, "swap bytecode %s", name)
		return b
	}
	chainID := new(big.Int).SetUint64(tmconfig.AutobahnEVMOnlyChainID)
	sp := &node1SwapPlan{signer: ethtypes.LatestSignerForChainID(chainID), native: os.Getenv("NODE1_SWAP_NATIVE") == "1"}
	sp.outPermille = node1EnvInt("NODE1_SWAP_NATIVE_OUT_PERMILLE", 0)
	require.True(t, sp.outPermille >= 0 && sp.outPermille <= 1000, "NODE1_SWAP_NATIVE_OUT_PERMILLE %d", sp.outPermille)
	require.True(t, sp.outPermille == 0 || sp.native, "NODE1_SWAP_NATIVE_OUT_PERMILLE needs NODE1_SWAP_NATIVE=1")
	deployer, err := crypto.ToECDSA(crypto.Keccak256([]byte("node1-swap-deployer")))
	require.NoError(t, err)
	d := crypto.PubkeyToAddress(deployer.PublicKey)
	dNonce := uint64(0)
	signValue := func(key *ecdsa.PrivateKey, nonce uint64, to *common.Address, data []byte, gas uint64, value *big.Int) evmonly.PreparedTx {
		tx, err := ethtypes.SignTx(ethtypes.NewTx(&ethtypes.LegacyTx{Nonce: nonce, GasPrice: big.NewInt(node1GasPrice), Gas: gas, To: to, Value: value, Data: data}), sp.signer, key)
		require.NoError(t, err)
		return evmonly.PreparedTx{Tx: tx, Sender: crypto.PubkeyToAddress(key.PublicKey)}
	}
	sign := func(key *ecdsa.PrivateKey, nonce uint64, to *common.Address, data []byte, gas uint64) evmonly.PreparedTx {
		return signValue(key, nonce, to, data, gas, new(big.Int))
	}
	nextValue := func(to *common.Address, data []byte, gas uint64, value *big.Int) evmonly.PreparedTx {
		tx := signValue(deployer, dNonce, to, data, gas, value)
		dNonce++
		return tx
	}
	next := func(to *common.Address, data []byte, gas uint64) evmonly.PreparedTx {
		return nextValue(to, data, gas, new(big.Int))
	}
	erc := bin("TestERC20")
	sp.tokenIn = crypto.CreateAddress(d, 0)
	sp.factory, sp.router = crypto.CreateAddress(d, 1), crypto.CreateAddress(d, 2)
	weth := node1SwapWETH
	if sp.native {
		sp.wsei = crypto.CreateAddress(d, 3)
		weth = sp.wsei
	}
	b0 := []evmonly.PreparedTx{
		next(nil, append(bytes.Clone(erc), node1SwapWord(d)...), 3_000_000),
		next(nil, append(bin("UniswapV2Factory"), node1SwapWord(d)...), 8_000_000),
		next(nil, append(append(bin("UniswapV2Router02"), node1SwapWord(sp.factory)...), node1SwapWord(weth)...), 8_000_000),
	}
	if sp.native {
		b0 = append(b0, next(nil, bin("WSEI"), 3_000_000))
	}
	sp.tokenOut, sp.pairs = make([]common.Address, pools), make([]common.Address, pools)
	for p := range pools {
		sp.tokenOut[p] = crypto.CreateAddress(d, dNonce)
		b0 = append(b0, next(nil, append(bytes.Clone(erc), node1SwapWord(d)...), 3_000_000))
	}
	var b1 []evmonly.PreparedTx
	for p := range pools {
		in := sp.tokenIn
		if sp.native {
			in = sp.wsei
		}
		t0, t1 := in, sp.tokenOut[p]
		if bytes.Compare(t0[:], t1[:]) > 0 {
			t0, t1 = t1, t0
		}
		sp.pairs[p] = crypto.CreateAddress2(sp.factory, crypto.Keccak256Hash(t0[:], t1[:]), common.FromHex(node1SwapPairInitCodeHash))
		b1 = append(b1, next(&sp.factory, node1SwapCall("createPair(address,address)", in, sp.tokenOut[p]), 5_000_000))
		if sp.native {
			b1 = append(b1,
				nextValue(&sp.wsei, node1SwapCall("deposit()"), 200_000, node1SwapNativeReserve),
				next(&sp.wsei, node1SwapCall("transfer(address,uint256)", sp.pairs[p], node1SwapNativeReserve), 200_000))
		} else {
			b1 = append(b1, next(&sp.tokenIn, node1SwapCall("mint(address,uint256)", sp.pairs[p], node1SwapReserve), 200_000))
		}
		b1 = append(b1,
			next(&sp.tokenOut[p], node1SwapCall("mint(address,uint256)", sp.pairs[p], node1SwapReserve), 200_000),
			next(&sp.pairs[p], node1SwapCall("mint(address)", d), 500_000))
	}
	setup := [][]evmonly.PreparedTx{b0, b1}
	max256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	var mints, approves []evmonly.PreparedTx
	if sp.outPermille > 0 {
		sp.senderPool = make(map[common.Address]uint16, cfg.accounts)
	}
	for a := range cfg.accounts {
		token := &sp.tokenIn
		if sp.outPermille > 0 {
			sp.senderPool[addrs[a]] = uint16(a % pools)
			token = &sp.tokenOut[a%pools]
		}
		mints = append(mints, next(token, node1SwapCall("mint(address,uint256)", addrs[a], node1SwapSenderToken), 200_000))
		approves = append(approves, sign(keys[a], 0, token, node1SwapCall("approve(address,uint256)", sp.router, max256), 100_000))
	}
	for i := 0; i < len(mints); i += cfg.txsPerBlock {
		setup = append(setup, mints[i:min(i+cfg.txsPerBlock, len(mints))])
	}
	for i := 0; i < len(approves); i += cfg.txsPerBlock {
		setup = append(setup, approves[i:min(i+cfg.txsPerBlock, len(approves))])
	}

	// Run the setup blocks on one thread and keep what they wrote to contracts. Pool accounts keep nonce 0 and the
	// harness's balances: their approvals are genesis state, so the timed swaps start at nonce 0.
	ms := evmonly.NewMemoryState()
	ms.SetBalance(d, new(big.Int).Mul(node1SenderBalance, big.NewInt(1000)))
	if sp.native {
		ms.SetBalance(d, new(big.Int).Mul(node1SwapNativeReserve, big.NewInt(int64(pools)+1)))
	}
	for a := range cfg.accounts {
		ms.SetBalance(addrs[a], node1SenderBalance)
	}
	store := &node1SwapSetupStore{MemoryStore: evmonly.NewMemoryStore(ms)}
	ex := evmonly.NewExecutor(evmonly.Config{OCCWorkers: 1, ParseWorkers: 1, RejectUnappliableTxs: true},
		evmonly.WithStore(store, store.EncodeChangeSet), evmonly.WithReceiptStore(evmonly.NewMemoryReceiptStore()))
	defer ex.Close()
	gw := &node1GenesisWriter{balances: map[common.Address]*big.Int{}, code: map[common.Address][]byte{}, storage: map[common.Address]map[common.Hash]common.Hash{}}
	contractNonces := map[common.Address]uint64{}
	setupGas := uint64(0)
	for bi, txs := range setup {
		res, err := ex.ExecutePreparedBlock(context.Background(), evmonly.PreparedBlock{
			Context: evmonly.BlockContext{
				Number: uint64(bi + 1), Time: 1_700_000_000 + uint64(bi), GasLimit: 1 << 62, ChainID: chainID,
				BaseFee: big.NewInt(0), BlobBaseFee: big.NewInt(0),
				ParentHash: common.Hash{byte(bi)}, BlockHash: common.Hash{byte(bi + 1)},
			},
			Txs: txs,
		})
		require.NoError(t, err, "swap setup block %d", bi)
		for i, r := range res.Txs {
			require.True(t, r.Status == ethtypes.ReceiptStatusSuccessful, "swap setup block %d tx %d failed: %v", bi, i, r.Err)
		}
		setupGas += res.GasUsed
		cs := res.ChangeSet
		require.Empty(t, cs.StorageClears, "swap setup block %d clears storage", bi)
		for _, c := range cs.Code {
			require.False(t, c.Delete, "swap setup block %d deletes code", bi)
			gw.SetCode(c.Address, c.Code)
		}
		for _, s := range cs.Storage {
			if s.Delete {
				gw.SetState(s.Address, s.Key, common.Hash{})
			} else {
				gw.SetState(s.Address, s.Key, s.Value)
			}
		}
		for _, n := range cs.Nonces {
			contractNonces[n.Address] = n.Nonce
		}
		for _, b := range cs.Balances {
			if sp.native && b.Address == sp.wsei {
				gw.SetBalance(b.Address, b.Balance)
			}
		}
		ms.ApplyChangeSet(cs)
	}
	for a, n := range contractNonces {
		if _, ok := gw.code[a]; ok {
			gw.SetNonce(a, n)
		}
	}
	sp.genesis = gw

	n := cfg.txsPerBlock * cfg.blocks
	sp.pool = make([]uint16, n)
	perPool := make([]int, pools)
	for i := range n {
		sp.pool[i] = node1SwapPool(uint64(i), pools, hotPermille)
		perPool[sp.pool[i]]++
	}
	maxPool := 0
	for _, c := range perPool {
		maxPool = max(maxPool, c)
	}
	// Integrity: the bytecode file, the runtime code the setup deployed, and the pools the transactions name.
	codeSum := sha256.New()
	for _, a := range []common.Address{sp.tokenIn, sp.factory, sp.router, sp.pairs[0], sp.tokenOut[0]} {
		codeSum.Write(gw.code[a])
	}
	fileSum := sha256.Sum256(raw)
	if sp.native {
		require.Equal(t, 0, new(big.Int).Mul(node1SwapNativeReserve, big.NewInt(int64(pools))).Cmp(gw.balances[sp.wsei]), "WSEI holds every pool's deposit")
	}
	fmt.Printf("NODE1 swap native=%t out_permille=%d path=%s sha256=%x pools=%d hot_permille=%d pairs=%d contracts=%d router=%s router_bytes=%d pair_bytes=%d token_bytes=%d code_sha256=%x setup_blocks=%d setup_gas=%d max_pool_txs=%d gas_limit=%d\n",
		sp.native, sp.outPermille, filepath.Base(path), fileSum, pools, hotPermille, len(sp.pairs), len(gw.code), sp.router.Hex(), len(gw.code[sp.router]), len(gw.code[sp.pairs[0]]),
		len(gw.code[sp.tokenIn]), codeSum.Sum(nil), len(setup), setupGas, maxPool, node1TxGasLimit)
	contracts := 2*pools + 3
	if sp.native {
		contracts++
	}
	require.Equal(t, contracts, len(gw.code), "swap contracts deployed")
	return sp
}

// seedGenesis lays the setup's contract code, storage and nonces into the node's genesis.
func (sp *node1SwapPlan) seedGenesis(gw *node1GenesisWriter) {
	for a, c := range sp.genesis.code {
		gw.SetCode(a, c)
	}
	for a, s := range sp.genesis.storage {
		for k, v := range s {
			gw.SetState(a, k, v)
		}
	}
	for a, n := range sp.genesis.nonces {
		gw.SetNonce(a, n)
	}
	for a, b := range sp.genesis.balances {
		gw.SetBalance(a, b)
	}
}

// buildTransaction signs transaction i, a router swap paid to the sender and priced as the loadtest's scenarios
// are: on a token plan, swapExactTokensForTokens of the input token for pool i's output token; on a native plan,
// swapExactETHForTokens for pool i's output token, or, for the share outPermille names, swapExactTokensForETH of
// the sender's own pool's output token.
func (sp *node1SwapPlan) buildTransaction(key *ecdsa.PrivateKey, sender common.Address, nonce uint64, i int) (*ethtypes.Transaction, error) {
	// swapExactTokensForTokens(amountIn, amountOutMin, path, to, deadline), the dynamic path at offset 0xa0
	data := node1SwapCall("swapExactTokensForTokens(uint256,uint256,address[],address,uint256)",
		node1SwapAmountIn, big.NewInt(0), uint64(0xa0), sender, node1SwapDeadline, uint64(2), sp.tokenIn, sp.tokenOut[sp.pool[i]])
	value := new(big.Int)
	if sp.native && node1SwapSellsNative(uint64(i), sp.outPermille) {
		// swapExactTokensForETH(amountIn, amountOutMin, path, to, deadline), the dynamic path at offset 0xa0, selling
		// the sender's own pool's output token for the native coin
		data = node1SwapCall("swapExactTokensForETH(uint256,uint256,address[],address,uint256)",
			node1SwapAmountIn, big.NewInt(0), uint64(0xa0), sender, node1SwapDeadline, uint64(2), sp.tokenOut[sp.senderPool[sender]], sp.wsei)
	} else if sp.native {
		// swapExactETHForTokens(amountOutMin, path, to, deadline), the dynamic path at offset 0x80, paying amountIn in wei
		data = node1SwapCall("swapExactETHForTokens(uint256,address[],address,uint256)",
			big.NewInt(0), uint64(0x80), sender, node1SwapDeadline, uint64(2), sp.wsei, sp.tokenOut[sp.pool[i]])
		value = node1SwapAmountIn
	}
	return ethtypes.SignTx(ethtypes.NewTx(&ethtypes.LegacyTx{
		Nonce:    nonce,
		GasPrice: big.NewInt(node1GasPrice),
		Gas:      node1TxGasLimit,
		To:       &sp.router,
		Value:    value,
		Data:     data,
	}), sp.signer, key)
}

// node1SwapSetupStore is the swap setup's store: the setup applies each block's change set to its state itself.
type node1SwapSetupStore struct {
	*evmonly.MemoryStore
}

func (*node1SwapSetupStore) CommitStateChanges(int64, []*seidbproto.NamedChangeSet) error { return nil }
