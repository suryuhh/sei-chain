//go:build benchharness

// One-validator Autobahn EVM-only node, in process: the real evmonlyapp, Giga executor, FlatKV, receipt store, block
// store and runExecute, fed presigned ERC-20 transfers through the producer's mempool (TestNode1). With
// NODE1_RPCQ_LIVE=serve the node's EVM-only JSON-RPC server is up; TestCallQClient, run as a separate process, sends
// one eth_call every NODE1_CALLQ_EVERY_US from the node's first executed block until it reports its last, never
// retried, and checks each answer's NUMBER and BALANCE(0x0) against the state the node recorded at that height.
// Prints NODE1 and CALLQCLIENT summary lines that bench/call/run.py parses.
package p2p_test

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
	"regexp"
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

	"io"
	"net/http"

	"github.com/ethereum/go-ethereum/common/hexutil"
	ethrpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/holiman/uint256"
	"github.com/sei-protocol/sei-chain/giga/evmonly"
	evmonlyrpc "github.com/sei-protocol/sei-chain/giga/evmonly/rpc"
	sdk "github.com/sei-protocol/sei-chain/sei-cosmos/types"
	"github.com/sei-protocol/sei-chain/sei-db/bootstrap"
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
	tmbytes "github.com/sei-protocol/sei-chain/sei-tendermint/libs/bytes"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/require"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/scope"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/tcp"
	tmproto "github.com/sei-protocol/sei-chain/sei-tendermint/proto/tendermint/types"
	"github.com/sei-protocol/sei-chain/sei-tendermint/rpc/coretypes"
	"github.com/sei-protocol/sei-chain/sei-tendermint/types"
	evmtypes "github.com/sei-protocol/sei-chain/x/evm/types"
)

type node1Config struct {
	txsPerBlock     int
	blocks          int
	accounts        int
	recipients      string
	admit           string
	receipts        bool
	canonical       bool
	blockIntervalMs int
	precheckAhead   int
	precheckWorkers int
	maxGas          int64
	workload        string
	gaugeMs         int
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
		node1TxGasLimit = 100_000
	case loadoffline.Transfer:
		node1TxGasLimit = 21_000
	default:
		t.Fatalf("NODE1_WORKLOAD must be erc20-transfer or transfer")
	}
	if cfg.admit != "self" && cfg.admit != "foreign" && cfg.admit != "carried" {
		t.Fatalf("NODE1_ADMIT must be self, foreign or carried")
	}
	if cfg.accounts < 2*cfg.txsPerBlock {

		t.Fatalf("NODE1_ACCOUNTS must be at least 2*NODE1_TXS")
	}

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

const node1GasPrice = 1_000_000_000

var node1TxGasLimit uint64 = 100_000

var (
	node1ERC20         = common.HexToAddress("0x000000000000000000000000000000000000e20c")
	node1SenderBalance = big.NewInt(1_000_000_000_000_000_000)
	node1TransferValue = big.NewInt(1)
	node1TokenBalance  = new(big.Int).Lsh(big.NewInt(1), 64)
)

type node1Workload struct {
	txs      [][]byte
	sentinel []byte
	addrs    []common.Address
	pubs     [][]byte
	genesis  evmonly.StateChangeSet
	presignS float64
}

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

	nAcc := cfg.accounts + 1
	keys := make([]*ecdsa.PrivateKey, nAcc)
	addrs := make([]common.Address, nAcc)
	pubs := make([][]byte, nAcc)
	parallelFor(nAcc, func(i int) {
		keys[i] = node1Key(uint64(i))
		addrs[i] = crypto.PubkeyToAddress(keys[i].PublicKey)
		pubs[i] = crypto.CompressPubkey(&keys[i].PublicKey)
	})
	gw := &node1GenesisWriter{balances: map[common.Address]*big.Int{}, code: map[common.Address][]byte{}, storage: map[common.Hash]common.Hash{}}
	require.NoError(t, scenario.SetupGenesis(gw))
	for _, a := range addrs {
		gw.SetBalance(a, node1SenderBalance)
		if cfg.workload == loadoffline.ERC20Transfer {
			gw.SetState(node1ERC20, loadoffline.ERC20BalanceSlot(a), common.BigToHash(node1TokenBalance))
		}
	}
	n := cfg.txsPerBlock * cfg.blocks
	txs := make([][]byte, n)
	A := uint64(cfg.accounts)
	var signErr atomic.Pointer[error]
	parallelFor(n, func(i int) {
		idx := uint64(i)
		account, nonce := idx%A, idx/A
		var recipient common.Address
		if cfg.recipients == "pooled" {
			recipient = addrs[(account+A/2)%A]
		} else {
			recipient = node1FreshRecipient(idx)
		}
		signed, err := scenario.BuildTransaction(keys[account], nonce, recipient)
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
	return &node1Workload{txs: txs, sentinel: sentinel, addrs: addrs, pubs: pubs, genesis: gw.changeSet(), presignS: time.Since(start).Seconds()}
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

type node1GenesisWriter struct {
	balances map[common.Address]*big.Int
	code     map[common.Address][]byte
	storage  map[common.Hash]common.Hash
}

func (w *node1GenesisWriter) SetBalance(a common.Address, b *big.Int) {
	w.balances[a] = new(big.Int).Set(b)
}
func (w *node1GenesisWriter) SetCode(a common.Address, c []byte) { w.code[a] = bytes.Clone(c) }
func (w *node1GenesisWriter) SetState(a common.Address, k, v common.Hash) {
	if a != node1ERC20 {
		panic("genesis storage outside the token")
	}
	w.storage[k] = v
}

func (w *node1GenesisWriter) changeSet() evmonly.StateChangeSet {
	var cs evmonly.StateChangeSet
	addrs := make([]common.Address, 0, len(w.balances)+1)
	for a := range w.balances {
		addrs = append(addrs, a)
	}
	for a := range w.code {
		if _, ok := w.balances[a]; !ok {
			addrs = append(addrs, a)
		}
	}
	sort.Slice(addrs, func(i, j int) bool { return bytes.Compare(addrs[i][:], addrs[j][:]) < 0 })
	for _, a := range addrs {
		if b, ok := w.balances[a]; ok {
			cs.Balances = append(cs.Balances, evmonly.BalanceChange{Address: a, Balance: b})
		}
		if c, ok := w.code[a]; ok {
			cs.Code = append(cs.Code, evmonly.CodeChange{Address: a, Code: c})
		}
		if a == node1ERC20 {
			slots := make([]common.Hash, 0, len(w.storage))
			for k := range w.storage {
				slots = append(slots, k)
			}
			sort.Slice(slots, func(i, j int) bool { return bytes.Compare(slots[i][:], slots[j][:]) < 0 })
			for _, k := range slots {
				cs.Storage = append(cs.Storage, evmonly.StorageChange{Address: a, Key: k, Value: w.storage[k]})
			}
		}
	}
	return cs
}

const node1Chunk = 256

type node1Block struct {
	height  int64
	appHash []byte
	txs     int
	ok      int
	failed  int
	gasUsed int64
	done    time.Time

	feedAhead int64
	feedDone  bool
}

type node1App struct {
	abci.Application
	cfg node1Config
	wl  *node1Workload

	index        map[string]int32
	resp         []*abci.ResponseCheckTxV2
	chunkDone    []chan struct{}
	nextChunk    atomic.Int64
	prechecked   atomic.Int64
	precheckErr  atomic.Pointer[string]
	mismatches   atomic.Int64
	keyChecks    atomic.Int64
	keyMismatch  atomic.Int64
	innerChecks  atomic.Int64
	admitter     abci.Application
	closeAdmit   func()
	admitChecks  atomic.Int64
	carriedBytes atomic.Int64
	reader       *sdkmetric.ManualReader
	startMetrics metricdata.ResourceMetrics
	cacheWaits   atomic.Int64
	cacheWaitNs  atomic.Int64
	uncached     atomic.Int64
	finalizedTxs utils.AtomicSend[int64]

	inserted   atomic.Int64
	insertDone atomic.Bool
	fillS      float64
	fillTxs    int64
	windowOpen chan struct{}

	nonceCalls atomic.Int64
	nonceNs    atomic.Int64
	nonceMaxNs atomic.Int64
	insCalls   atomic.Int64
	insNs      atomic.Int64
	insSlow    atomic.Int64
	insSlowNs  atomic.Int64
	insMaxNs   atomic.Int64
	insGapNs   atomic.Int64

	nonceTable   bool
	addrIdx      map[common.Address]int
	execNonce    []atomic.Uint64
	nonceChecked atomic.Int64
	nonceBehind  atomic.Int64
	nonceAhead   atomic.Int64

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

func (a *node1App) admission(i int) *abci.ResponseCheckTxV2 {
	A := a.cfg.accounts
	account, nonce := a.accountOf(i), uint64(0)
	if i < len(a.wl.txs) {
		nonce = uint64(i / A)
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

func (a *node1App) accountOf(i int) int {
	if i < len(a.wl.txs) {
		return i % a.cfg.accounts
	}
	return a.cfg.accounts
}

func carriedKey(r *abci.ResponseCheckTxV2) []byte {
	if !carriedFieldPresent() {
		return nil
	}
	return reflect.ValueOf(r).Elem().FieldByName(carriedKeyField).Bytes()
}

const carriedKeyField = "EVMSenderPubKey"

func carriedFieldPresent() bool {
	f, ok := reflect.TypeOf(abci.ResponseCheckTxV2{}).FieldByName(carriedKeyField)
	return ok && f.Type == reflect.TypeOf([]byte(nil))
}

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
	if callqRecordOn {

		if bal, ok := a.Application.(interface {
			EvmBalance(common.Address, []byte) uint256.Int
		}); ok {
			v := bal.EvmBalance(common.Address{}, nil)
			callqCoinbaseMu.Lock()
			callqCoinbase[req.Header.Height] = v.Hex()
			callqCoinbaseMu.Unlock()
		}
	}
	if !a.windowStarted.Swap(true) {
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &a.startRusage)
		a.startGC = node1GCCycles()
		a.precheckAtWin = a.prechecked.Load()

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

		if r.Code == abci.CodeTypeOK && r.Log == "" {
			b.ok++
		} else {
			b.failed++
			if a.firstFailure == "" {
				a.firstFailure = fmt.Sprintf("height=%d idx=%d code=%d log=%q", b.height, i, r.Code, r.Log)
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
	var serveSrv *evmonlyrpc.Server
	if os.Getenv("NODE1_RPCQ_LIVE") == "serve" {

		var err error
		serveSrv, err = evmonlyrpc.Start(rpcqBackend{giga: giga, app: inner}, manager.ReceiptDB())
		require.NoError(t, err)
		go func() { _ = serveSrv.Serve(context.Background()) }()
		var hb strings.Builder
		for i := range wl.txs {
			hb.WriteString(crypto.Keccak256Hash(wl.txs[i]).Hex())
			hb.WriteByte('\n')
		}
		hf := os.Getenv("NODE1_RPCQ_HASHFILE")
		require.NoError(t, os.WriteFile(hf+".tmp", []byte(hb.String()), 0o600))
		require.NoError(t, os.Rename(hf+".tmp", hf))
	}
	err = scope.Run(ctx, func(ctx context.Context, s scope.Scope) error {
		s.SpawnBgNamed("router", func() error { return utils.IgnoreCancel(router.Run(ctx)) })
		s.SpawnBgNamed("giga", func() error { return utils.IgnoreCancel(giga.Run(ctx)) })

		t0 := time.Now()
		for range max(1, cfg.precheckWorkers) {
			s.SpawnBg(func() error { return app.runPrecheck(ctx) })
		}
		initialChunks := min(len(app.chunkDone), cfg.precheckAhead/node1Chunk)
		for c := range initialChunks {

			select {
			case <-app.chunkDone[c]:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		precheckInitialS = time.Since(t0).Seconds()
		if app.closeAdmit != nil && initialChunks == len(app.chunkDone) {

			app.closeAdmit()
		}
		if msg := app.precheckErr.Load(); msg != nil {
			return fmt.Errorf("CheckTx rejected a presigned tx: %s", *msg)
		}

		if mode := os.Getenv("NODE1_RPCQ_LIVE"); mode == "full" || mode == "lean" {
			liveCtx, stopLive := context.WithCancel(ctx)
			defer stopLive()
			rpcqLive(liveCtx, s, mode, giga, manager.ReceiptDB(), wl.txs)
		}
		if os.Getenv("NODE1_RPCQ_LIVE") == "http" {
			srv, err := evmonlyrpc.Start(rpcqBackend{giga: giga, app: inner}, manager.ReceiptDB())
			if err != nil {
				return fmt.Errorf("evmonlyrpc.Start: %w", err)
			}
			s.SpawnBg(func() error { _ = srv.Serve(ctx); return nil })
			liveCtx, stopLive := context.WithCancel(ctx)
			defer stopLive()
			defer srv.Stop()
			rpcqHTTP(liveCtx, s, wl.txs)
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
		if os.Getenv("NODE1_RPCQ_LIVE") == "http" {
			rpcqVerify(ctx, giga, wl.txs)
		}
		return nil
	})
	require.NoError(t, err)
	var endRusage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &endRusage)
	endGC := node1GCCycles()
	if serveSrv != nil {

		var bh strings.Builder
		lastH := int64(node1EnvInt("NODE1_BLOCKS", 200)) + 1
		for h := int64(2); h <= lastH; h++ {
			hctx, cancelH := context.WithTimeout(context.Background(), 10*time.Second)
			b, err := giga.BlockByNumber(hctx, atypes.GlobalBlockNumber(h))
			cancelH()
			if err != nil || b == nil || b.Block == nil {
				break
			}
			fmt.Fprintf(&bh, "%d %s\n", h, common.BytesToHash(b.BlockID.Hash).Hex())
		}
		if callqRecordOn {
			var cb strings.Builder
			callqCoinbaseMu.Lock()
			for h, v := range callqCoinbase {
				fmt.Fprintf(&cb, "%d %s\n", h, v)
			}
			callqCoinbaseMu.Unlock()
			cf := os.Getenv("NODE1_RPCQ_HASHFILE") + ".coinbase"
			require.NoError(t, os.WriteFile(cf+".tmp", []byte(cb.String()), 0o600))
			require.NoError(t, os.Rename(cf+".tmp", cf))
		}
		bf := os.Getenv("NODE1_RPCQ_HASHFILE") + ".blocks"
		require.NoError(t, os.WriteFile(bf+".tmp", []byte(bh.String()), 0o600))
		require.NoError(t, os.Rename(bf+".tmp", bf))

		holdEnd := time.Now().Add(time.Duration(node1EnvInt("NODE1_RPCQ_HOLD_S", 20)) * time.Second)
		for done := os.Getenv("NODE1_RPCQ_DONEFILE"); time.Now().Before(holdEnd); time.Sleep(50 * time.Millisecond) {
			if _, err := os.Stat(done); done != "" && err == nil {
				break
			}
		}
		serveSrv.Stop()
	}

	require.NoError(t, app.AwaitCommits())
	app.mu.Lock()
	blocks := slices_clone(app.blocks)
	firstFailure := app.firstFailure
	app.mu.Unlock()
	lastHeight := blocks[len(blocks)-1].height

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
	if os.Getenv("NODE1_RPCQ") != "" && cfg.receipts {
		rpcqRun(t, giga, manager.ReceiptDB(), manager.BlockStore(), wl.txs, lastHeight)
	}

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
	fmt.Printf("RPCQLIVE sleep_ms=%s mode=%s hits=%d misses=%d block_reads=%d http_errors=%d http_bytes=%d\n", os.Getenv("NODE1_RPCQ_SLEEP_MS"), os.Getenv("NODE1_RPCQ_LIVE"), rpcqLiveHits.Load(), rpcqLiveMiss.Load(), rpcqLiveBlockReads.Load(), rpcqHTTPErrors.Load(), rpcqHTTPBytes.Load())
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

type rpcqReceiptDB interface {
	GetReceipt(ctx sdk.Context, h common.Hash) (*evmtypes.Receipt, error)
}

func rpcqEncode(hash common.Hash, stored *evmtypes.Receipt, blockHash common.Hash) map[string]any {
	logs := make([]*ethtypes.Log, 0, len(stored.Logs))
	for _, sl := range stored.Logs {
		if sl == nil {
			continue
		}
		topics := make([]common.Hash, len(sl.Topics))
		for i, tp := range sl.Topics {
			topics[i] = common.HexToHash(tp)
		}
		logs = append(logs, &ethtypes.Log{Address: common.HexToAddress(sl.Address), Topics: topics,
			Data: append([]byte(nil), sl.Data...), BlockNumber: stored.BlockNumber, TxHash: hash,
			TxIndex: uint(stored.TransactionIndex), BlockHash: blockHash, Index: uint(sl.Index)})
	}
	bloom := ethtypes.Bloom{}
	bloom.SetBytes(stored.LogsBloom)
	return map[string]any{"blockHash": blockHash, "blockNumber": hexutil.Uint64(stored.BlockNumber),
		"cumulativeGasUsed": hexutil.Uint64(stored.CumulativeGasUsed),
		"effectiveGasPrice": (*hexutil.Big)(new(big.Int).SetUint64(stored.EffectiveGasPrice)),
		"from":              common.HexToAddress(stored.From), "gasUsed": hexutil.Uint64(stored.GasUsed), "logs": logs,
		"logsBloom": bloom, "status": hexutil.Uint64(stored.Status), "transactionHash": hash,
		"transactionIndex": hexutil.Uint64(stored.TransactionIndex), "type": hexutil.Uint64(stored.TxType)}
}

func rpcqCPU() float64 {
	var r syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return rusageCPU(&r)
}

type rpcqBlockStore interface {
	ReadBlockByNumber(n atypes.GlobalBlockNumber) (utils.Option[*atypes.Block], error)
	ReadQCByBlockNumber(n atypes.GlobalBlockNumber) (utils.Option[*atypes.FullCommitQC], error)
}

func rpcqRun(t *testing.T, giga p2p.GigaRouter, rdbAny any, bs rpcqBlockStore, txs [][]byte, lastHeight int64) {
	rdb := rdbAny.(rpcqReceiptDB)
	ctx := context.Background()
	sctx := sdk.Context{}.WithContext(ctx)
	hashes := make([]common.Hash, len(txs))
	for i := range txs {
		hashes[i] = crypto.Keccak256Hash(txs[i])
	}

	blockHash := map[uint64]common.Hash{}
	for _, h := range hashes {
		r, err := rdb.GetReceipt(sctx, h)
		require.NoError(t, err)
		if _, ok := blockHash[r.BlockNumber]; !ok {
			b, err := giga.BlockByNumber(ctx, atypes.GlobalBlockNumber(r.BlockNumber))
			require.NoError(t, err)
			blockHash[r.BlockNumber] = common.BytesToHash(b.BlockID.Hash)
		}
	}
	var hmu sync.Mutex
	workers := node1EnvInt("NODE1_RPCQ_WORKERS", runtime.GOMAXPROCS(0))
	n := node1EnvInt("NODE1_RPCQ_N", 40000)
	kinds := strings.Split(node1EnvStr("NODE1_RPCQ_KINDS", "lean,full,block,lean,full,block"), ",")
	for _, kind := range kinds {
		var next atomic.Int64
		var bytesOut atomic.Int64
		runtime.GC()
		c0, t0 := rpcqCPU(), time.Now()
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for {
					i := next.Add(1) - 1
					if i >= int64(n) {
						return
					}
					h := hashes[(i*7919+int64(w))%int64(len(hashes))]
					switch kind {
					case "block":
						r, err := rdb.GetReceipt(sctx, h)
						if err != nil {
							panic(err)
						}
						b, err := giga.BlockByNumber(ctx, atypes.GlobalBlockNumber(r.BlockNumber))
						if err != nil || b.Block == nil {
							panic(fmt.Sprint("block ", err))
						}
					case "full", "lean":
						r, err := rdb.GetReceipt(sctx, h)
						if err != nil {
							panic(err)
						}
						var bh common.Hash
						if kind == "full" {
							b, err := giga.BlockByNumber(ctx, atypes.GlobalBlockNumber(r.BlockNumber))
							if err != nil || b.Block == nil {
								panic(fmt.Sprint("block ", err))
							}
							bh = common.BytesToHash(b.BlockID.Hash)
						} else {
							hmu.Lock()
							bh = blockHash[r.BlockNumber]
							hmu.Unlock()
						}
						out, err := json.Marshal(rpcqEncode(h, r, bh))
						if err != nil {
							panic(err)
						}
						bytesOut.Add(int64(len(out)))
					case "rblock", "rqc", "hdr":
						r, err := rdb.GetReceipt(sctx, h)
						if err != nil {
							panic(err)
						}
						gn := atypes.GlobalBlockNumber(r.BlockNumber)
						if kind == "rqc" {
							if _, err := bs.ReadQCByBlockNumber(gn); err != nil {
								panic(err)
							}
							break
						}
						b, err := bs.ReadBlockByNumber(gn)
						if err != nil {
							panic(err)
						}
						if kind == "hdr" {
							_ = b.OrPanic("block").Header().Hash()
						}
					case "receipt":
						if _, err := rdb.GetReceipt(sctx, h); err != nil {
							panic(err)
						}
					}
				}
			}(w)
		}
		wg.Wait()
		wall := time.Since(t0).Seconds()
		cpu := rpcqCPU() - c0
		fmt.Printf("RPCQ kind=%s workers=%d n=%d blocks=%d wall_s=%.3f qps=%.0f cpu_us_per_q=%.1f bytes_per_q=%d\n",
			kind, workers, n, len(blockHash), wall, float64(n)/wall, cpu*1e6/float64(n), bytesOut.Load()/int64(n))
	}
}

var rpcqLiveHits, rpcqLiveMiss, rpcqLiveBlockReads atomic.Int64

func rpcqLive(ctx context.Context, s scope.Scope, mode string, giga p2p.GigaRouter, rdbAny any, txs [][]byte) {
	rdb := rdbAny.(rpcqReceiptDB)
	sctx := sdk.Context{}.WithContext(ctx)
	pollers := node1EnvInt("NODE1_RPCQ_POLLERS", 8)
	sleep := time.Duration(node1EnvInt("NODE1_RPCQ_SLEEP_MS", 1)) * time.Millisecond
	var hashCache sync.Map
	for p := 0; p < pollers; p++ {
		s.SpawnBg(func() error {
			for i := p; i < len(txs); i += pollers {
				h := crypto.Keccak256Hash(txs[i])
				for {
					if ctx.Err() != nil {
						return nil
					}
					r, err := rdb.GetReceipt(sctx, h)
					if err != nil || r == nil {
						rpcqLiveMiss.Add(1)
						time.Sleep(sleep)
						continue
					}
					var bh common.Hash
					if v, ok := hashCache.Load(r.BlockNumber); ok && mode == "lean" {
						bh = v.(common.Hash)
					} else {
						b, err := giga.BlockByNumber(ctx, atypes.GlobalBlockNumber(r.BlockNumber))
						if err != nil || b == nil || b.Block == nil {
							if ctx.Err() != nil {
								return nil
							}
							rpcqLiveMiss.Add(1)
							time.Sleep(time.Millisecond)
							continue
						}
						rpcqLiveBlockReads.Add(1)
						bh = common.BytesToHash(b.BlockID.Hash)
						hashCache.Store(r.BlockNumber, bh)
					}
					if _, err := json.Marshal(rpcqEncode(h, r, bh)); err != nil {
						panic(err)
					}
					rpcqLiveHits.Add(1)
					break
				}
			}
			return nil
		})
	}
}

type rpcqBackend struct {
	giga p2p.GigaRouter
	app  abci.Application
}

func (b rpcqBackend) height(req *coretypes.RequestBlockInfo) (atypes.GlobalBlockNumber, error) {
	last := b.app.Info().LastBlockHeight
	if req.Height == nil || int64(*req.Height) <= 0 {
		return atypes.GlobalBlockNumber(last), nil
	}
	if int64(*req.Height) > last {
		return 0, coretypes.ErrHeightExceedsChainHead
	}
	return atypes.GlobalBlockNumber(*req.Height), nil
}

func (b rpcqBackend) Block(ctx context.Context, req *coretypes.RequestBlockInfo) (*coretypes.ResultBlock, error) {
	n, err := b.height(req)
	if err != nil {
		return nil, err
	}
	rpcqLiveBlockReads.Add(1)
	return b.giga.BlockByNumber(ctx, n)
}

func (b rpcqBackend) BlockHash(ctx context.Context, req *coretypes.RequestBlockInfo) (tmbytes.HexBytes, error) {
	n, err := b.height(req)
	if err != nil {
		return nil, err
	}
	g, ok := b.giga.(interface {
		BlockHash(context.Context, atypes.GlobalBlockNumber) (tmbytes.HexBytes, error)
	})
	if !ok {
		panic("router has no BlockHash")
	}
	return g.BlockHash(ctx, n)
}

func (b rpcqBackend) BlockByHash(ctx context.Context, req *coretypes.RequestBlockByHash) (*coretypes.ResultBlock, error) {
	panic("rpcq: BlockByHash")
}
func (b rpcqBackend) BroadcastTx(context.Context, *coretypes.RequestBroadcastTx) (*coretypes.ResultBroadcastTx, error) {
	panic("rpcq: BroadcastTx")
}
func (b rpcqBackend) EvmBalance(common.Address) uint256.Int { return uint256.Int{} }
func (b rpcqBackend) EvmBaseFee() (*big.Int, error)         { return new(big.Int), nil }
func (b rpcqBackend) EvmBlockNumber() uint64                { return uint64(b.app.Info().LastBlockHeight) }
func (b rpcqBackend) ExecutedBlocks() (utils.AtomicRecv[atypes.ExecutedBlocks], error) {
	return b.giga.ExecutedBlocks(), nil
}

func (b rpcqBackend) EvmCall(ctx context.Context, msg *ethcore.Message) (*ethcore.ExecutionResult, error) {
	return b.app.(interface {
		EvmCall(context.Context, *ethcore.Message) (*ethcore.ExecutionResult, error)
	}).EvmCall(ctx, msg)
}
func (b rpcqBackend) EvmChainConfig() (*params.ChainConfig, error) { panic("rpcq: EvmChainConfig") }
func (b rpcqBackend) EvmChainID() uint64                           { return 0 }
func (b rpcqBackend) EvmGasLimit() (uint64, error)                 { return 0, nil }
func (b rpcqBackend) EvmMinGasPrice() (*big.Int, error)            { return new(big.Int), nil }
func (b rpcqBackend) EvmProxy(common.Address) utils.Option[*ethrpc.Client] {
	return utils.None[*ethrpc.Client]()
}
func (b rpcqBackend) EvmProxyEnabled() bool                     { return false }
func (b rpcqBackend) EvmTransactionCount(common.Address) uint64 { return 0 }

var rpcqHTTPErrors atomic.Int64
var rpcqHTTPBytes atomic.Int64

func rpcqHTTP(ctx context.Context, s scope.Scope, txs [][]byte) {
	pollers := node1EnvInt("NODE1_RPCQ_POLLERS", 8)
	sleep := time.Duration(node1EnvInt("NODE1_RPCQ_SLEEP_MS", 1)) * time.Millisecond
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: pollers, MaxIdleConns: pollers}}
	url := "http://127.0.0.1:8545"
	for p := 0; p < pollers; p++ {
		s.SpawnBg(func() error {
			for i := p; i < len(txs); i += pollers {
				body := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["%s"]}`, crypto.Keccak256Hash(txs[i]).Hex()))
				for {
					if ctx.Err() != nil {
						return nil
					}
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					resp, err := client.Do(req)
					if err != nil {
						if ctx.Err() != nil {
							return nil
						}
						rpcqHTTPErrors.Add(1)
						time.Sleep(sleep)
						continue
					}
					raw, _ := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					var out struct {
						Result json.RawMessage `json:"result"`
						Error  *struct {
							Message string `json:"message"`
						} `json:"error"`
					}
					if json.Unmarshal(raw, &out) != nil || out.Error != nil {
						rpcqHTTPErrors.Add(1)
						time.Sleep(sleep)
						continue
					}
					if len(out.Result) == 0 || string(out.Result) == "null" {
						rpcqLiveMiss.Add(1)
						time.Sleep(sleep)
						continue
					}
					rpcqLiveHits.Add(1)
					rpcqHTTPBytes.Add(int64(len(raw)))
					break
				}
			}
			return nil
		})
	}
}

func rpcqVerify(ctx context.Context, giga p2p.GigaRouter, txs [][]byte) {
	client := &http.Client{}
	checked, mismatched, unanswered := 0, 0, 0
	for i := 0; i < len(txs); i += 37 {
		body := []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["%s"]}`, crypto.Keccak256Hash(txs[i]).Hex()))
		var out struct {
			Result *struct {
				BlockHash   common.Hash    `json:"blockHash"`
				BlockNumber hexutil.Uint64 `json:"blockNumber"`
			} `json:"result"`
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			resp, err := client.Post("http://127.0.0.1:8545", "application/json", bytes.NewReader(body))
			if err == nil {
				raw, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if json.Unmarshal(raw, &out) == nil && out.Result != nil {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		if out.Result == nil {
			unanswered++
			continue
		}
		b, err := giga.BlockByNumber(ctx, atypes.GlobalBlockNumber(out.Result.BlockNumber))
		checked++
		if err != nil || common.BytesToHash(b.BlockID.Hash) != out.Result.BlockHash {
			mismatched++
		}
	}
	fmt.Printf("RPCQVERIFY checked=%d mismatched=%d unanswered=%d\n", checked, mismatched, unanswered)
}

var rpcqBlockHashRE = regexp.MustCompile(`"blockHash":"(0x[0-9a-fA-F]{64})"`)

func TestRPCQClient(t *testing.T) {
	hf := os.Getenv("NODE1_RPCQ_HASHFILE")
	if hf == "" {
		t.Skip("no NODE1_RPCQ_HASHFILE")
	}
	var raw []byte
	for deadline := time.Now().Add(10 * time.Minute); ; time.Sleep(50 * time.Millisecond) {
		if b, err := os.ReadFile(hf); err == nil {
			raw = b
			break
		}
		require.True(t, time.Now().Before(deadline), "hash file never appeared")
	}
	hashes := strings.Fields(string(raw))
	perBlock := node1EnvInt("NODE1_TXS", 2000)
	first := int64(node1EnvInt("NODE1_RPCQ_FIRST", 2))
	delay := time.Duration(node1EnvInt("NODE1_RPCQ_DELAY_MS", 1000)) * time.Millisecond
	workers := node1EnvInt("NODE1_RPCQ_WORKERS", 256)
	runFor := time.Duration(node1EnvInt("NODE1_RPCQ_CLIENT_S", 120)) * time.Second
	url := "http://127.0.0.1:8545"
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: workers, MaxIdleConns: workers}, Timeout: 30 * time.Second}
	call := func(body string) ([]byte, error) {
		resp, err := client.Post(url, "application/json", strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	}
	type dueReq struct {
		i   int
		due time.Time
	}
	dueReqs := make(chan dueReq, len(hashes)+16)
	var answered, wrongBlock, httpErrs, retries, requested atomic.Int64
	every := node1EnvInt("NODE1_RPCQ_EVERY", 1)

	dumpEvery := node1EnvInt("NODE1_RPCQ_DUMP_EVERY", 0)
	var dumpMu sync.Mutex
	dumpFile := io.Discard
	if dumpEvery > 0 {
		f, err := os.Create(os.Getenv("NODE1_RPCQ_DUMP"))
		require.NoError(t, err)
		defer f.Close()
		dumpFile = f
	}
	answerSum := make([][32]byte, len(hashes))
	answerHash := make([]string, len(hashes))
	var hashBad atomic.Int64
	lat := make([]float64, len(hashes))
	for i := range lat {
		lat[i] = -1
	}
	start := time.Now()
	end := start.Add(runFor)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range dueReqs {
				if d := time.Until(j.due); d > 0 {
					time.Sleep(d)
				}
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["%s"]}`, hashes[j.i])
				for time.Now().Before(end) {
					b, err := call(body)
					var out struct {
						Result json.RawMessage           `json:"result"`
						Error  *struct{ Message string } `json:"error"`
					}
					if err != nil || json.Unmarshal(b, &out) != nil || out.Error != nil {
						httpErrs.Add(1)
						time.Sleep(200 * time.Millisecond)
						continue
					}
					if len(out.Result) == 0 || string(out.Result) == "null" {
						retries.Add(1)
						time.Sleep(200 * time.Millisecond)
						continue
					}
					var head struct {
						BlockNumber hexutil.Uint64 `json:"blockNumber"`
					}
					if json.Unmarshal(out.Result, &head) != nil || int64(head.BlockNumber) != first+int64(j.i/perBlock) {
						wrongBlock.Add(1)
					}

					hs := rpcqBlockHashRE.FindAllSubmatch(out.Result, -1)
					if len(hs) == 0 {
						hashBad.Add(1)
					} else {
						answerHash[j.i] = string(hs[0][1])
						for _, m := range hs[1:] {
							if string(m[1]) != answerHash[j.i] {
								hashBad.Add(1)
							}
						}
					}
					answerSum[j.i] = sha256.Sum256(rpcqBlockHashRE.ReplaceAll(out.Result, []byte(`"blockHash":""`)))
					if dumpEvery > 0 && j.i%dumpEvery == 0 {
						dumpMu.Lock()
						fmt.Fprintf(dumpFile, "%d %s\n", j.i, out.Result)
						dumpMu.Unlock()
					}
					lat[j.i] = time.Since(j.due).Seconds() * 1000
					answered.Add(1)
					break
				}
			}
		}()
	}

	next := first
	for time.Now().Before(end) && int(next-first)*perBlock < len(hashes) {
		b, err := call(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
		var out struct {
			Result hexutil.Uint64 `json:"result"`
		}
		if err == nil && json.Unmarshal(b, &out) == nil {
			now := time.Now()
			for ; next <= int64(out.Result) && int(next-first)*perBlock < len(hashes); next++ {
				lo := int(next-first) * perBlock
				for i := lo; i < min(lo+perBlock, len(hashes)); i++ {
					if i%every == 0 {
						requested.Add(1)
						dueReqs <- dueReq{i: i, due: now.Add(delay)}
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(dueReqs)
	wg.Wait()
	var got []float64
	for _, l := range lat {
		if l >= 0 {
			got = append(got, l)
		}
	}
	sort.Float64s(got)
	q := func(p float64) float64 {
		if len(got) == 0 {
			return -1
		}
		return got[min(len(got)-1, int(p*float64(len(got))))]
	}

	nodeHash := map[int64]string{}
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if raw, err := os.ReadFile(hf + ".blocks"); err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
				var h int64
				var hex string
				if _, err := fmt.Sscanf(line, "%d %s", &h, &hex); err == nil {
					nodeHash[h] = strings.ToLower(hex)
				}
			}
			break
		}
	}
	var hashChecked int64
	for i := range answerHash {
		if i%every != 0 || answerHash[i] == "" {
			continue
		}
		hashChecked++
		if nodeHash[first+int64(i/perBlock)] != strings.ToLower(answerHash[i]) {
			hashBad.Add(1)
		}
	}
	fmt.Printf("RPCQCLIENT node_heights=%d hash_checked=%d hash_mismatch=%d\n", len(nodeHash), hashChecked, hashBad.Load())

	ad := sha256.New()
	for i := range answerSum {
		if i%every == 0 {
			ad.Write(answerSum[i][:])
		}
	}
	if done := os.Getenv("NODE1_RPCQ_DONEFILE"); done != "" {
		_ = os.WriteFile(done, []byte("done\n"), 0o600)
	}
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)

	ad10 := sha256.New()
	for i := range answerSum {
		if i%10 == 0 {
			ad10.Write(answerSum[i][:])
		}
	}
	fmt.Printf("RPCQCLIENT every=%d requested=%d answer_digest=%x answer_digest10=%x\n", every, requested.Load(), ad.Sum(nil), ad10.Sum(nil))
	fmt.Printf("RPCQCLIENT txs=%d answered=%d unanswered=%d wrong_block=%d http_errors=%d null_retries=%d delay_ms=%d workers=%d lat_ms_p50=%.1f lat_ms_p90=%.1f lat_ms_p99=%.1f lat_ms_max=%.1f elapsed_s=%.2f client_cpu_s=%.2f\n",
		len(hashes), answered.Load(), requested.Load()-answered.Load(), wrongBlock.Load(), httpErrs.Load(), retries.Load(),
		delay.Milliseconds(), workers, q(0.5), q(0.9), q(0.99), q(1), time.Since(start).Seconds(), rusageCPU(&ru))
}

var callqRecordOn = os.Getenv("NODE1_CALLQ") == "1"
var callqCoinbaseMu sync.Mutex
var callqCoinbase = map[int64]string{}

func TestCallQClient(t *testing.T) {
	hf := os.Getenv("NODE1_RPCQ_HASHFILE")
	if hf == "" || os.Getenv("NODE1_CALLQ") != "1" {
		t.Skip("no NODE1_RPCQ_HASHFILE or NODE1_CALLQ")
	}
	for deadline := time.Now().Add(10 * time.Minute); ; time.Sleep(50 * time.Millisecond) {
		if _, err := os.Stat(hf); err == nil {
			break
		}
		require.True(t, time.Now().Before(deadline), "hash file never appeared")
	}
	first := int64(node1EnvInt("NODE1_RPCQ_FIRST", 2))
	last := first + int64(node1EnvInt("NODE1_BLOCKS", 120)) - 1
	every := time.Duration(node1EnvInt("NODE1_CALLQ_EVERY_US", 500)) * time.Microsecond
	maxWait := time.Duration(node1EnvInt("NODE1_RPCQ_CLIENT_S", 360)) * time.Second
	url := node1EnvStr("NODE1_CALLQ_URL", "http://127.0.0.1:8545")
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512, MaxIdleConns: 512}, Timeout: 60 * time.Second}
	post := func(body string) ([]byte, error) {
		resp, err := client.Post(url, "application/json", strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	}
	head := func() int64 {
		b, err := post(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
		var out struct {
			Result hexutil.Uint64 `json:"result"`
		}
		if err != nil || json.Unmarshal(b, &out) != nil {
			return -1
		}
		return int64(out.Result)
	}

	body := `{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"data":"0x4360005260003160205260406000f3"},"latest"]}`
	type result struct {
		sent, done time.Time
		class      string
		number     uint64
		balance    string
		errMsg     string
	}
	var mu sync.Mutex
	var results []*result
	var wg sync.WaitGroup
	start := time.Now()
	for head() < first {
		require.True(t, time.Since(start) < maxWait, "node never executed its first block")
		time.Sleep(5 * time.Millisecond)
	}
	spanStart := time.Now()
	var spanEnd time.Time
	stop := make(chan struct{})
	go func() {
		for {
			if h := head(); h >= last {
				mu.Lock()
				spanEnd = time.Now()
				mu.Unlock()
				close(stop)
				return
			}
			if time.Since(start) > maxWait {
				close(stop)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	tk := time.NewTicker(every)
offer:
	for {
		select {
		case <-stop:
			break offer
		case <-tk.C:
		}
		r := &result{sent: time.Now()}
		mu.Lock()
		results = append(results, r)
		mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := post(body)
			done := time.Now()
			var out struct {
				Result hexutil.Bytes             `json:"result"`
				Error  *struct{ Message string } `json:"error"`
			}
			mu.Lock()
			defer mu.Unlock()
			r.done = done
			switch {
			case err != nil || json.Unmarshal(b, &out) != nil:
				r.class = "http"
			case out.Error != nil:
				r.class, r.errMsg = "refused", out.Error.Message
			case len(out.Result) != 64:
				r.class = "bad"
			default:
				r.class = "answered"
				r.number = new(big.Int).SetBytes(out.Result[:32]).Uint64()
				r.balance = (*hexutil.Big)(new(big.Int).SetBytes(out.Result[32:])).String()
			}
		}()
	}
	tk.Stop()

	mu.Lock()
	end := spanEnd
	offered := len(results)
	outstanding := 0
	for _, r := range results {
		if r.done.IsZero() {
			outstanding++
		}
	}
	mu.Unlock()
	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(30 * time.Second):
	}
	coinbase := map[uint64]string{}
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if raw, err := os.ReadFile(hf + ".coinbase"); err == nil {
			for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
				var h uint64
				var v string
				if _, err := fmt.Sscanf(line, "%d %s", &h, &v); err == nil {
					coinbase[h] = v
				}
			}
			break
		}
	}
	mu.Lock()
	var answeredInSpan, answeredLate, refused, httpErrs, bad, matched, mismatched, unrecorded int
	var lat []float64
	heights := map[uint64]bool{}
	errMsgs := map[string]int{}
	for _, r := range results {
		switch r.class {
		case "answered":
			heights[r.number] = true
			if want, ok := coinbase[r.number]; !ok {
				unrecorded++
			} else if want == r.balance {
				matched++
			} else {
				mismatched++
			}
			if !end.IsZero() && r.done.After(end) {
				answeredLate++
			} else {
				answeredInSpan++
				lat = append(lat, r.done.Sub(r.sent).Seconds()*1000)
			}
		case "refused":
			refused++
			m := r.errMsg
			if len(m) > 60 {
				m = m[:60]
			}
			errMsgs[m]++
		case "http":
			httpErrs++
		case "bad":
			bad++
		}
	}
	unfinished := 0
	for _, r := range results {
		if r.done.IsZero() {
			unfinished++
		}
	}
	mu.Unlock()
	sort.Float64s(lat)
	q := func(p float64) float64 {
		if len(lat) == 0 {
			return -1
		}
		return lat[min(len(lat)-1, int(p*float64(len(lat))))]
	}
	if done := os.Getenv("NODE1_RPCQ_DONEFILE"); done != "" {
		_ = os.WriteFile(done, []byte("done\n"), 0o600)
	}
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	for m, n := range errMsgs {
		fmt.Printf("CALLQCLIENT_ERR n=%d msg=%q\n", n, m)
	}
	spanS := -1.0
	if !end.IsZero() {
		spanS = end.Sub(spanStart).Seconds()
	}
	fmt.Printf("CALLQCLIENT every_us=%d span_ended=%t span_s=%.3f offered=%d answered_in_span=%d refused=%d http_errors=%d bad_return=%d outstanding_at_end=%d answered_late=%d unfinished=%d\n",
		every.Microseconds(), !end.IsZero(), spanS, offered, answeredInSpan, refused, httpErrs, bad, outstanding, answeredLate, unfinished)
	fmt.Printf("CALLQCLIENT recorded_heights=%d heights_answered=%d matched=%d mismatched=%d unrecorded=%d lat_ms_p50=%.3f lat_ms_p90=%.3f lat_ms_p99=%.3f lat_ms_max=%.3f client_cpu_s=%.2f\n",
		len(coinbase), len(heights), matched, mismatched, unrecorded, q(0.5), q(0.9), q(0.99), q(1), rusageCPU(&ru))
}
