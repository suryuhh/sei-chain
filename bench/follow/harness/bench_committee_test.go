//go:build benchharness

package p2p

// Committee follow benchmark: an N-validator Autobahn committee over TCP, every validator fed a light transaction
// rate so each validator's producer seals on its 400 ms BlockInterval, and one real fullnode router
// (NewGigaFullnodeRouter, as a non-validator RPC node runs) dialing the committee. Reads the chain's global blocks/s
// at validator 0 and the share of those the fullnode finalized over the same span.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dbm "github.com/tendermint/tm-db"
	"golang.org/x/time/rate"

	"github.com/sei-protocol/sei-chain/sei-db/ledger_db/block/littblock"
	abci "github.com/sei-protocol/sei-chain/sei-tendermint/abci/types"
	"github.com/sei-protocol/sei-chain/sei-tendermint/autobahn/blockstore"
	atypes "github.com/sei-protocol/sei-chain/sei-tendermint/autobahn/types"
	"github.com/sei-protocol/sei-chain/sei-tendermint/internal/autobahn/producer"
	"github.com/sei-protocol/sei-chain/sei-tendermint/internal/p2p/conn"
	"github.com/sei-protocol/sei-chain/sei-tendermint/internal/proxy"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/require"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/scope"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/tcp"
	"github.com/sei-protocol/sei-chain/sei-tendermint/types"
)

type mlApp struct {
	*testApp
	mu      sync.Mutex
	blocks  []time.Time
	sizes   []int
	hashes  map[int64][]byte
	digests map[int64][32]byte
	dig     [32]byte
	plant   bool
	height  atomic.Int64
	lastH   atomic.Int64
}

func (a *mlApp) FinalizeBlock(ctx context.Context, req *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	now := time.Now()
	a.mu.Lock()
	a.blocks = append(a.blocks, now)
	a.sizes = append(a.sizes, len(req.Txs))
	a.mu.Unlock()
	a.height.Add(1)
	resp, err := a.testApp.FinalizeBlock(ctx, req)
	if err == nil {
		txs := req.Txs
		if a.plant && req.Header.Height == 50 && len(txs) > 0 {
			txs = txs[:len(txs)-1]
		}
		a.mu.Lock()
		if a.hashes == nil {
			a.hashes = map[int64][]byte{}
			a.digests = map[int64][32]byte{}
		}
		a.hashes[req.Header.Height] = resp.AppHash
		h := sha256.New()
		h.Write(a.dig[:])
		_ = binary.Write(h, binary.BigEndian, req.Header.Height)
		for _, tx := range txs {
			_ = binary.Write(h, binary.BigEndian, uint32(len(tx)))
			h.Write(tx)
		}
		copy(a.dig[:], h.Sum(nil))
		a.digests[req.Header.Height] = a.dig
		a.mu.Unlock()
		a.lastH.Store(req.Header.Height)
	}
	return resp, err
}

// hashAt returns the AppHash this app computed at height h, if it finalized h.
func (a *mlApp) hashAt(h int64) ([]byte, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.hashes[h]
	return v, ok
}

// digestAt returns the chained digest of every transaction this app finalized through height h, in order.
func (a *mlApp) digestAt(h int64) ([32]byte, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.digests[h]
	return v, ok
}

// window counts blocks and txs finalized in [from, to).
func (a *mlApp) window(from, to time.Time) (nb, ntx int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for j, at := range a.blocks {
		if !at.Before(from) && at.Before(to) {
			nb++
			ntx += a.sizes[j]
		}
	}
	return nb, ntx
}

func mlEnv(k string, d int) int {
	if v, err := strconv.Atoi(os.Getenv(k)); err == nil {
		return v
	}
	return d
}

func TestCommitteeFollow(t *testing.T) {
	if os.Getenv("ML_RUN") == "" {
		t.Skip("ML_RUN unset (benchmark harness)")
	}
	n := mlEnv("ML_N", 8)
	producers := mlEnv("ML_PRODUCERS", n)
	rateTotal := mlEnv("ML_RATE", 5*producers)
	warm := time.Duration(mlEnv("ML_WARM_S", 5)) * time.Second
	span := time.Duration(mlEnv("ML_SECS", 30)) * time.Second
	bi := time.Duration(mlEnv("ML_BI_MS", 400)) * time.Millisecond
	vt := time.Duration(mlEnv("ML_VT_MS", 1500)) * time.Millisecond
	txSize := mlEnv("ML_TXB", 150)
	const maxTxsPerBlock = 2000

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rng := utils.TestRng()
	_, keys := atypes.GenCommittee(rng, n)
	var cfgs []*testNodeCfg
	for _, key := range keys {
		cfgs = append(cfgs, &testNodeCfg{validatorKey: key, nodeKey: makeKey(rng), addr: tcp.TestReserveAddr()})
	}
	addrs := map[atypes.PublicKey]GigaNodeAddr{}
	for _, cfg := range cfgs {
		addrs[cfg.validatorKey.Public()] = cfg.GigaNodeAddr()
	}
	genDoc := &types.GenesisDoc{ChainID: "committee-follow", InitialHeight: 1, AppState: testAppStateJSON(rng)}
	require.NoError(t, genDoc.ValidateAndComplete())
	base := os.Getenv("ML_DIR")
	if base == "" {
		base = t.TempDir()
	}
	openStore := func(dir string) *blockstore.Store {
		require.NoError(t, os.MkdirAll(dir, 0o755))
		littCfg, err := littblock.DefaultConfig(filepath.Join(dir, "blockdb"))
		require.NoError(t, err)
		littCfg.Litt.Fsync = true
		db, err := littblock.NewBlockDB(littCfg)
		require.NoError(t, err)
		bs, err := blockstore.New(db)
		require.NoError(t, err)
		t.Cleanup(func() { _ = bs.Close() })
		return bs
	}
	startRouter := func(s scope.Scope, name string, key NodeSecretKey, addr netip.AddrPort, g GigaRouter) {
		nodeInfo := makeInfo(key)
		nodeInfo.ListenAddr = addr.String()
		nodeInfo.Network = genDoc.ChainID
		e := Endpoint{AddrPort: addr}
		router, err := NewRouter(key, func() *types.NodeInfo { return &nodeInfo }, dbm.NewMemDB(), &RouterOptions{
			SelfAddress:              utils.Some(e.NodeAddress(key.Public().NodeID())),
			Endpoint:                 e,
			Connection:               conn.DefaultMConnConfig(),
			IncomingConnectionWindow: utils.Some(time.Duration(0)),
			MaxAcceptRate:            rate.Inf,
			MaxDialRate:              rate.Limit(30),
			Giga:                     utils.Some[GigaRouter](g),
		})
		require.NoError(t, err)
		s.SpawnBgNamed("router["+name+"]", func() error { return utils.IgnoreCancel(router.Run(ctx)) })
	}

	err := scope.Run(ctx, func(ctx context.Context, s scope.Scope) error {
		var apps []*mlApp
		var gigas []*gigaValidatorRouter
		for i, cfg := range cfgs {
			app := &mlApp{testApp: newTestApp()}
			dir := filepath.Join(base, fmt.Sprintf("v%d", i))
			commonCfg := GigaRouterCommonConfig{
				DialInterval:       100 * time.Millisecond,
				ValidatorAddrs:     addrs,
				PersistentStateDir: utils.Some(dir),
				App:                proxy.New(app),
				GenDoc:             genDoc,
				EnableEvmProxy:     false,
				// The production default (config/autobahn.go DefaultMaxInboundFullnodePeers).
				MaxInboundFullnodePeers: 10,
			}
			dataState, err := BuildDataState(&commonCfg, openStore(dir))
			require.NoError(t, err)
			giga, err := NewGigaValidatorRouter(&GigaValidatorConfig{
				GigaRouterCommonConfig: commonCfg,
				ValidatorKey:           cfg.validatorKey,
				ViewTimeout:            func(atypes.View) time.Duration { return vt },
				Producer: &producer.Config{
					MaxGasWantedPerBlock:    maxTxsPerBlock,
					MaxGasEstimatedPerBlock: maxTxsPerBlock,
					MaxTxsPerBlock:          maxTxsPerBlock,
					MaxTxsPerSecond:         utils.None[uint64](),
					BlockInterval:           bi,
					AllowEmptyBlocks:        false,
					MaxPendingInserts:       producer.DefaultMaxPendingInserts,
				},
			}, cfg.nodeKey, dataState)
			require.NoError(t, err)
			startRouter(s, fmt.Sprintf("v%d", i), cfg.nodeKey, cfg.addr, giga)
			s.SpawnBgNamed(fmt.Sprintf("giga[%v]", i), func() error { return utils.IgnoreCancel(giga.Run(ctx)) })
			apps = append(apps, app)
			gigas = append(gigas, giga)
		}

		// The fullnode: no validator key, no producer, its own stores and app, dialing the committee.
		// ML_PLANT drops one transaction from the fullnode's record at height 50, to show the digest check can fail.
		fnApp := &mlApp{testApp: newTestApp(), plant: os.Getenv("ML_PLANT") != ""}
		fnKey := makeKey(rng)
		fnAddr := tcp.TestReserveAddr()
		fnDir := filepath.Join(base, "fullnode")
		fnCfg := &GigaRouterCommonConfig{
			DialInterval:       100 * time.Millisecond,
			ValidatorAddrs:     addrs,
			PersistentStateDir: utils.Some(fnDir),
			App:                proxy.New(fnApp),
			GenDoc:             genDoc,
			EnableEvmProxy:     false,
		}
		fnData, err := BuildDataState(fnCfg, openStore(fnDir))
		require.NoError(t, err)
		fn, err := NewGigaFullnodeRouter(fnCfg, fnKey, fnData)
		require.NoError(t, err)
		startRouter(s, "fullnode", fnKey, fnAddr, fn)
		s.SpawnBgNamed("giga[fullnode]", func() error { return utils.IgnoreCancel(fn.Run(ctx)) })

		// Start-up: one tx per producer, finalized at every validator.
		t0 := time.Now()
		for i := range producers {
			tx := utils.GenBytes(rng, txSize)
			_, err := gigas[i].Mempool().OrPanic("mempool").InsertTx(ctx, tx)
			require.NoError(t, err)
			for _, a := range apps {
				require.NoError(t, a.WaitForTx(ctx, tx))
			}
		}
		fmt.Printf("ML start_ms=%d n=%d producers=%d rate=%d bi_ms=%d vt_ms=%d txb=%d fullnode_height=%d\n",
			time.Since(t0).Milliseconds(), n, producers, rateTotal, bi.Milliseconds(), vt.Milliseconds(), txSize, fnApp.height.Load())

		per := float64(rateTotal) / float64(producers)
		stopAt := time.Now().Add(warm + span)
		spanFrom := time.Now().Add(warm)
		var wg sync.WaitGroup
		var inserted, insertErrs atomic.Int64
		for i := range producers {
			wg.Add(1)
			lrng := utils.TestRng()
			go func() {
				defer wg.Done()
				mp := gigas[i].Mempool().OrPanic("mempool")
				gap := time.Duration(float64(time.Second) / per)
				next := time.Now().Add(time.Duration(i) * gap / time.Duration(producers))
				k := 0
				for time.Now().Before(stopAt) {
					if d := time.Until(next); d > 0 {
						time.Sleep(d)
					}
					next = next.Add(gap)
					tx := utils.GenBytes(lrng, txSize)
					tx[0], tx[1], tx[2], tx[3] = byte(i), byte(k>>16), byte(k>>8), byte(k)
					k++
					if _, err := mp.InsertTx(ctx, tx); err != nil {
						insertErrs.Add(1)
						return
					}
					inserted.Add(1)
				}
			}()
		}
		// Heights at the span's edges.
		time.Sleep(time.Until(spanFrom))
		v0From, fnFrom := apps[0].height.Load(), fnApp.height.Load()
		wg.Wait()
		v0To, fnTo := apps[0].height.Load(), fnApp.height.Load()
		vb, vtx := apps[0].window(spanFrom, stopAt)
		fb, ftx := fnApp.window(spanFrom, stopAt)
		share := -1.0
		if vb > 0 {
			share = float64(fb) / float64(vb)
		}
		fmt.Printf("ML chain_blocks=%d fullnode_blocks=%d chain_blocks_per_s=%.4f chain_tx_per_s=%.1f txs_per_block=%.1f fullnode_blocks_per_s=%.4f follow_share=%.4f lag_start=%d lag_end=%d v0_height=%d fullnode_height=%d inserted=%d span_s=%.0f\n",
			vb, fb, float64(vb)/span.Seconds(), float64(vtx)/span.Seconds(), float64(vtx)/float64(max(vb, 1)),
			float64(fb)/span.Seconds(), share, v0From-fnFrom, v0To-fnTo, v0To, fnTo, inserted.Load(), span.Seconds())
		_ = ftx
		// Agreement: the fullnode's last finalized height carries the AppHash validator 0 computed there (the testApp's
		// hash chains every block's hash, so equality covers every earlier block), its blocks are contiguous and name
		// committee proposers, and validator n-1 agrees with validator 0 at its own last height.
		// The fullnode follows whichever validator it dialed and can be ahead of validator 0, so the
		// comparison is at the lower of the two last heights.
		fnH := fnApp.lastH.Load()
		cmpH := min(fnH, apps[0].lastH.Load())
		fnHash, fnOK := fnApp.hashAt(cmpH)
		v0Hash, v0OK := apps[0].hashAt(cmpH)
		fnDig, fnDOK := fnApp.digestAt(cmpH)
		v0Dig, v0DOK := apps[0].digestAt(cmpH)
		fnAgree := fnOK && v0OK && cmpH > 0 && string(fnHash) == string(v0Hash)
		fnTxAgree := fnDOK && v0DOK && fnDig == v0Dig
		fnSnap := fnApp.Snapshot()
		fnContig := fnSnap.CheckBlocks() == nil
		vlH := min(apps[n-1].lastH.Load(), apps[0].lastH.Load())
		vlHash, vlOK := apps[n-1].hashAt(vlH)
		v0HashL, v0OKL := apps[0].hashAt(vlH)
		valAgree := vlOK && v0OKL && string(vlHash) == string(v0HashL)
		fmt.Printf("ML agree fullnode_height=%d compared_height=%d fullnode_apphash_equal=%t fullnode_txdigest_equal=%t fullnode_blocks_contiguous=%t validator_last_height=%d validator_apphash_equal=%t insert_errors=%d offered=%d\n",
			fnH, cmpH, fnAgree, fnTxAgree, fnContig, vlH, valAgree, insertErrs.Load(), int64(float64(rateTotal)*(warm+span).Seconds()))
		cancel()
		return nil
	})
	if err != nil && ctx.Err() == nil {
		require.NoError(t, err)
	}
}
