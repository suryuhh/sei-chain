package evmonlyapp

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethcore "github.com/ethereum/go-ethereum/core"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	gigaconfig "github.com/sei-protocol/sei-chain/giga/config"
	"github.com/sei-protocol/sei-chain/giga/evmonly"
	"github.com/sei-protocol/sei-chain/sei-db/bootstrap"
	seidbconfig "github.com/sei-protocol/sei-chain/sei-db/config"
	"github.com/sei-protocol/sei-chain/sei-db/proto"
	abci "github.com/sei-protocol/sei-chain/sei-tendermint/abci/types"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/require"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/scope"
	tmproto "github.com/sei-protocol/sei-chain/sei-tendermint/proto/tendermint/types"
)

// storeCode returns runtime bytecode that unconditionally SSTOREs value at
// key on every invocation.
func storeCode(key, value common.Hash) []byte {
	code := append([]byte{0x7f}, value.Bytes()...) // PUSH32 value
	code = append(code, 0x7f)                      // PUSH32 key
	code = append(code, key.Bytes()...)
	return append(code, 0x55, 0x00) // SSTORE, STOP
}

// initCode wraps runtime bytecode in the standard CODECOPY+RETURN preamble a
// contract-creation transaction executes to install it.
func initCode(runtime []byte) []byte {
	if len(runtime) > 255 {
		panic("test runtime too large")
	}
	runtimeLen := byte(len(runtime)) //nolint:gosec // bounded by the check above.
	code := []byte{
		0x60, runtimeLen,
		0x60, 0x0c,
		0x60, 0x00,
		0x39,
		0x60, runtimeLen,
		0x60, 0x00,
		0xf3,
	}
	return append(code, runtime...)
}

func signedEVMOnlyCreateTx(t *testing.T, chainID uint64, data []byte, gas uint64) (raw []byte, sender, contractAddr common.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender = crypto.PubkeyToAddress(key.PublicKey)
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{
		Nonce:    0,
		GasPrice: big.NewInt(evmOnlyMinGasPrice),
		Gas:      gas,
		Value:    big.NewInt(0),
		Data:     data,
	})
	signed, err := ethtypes.SignTx(tx, ethtypes.LatestSignerForChainID(new(big.Int).SetUint64(chainID)), key)
	require.NoError(t, err)
	raw, err = signed.MarshalBinary()
	require.NoError(t, err)
	return raw, sender, crypto.CreateAddress(sender, 0)
}

func callMessage(from common.Address, to *common.Address) *ethcore.Message {
	return &ethcore.Message{
		From:             from,
		To:               to,
		GasLimit:         100_000,
		GasPrice:         new(big.Int),
		GasFeeCap:        new(big.Int),
		GasTipCap:        new(big.Int),
		Value:            new(big.Int),
		SkipNonceChecks:  true,
		SkipFromEOACheck: true,
	}
}

func TestEVMOnlyApplicationEvmCallReadsCommittedContractCode(t *testing.T) {
	app := newInitializedEVMOnlyTestApp(t)
	evmApp := app.(*evmOnlyApplication)
	slot := common.BytesToHash([]byte{0x11})
	value := common.BytesToHash([]byte{0x22})
	runtime := storeCode(slot, value)
	deployRaw, sender, contractAddr := signedEVMOnlyCreateTx(t, evmOnlyTestChainID, initCode(runtime), 300_000)

	_, err := app.FinalizeBlock(t.Context(), &abci.RequestFinalizeBlock{
		Txs:  [][]byte{deployRaw},
		Hash: crypto.Keccak256([]byte("block-1")),
		Header: &tmproto.Header{
			Height: 1,
			Time:   time.Unix(1_700_000_001, 0),
		},
	})
	require.NoError(t, err)
	_, err = app.Commit(t.Context())
	require.NoError(t, err)

	result, err := evmApp.EvmCall(t.Context(), callMessage(sender, &contractAddr))

	require.NoError(t, err)
	require.False(t, result.Failed())
}

func TestEVMOnlyApplicationEvmCallDoesNotMutateCommittedState(t *testing.T) {
	app := newInitializedEVMOnlyTestApp(t)
	evmApp := app.(*evmOnlyApplication)
	slot := common.BytesToHash([]byte{0x44})
	writtenValue := common.BytesToHash([]byte{0x55})
	// This contract unconditionally SSTOREs on every invocation; EvmCall must
	// never let that write reach committed state.
	runtime := storeCode(slot, writtenValue)
	deployRaw, sender, contractAddr := signedEVMOnlyCreateTx(t, evmOnlyTestChainID, initCode(runtime), 300_000)

	_, err := app.FinalizeBlock(t.Context(), &abci.RequestFinalizeBlock{
		Txs:  [][]byte{deployRaw},
		Hash: crypto.Keccak256([]byte("block-1")),
		Header: &tmproto.Header{
			Height: 1,
			Time:   time.Unix(1_700_000_001, 0),
		},
	})
	require.NoError(t, err)
	_, err = app.Commit(t.Context())
	require.NoError(t, err)

	before := evmApp.storage.StateDB().OpenView()
	beforeValue := before.GetStorage(contractAddr, slot)
	before.Close()
	require.Equal(t, common.Hash{}, beforeValue)

	result, err := evmApp.EvmCall(t.Context(), callMessage(sender, &contractAddr))
	require.NoError(t, err)
	require.False(t, result.Failed())

	after := evmApp.storage.StateDB().OpenView()
	defer after.Close()
	require.Equal(t, common.Hash{}, after.GetStorage(contractAddr, slot))
}

func TestEVMOnlyApplicationEvmCallWaitsOutPendingCommit(t *testing.T) {
	app := newInitializedEVMOnlyTestApp(t)
	evmApp := app.(*evmOnlyApplication)
	chain := newBlockNumberChain(t)
	_, err := app.FinalizeBlock(t.Context(), chain.block(t, 1))
	require.NoError(t, err)

	// Until Commit, the store holds block 1's state while NUMBER is still 0.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = evmApp.EvmCall(ctx, chain.readMessage())
	require.ErrorIs(t, err, context.DeadlineExceeded)

	got, err := scope.Run1(t.Context(), func(ctx context.Context, s scope.Scope) (numberAndSlot, error) {
		call := scope.Spawn1(s, func() (numberAndSlot, error) { return chain.read(ctx, evmApp) })
		if _, err := app.Commit(ctx); err != nil {
			return numberAndSlot{}, err
		}
		return call.Join(ctx)
	})
	require.NoError(t, err)
	require.Equal(t, numberAndSlot{number: 1, slot: 1}, got)
}

func TestEVMOnlyApplicationAnswersCallsWhileABlockExecutes(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	encodes := 0
	// Parks FinalizeBlock of block 2 inside its execution, before its state changes are committed.
	app := newInitializedEVMOnlyTestAppWithEncoder(t, func(encode evmonly.NamedChangeSetEncoder) evmonly.NamedChangeSetEncoder {
		return func(changes evmonly.StateChangeSet) ([]*proto.NamedChangeSet, error) {
			if encodes++; encodes == 2 {
				close(entered)
				<-release
			}
			return encode(changes)
		}
	})
	evmApp := app.(*evmOnlyApplication)
	chain := newBlockNumberChain(t)
	finalizeAndCommitEVMOnlyTestBlock(t, app, chain.block(t, 1))

	var during numberAndSlot
	var estimateErr error
	err := scope.Run(t.Context(), func(ctx context.Context, s scope.Scope) error {
		s.Spawn(func() error {
			_, err := app.FinalizeBlock(ctx, chain.block(t, 2))
			return err
		})
		if _, _, err := utils.RecvOrClosed(ctx, entered); err != nil {
			return err
		}
		defer close(release)
		var err error
		if during, err = chain.read(ctx, evmApp); err != nil {
			return err
		}
		_, _, estimateErr = evmApp.EvmEstimateGas(ctx, chain.writeMessage(), 0)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, numberAndSlot{number: 1, slot: 1}, during)
	require.NoError(t, estimateErr)

	_, err = app.Commit(t.Context())
	require.NoError(t, err)
	after, err := chain.read(t.Context(), evmApp)
	require.NoError(t, err)
	require.Equal(t, numberAndSlot{number: 2, slot: 2}, after)
}

func TestEVMOnlyApplicationCallsReadEachBlocksOwnState(t *testing.T) {
	const blocks = 200
	const callers = 4
	app := newInitializedEVMOnlyTestApp(t)
	evmApp := app.(*evmOnlyApplication)
	chain := newBlockNumberChain(t)
	finalizeAndCommitEVMOnlyTestBlock(t, app, chain.block(t, 1))
	requests := make([]*abci.RequestFinalizeBlock, 0, blocks-1)
	for height := int64(2); height <= blocks; height++ {
		requests = append(requests, chain.block(t, height))
	}

	var answered atomic.Int64
	err := scope.Run(t.Context(), func(ctx context.Context, s scope.Scope) error {
		var done atomic.Bool
		for range callers {
			s.Spawn(func() error {
				for !done.Load() {
					got, err := chain.read(ctx, evmApp)
					if err != nil {
						return err
					}
					if got.number != got.slot {
						return fmt.Errorf("call at NUMBER %d read the state of block %d", got.number, got.slot)
					}
					if _, _, err := evmApp.EvmEstimateGas(ctx, chain.writeMessage(), 0); err != nil {
						return err
					}
					answered.Add(1)
				}
				return nil
			})
		}
		defer done.Store(true)
		for _, req := range requests {
			if _, err := app.FinalizeBlock(ctx, req); err != nil {
				return err
			}
			if _, err := app.Commit(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Positive(t, answered.Load())
}

func TestEVMOnlyApplicationExposesChainMetadata(t *testing.T) {
	app := newInitializedEVMOnlyTestApp(t)
	evmApp := app.(*evmOnlyApplication)

	// Test: the chain-id / config / fee / gas-limit accessors used by RPC.
	chainID := evmApp.EvmChainID()
	chainConfig := evmApp.EvmChainConfig()
	baseFee := evmApp.EvmBaseFee()
	gasLimit := evmApp.EvmGasLimit()

	// Verify: InitChain values, not the BaseApplication zeros.
	require.Equal(t, evmOnlyTestChainID, chainID)
	require.NotNil(t, chainConfig)
	require.Equal(t, new(big.Int).SetUint64(evmOnlyTestChainID), chainConfig.ChainID)
	require.Equal(t, big.NewInt(0), baseFee)
	require.Equal(t, uint64(30_000_000), gasLimit)
}

// blockNumberRuntime is runtime bytecode that, called with calldata, stores
// NUMBER in slot 0 and, called without, returns NUMBER and slot 0 as two words.
var blockNumberRuntime = []byte{
	0x36, 0x60, 0x13, 0x57, // CALLDATASIZE, PUSH1 0x13, JUMPI
	0x43, 0x60, 0x00, 0x52, // NUMBER, PUSH1 0, MSTORE
	0x60, 0x00, 0x54, 0x60, 0x20, 0x52, // PUSH1 0, SLOAD, PUSH1 32, MSTORE
	0x60, 0x40, 0x60, 0x00, 0xf3, // PUSH1 64, PUSH1 0, RETURN
	0x5b, 0x43, 0x60, 0x00, 0x55, 0x00, // JUMPDEST, NUMBER, PUSH1 0, SSTORE, STOP
}

// blockNumberChain builds blocks in which one account deploys
// blockNumberRuntime at height 1 and makes it record NUMBER in every block.
type blockNumberChain struct {
	key      *ecdsa.PrivateKey
	sender   common.Address
	contract common.Address
}

func newBlockNumberChain(t *testing.T) *blockNumberChain {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	return &blockNumberChain{key: key, sender: sender, contract: crypto.CreateAddress(sender, 0)}
}

// block returns the block at height: the deployment and the first record at
// height 1, one record at every later height.
func (c *blockNumberChain) block(t *testing.T, height int64) *abci.RequestFinalizeBlock {
	t.Helper()
	var txs [][]byte
	if height == 1 {
		txs = append(txs, c.sign(t, 0, nil, initCode(blockNumberRuntime)))
	}
	txs = append(txs, c.sign(t, uint64(height), &c.contract, []byte{1})) //nolint:gosec // test heights are positive.
	return &abci.RequestFinalizeBlock{
		Txs:  txs,
		Hash: crypto.Keccak256([]byte(fmt.Sprintf("block-%d", height))),
		Header: &tmproto.Header{
			Height: height,
			Time:   time.Unix(1_700_000_000+height, 0),
		},
	}
}

func (c *blockNumberChain) sign(t *testing.T, nonce uint64, to *common.Address, data []byte) []byte {
	t.Helper()
	tx := ethtypes.NewTx(&ethtypes.LegacyTx{
		Nonce:    nonce,
		GasPrice: big.NewInt(evmOnlyMinGasPrice),
		Gas:      300_000,
		To:       to,
		Value:    big.NewInt(0),
		Data:     data,
	})
	signed, err := ethtypes.SignTx(tx, ethtypes.LatestSignerForChainID(new(big.Int).SetUint64(evmOnlyTestChainID)), c.key)
	require.NoError(t, err)
	raw, err := signed.MarshalBinary()
	require.NoError(t, err)
	return raw
}

func (c *blockNumberChain) readMessage() *ethcore.Message {
	return callMessage(c.sender, &c.contract)
}

func (c *blockNumberChain) writeMessage() *ethcore.Message {
	msg := callMessage(c.sender, &c.contract)
	msg.Data = []byte{1}
	return msg
}

type numberAndSlot struct {
	number uint64
	slot   uint64
}

// read calls the contract and returns the NUMBER it ran at and the NUMBER the
// state it read was recorded at.
func (c *blockNumberChain) read(ctx context.Context, app *evmOnlyApplication) (numberAndSlot, error) {
	result, err := app.EvmCall(ctx, c.readMessage())
	if err != nil {
		return numberAndSlot{}, err
	}
	if result.Failed() || len(result.ReturnData) != 64 {
		return numberAndSlot{}, fmt.Errorf("read call failed: %v, %d bytes returned", result.Err, len(result.ReturnData))
	}
	return numberAndSlot{
		number: new(big.Int).SetBytes(result.ReturnData[:32]).Uint64(),
		slot:   new(big.Int).SetBytes(result.ReturnData[32:]).Uint64(),
	}, nil
}

func finalizeAndCommitEVMOnlyTestBlock(t *testing.T, app abci.Application, req *abci.RequestFinalizeBlock) {
	t.Helper()
	_, err := app.FinalizeBlock(t.Context(), req)
	require.NoError(t, err)
	_, err = app.Commit(t.Context())
	require.NoError(t, err)
}

// newInitializedEVMOnlyTestAppWithEncoder returns an initialized application
// whose executor encodes state changes through wrap applied to the default
// encoder.
func newInitializedEVMOnlyTestAppWithEncoder(
	t *testing.T,
	wrap func(evmonly.NamedChangeSetEncoder) evmonly.NamedChangeSetEncoder,
) abci.Application {
	t.Helper()
	storageConfig, err := seidbconfig.AutobahnStorageConfig(t.TempDir())
	require.NoError(t, err)
	storage, err := bootstrap.NewGigaStorageManager(t.Context(), storageConfig)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	app := NewEVMOnlyApplication(
		evmOnlyTestChainID,
		nil,
		storage,
		wrap(evmonly.NewFlatKVChangeSetEncoder(storage.SC())),
		gigaconfig.DefaultConfig.Execution,
	)
	_, err = app.InitChain(&abci.RequestInitChain{
		InitialHeight: 1,
		ConsensusParams: &tmproto.ConsensusParams{
			Block: &tmproto.BlockParams{MaxGas: 30_000_000},
		},
	})
	require.NoError(t, err)
	return app
}

func TestEVMOnlyApplicationEvmCallRequiresInitChain(t *testing.T) {
	app := newEVMOnlyTestApp(t, nil)
	evmApp := app.(*evmOnlyApplication)

	_, err := evmApp.EvmCall(t.Context(), callMessage(common.Address{}, nil))

	require.Error(t, err)
}
