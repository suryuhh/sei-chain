package evmrpc_test

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/require"

	"github.com/sei-protocol/sei-chain/evmrpc"
	"github.com/sei-protocol/sei-chain/sei-cosmos/client"
	sdk "github.com/sei-protocol/sei-chain/sei-cosmos/types"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils"
	"github.com/sei-protocol/sei-chain/sei-tendermint/rpc/coretypes"
	tmtypes "github.com/sei-protocol/sei-chain/sei-tendermint/types"
)

// risingLatestClient is MockClient with a latest height the test can raise while lookups run.
type risingLatestClient struct {
	*MockClient
	latest atomic.Int64
}

func (c *risingLatestClient) Status(context.Context) (*coretypes.ResultStatus, error) {
	return &coretypes.ResultStatus{
		SyncInfo: coretypes.SyncInfo{LatestBlockHeight: c.latest.Load(), EarliestBlockHeight: 1},
	}, nil
}

// block8Tx is a transaction of MockClient's block 8 whose receipt the test setup stores.
var block8Tx = common.HexToHash("0xa16d8f7ea8741acd23f15fc19b0dd26512aff68c01c6260d7c3a51b297399d32")

// newAwaitTxAPI returns a transaction API whose chain's latest height is 7, so block8Tx's receipt
// is stored but not yet readable.
func newAwaitTxAPI(t *testing.T) (*evmrpc.TransactionAPI, *risingLatestClient, common.Hash) {
	hash := block8Tx
	waitForReceipt(t, Ctx, hash)

	tmClient := &risingLatestClient{MockClient: &MockClient{}}
	tmClient.latest.Store(MockHeight8 - 1)
	return newTxAPIFor(t, tmClient), tmClient, hash
}

// newTxAPIFor returns a transaction API reading the chain through tmClient.
func newTxAPIFor(t *testing.T, tmClient client.LocalClient) *evmrpc.TransactionAPI {
	ctxProvider := func(int64) sdk.Context { return Ctx.WithIsTracing(true) }
	txConfigProvider := func(int64) client.TxConfig { return TxConfig }
	watermarks := evmrpc.NewWatermarkManager(tmClient, ctxProvider, nil, EVMKeeper.ReceiptStore())
	return evmrpc.NewTransactionAPI(tmClient, EVMKeeper, ctxProvider, txConfigProvider, t.TempDir(), evmrpc.ConnectionTypeHTTP, utils.None[time.Duration](), watermarks, evmrpc.NewBlockCache(8), &sync.Mutex{})
}

func TestReceiptLookupOfAcceptedTxAnswersWhenItBecomesReadable(t *testing.T) {
	txAPI, tmClient, hash := newAwaitTxAPI(t)
	unheld, unheldClient, _ := newAwaitTxAPI(t)
	before, err := unheld.GetTransactionReceipt(context.Background(), hash)
	require.NoError(t, err)
	require.Nil(t, before, "the receipt is stored but its block is not yet readable")
	txAPI.AwaitAcceptedForTest(hash, 4*time.Second)

	const readableAfter = 200 * time.Millisecond
	go func() {
		time.Sleep(readableAfter)
		tmClient.latest.Store(MockHeight8)
	}()
	start := time.Now()
	result, err := txAPI.GetTransactionReceipt(context.Background(), hash)
	elapsed := time.Since(start)

	// The held lookup answers what an unheld lookup answers once the block is readable.
	require.GreaterOrEqual(t, elapsed, readableAfter)
	require.Less(t, elapsed, readableAfter+time.Second)
	unheldClient.latest.Store(MockHeight8)
	want, wantErr := unheld.GetTransactionReceipt(context.Background(), hash)
	require.Equal(t, wantErr, err)
	require.Equal(t, want, result)
	require.False(t, result == nil && err == nil, "once readable the lookup no longer answers null")
}

func TestReceiptLookupOfAcceptedTxAnswersNullAtItsBound(t *testing.T) {
	txAPI, _, hash := newAwaitTxAPI(t)
	const bound = 300 * time.Millisecond
	txAPI.AwaitAcceptedForTest(hash, bound)

	start := time.Now()
	result, err := txAPI.GetTransactionReceipt(context.Background(), hash)
	require.NoError(t, err)
	require.Nil(t, result)
	require.GreaterOrEqual(t, time.Since(start), bound)
}

func TestReceiptLookupAfterAWaitReachedItsBoundAnswersAtOnce(t *testing.T) {
	txAPI, _, hash := newAwaitTxAPI(t)
	const bound = 300 * time.Millisecond
	txAPI.AwaitAcceptedForTest(hash, bound)

	result, err := txAPI.GetTransactionReceipt(context.Background(), hash)
	require.NoError(t, err)
	require.Nil(t, result)

	start := time.Now()
	result, err = txAPI.GetTransactionReceipt(context.Background(), hash)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Less(t, time.Since(start), bound/2)
}

func TestReceiptLookupOfUnacceptedTxAnswersAtOnce(t *testing.T) {
	txAPI, _, hash := newAwaitTxAPI(t)
	var other common.Hash
	_, err := rand.Read(other[:])
	require.NoError(t, err)
	txAPI.AwaitAcceptedForTest(other, 4*time.Second)

	start := time.Now()
	result, err := txAPI.GetTransactionReceipt(context.Background(), hash)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Less(t, time.Since(start), time.Second)
}

func TestReceiptLookupOfAcceptedTxEndsWithItsCaller(t *testing.T) {
	txAPI, _, hash := newAwaitTxAPI(t)
	txAPI.AwaitAcceptedForTest(hash, 4*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	result, err := txAPI.GetTransactionReceipt(ctx, hash)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Less(t, time.Since(start), time.Second)
}

// countingBlockClient is risingLatestClient counting its block reads.
type countingBlockClient struct {
	*risingLatestClient
	blockReads atomic.Int64
}

func (c *countingBlockClient) Block(ctx context.Context, h *int64) (*coretypes.ResultBlock, error) {
	c.blockReads.Add(1)
	return c.MockClient.Block(ctx, h)
}

func TestHeldLookupsReadEachReadableBlockOnceWhateverTheirNumber(t *testing.T) {
	rising := &risingLatestClient{MockClient: &MockClient{}}
	rising.latest.Store(MockHeight8 - 1)
	tmClient := &countingBlockClient{risingLatestClient: rising}
	ctxProvider := func(int64) sdk.Context { return Ctx.WithIsTracing(true) }
	txConfigProvider := func(int64) client.TxConfig { return TxConfig }
	watermarks := evmrpc.NewWatermarkManager(tmClient, ctxProvider, nil, EVMKeeper.ReceiptStore())
	txAPI := evmrpc.NewTransactionAPI(tmClient, EVMKeeper, ctxProvider, txConfigProvider, t.TempDir(), evmrpc.ConnectionTypeHTTP, utils.None[time.Duration](), watermarks, evmrpc.NewBlockCache(8), &sync.Mutex{})

	const waiters = 200
	hashes := make([]common.Hash, waiters)
	for i := range hashes {
		_, err := rand.Read(hashes[i][:])
		require.NoError(t, err)
		txAPI.AwaitAcceptedForTest(hashes[i], 500*time.Millisecond)
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		rising.latest.Store(MockHeight8)
	}()
	var wg sync.WaitGroup
	for _, h := range hashes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := txAPI.GetTransactionReceipt(context.Background(), h)
			require.NoError(t, err)
			require.Nil(t, result)
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), tmClient.blockReads.Load(), "one height became readable, so one block read serves every waiter")
}

// failingFirstBlockClient is risingLatestClient whose first block read fails.
type failingFirstBlockClient struct {
	*risingLatestClient
	failed atomic.Bool
}

func (c *failingFirstBlockClient) Block(ctx context.Context, h *int64) (*coretypes.ResultBlock, error) {
	if c.failed.CompareAndSwap(false, true) {
		return nil, errors.New("block store busy")
	}
	return c.MockClient.Block(ctx, h)
}

func TestHeldLookupAnswersWhenABlockReadFailsOnce(t *testing.T) {
	rising := &risingLatestClient{MockClient: &MockClient{}}
	rising.latest.Store(MockHeight8 - 1)
	tmClient := &failingFirstBlockClient{risingLatestClient: rising}
	txAPI := newTxAPIFor(t, tmClient)
	hash := block8Tx
	waitForReceipt(t, Ctx, hash)
	const bound = 3 * time.Second
	txAPI.AwaitAcceptedForTest(hash, bound)

	go func() {
		time.Sleep(100 * time.Millisecond)
		rising.latest.Store(MockHeight8)
	}()
	start := time.Now()
	result, err := txAPI.GetTransactionReceipt(context.Background(), hash)
	require.Less(t, time.Since(start), bound/2, "a failed block read is retried rather than skipped")
	require.False(t, result == nil && err == nil, "once readable the lookup no longer answers null")
}

// gatedStatusClient is risingLatestClient whose next status read, once armed, blocks until
// released.
type gatedStatusClient struct {
	*risingLatestClient
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (c *gatedStatusClient) Status(ctx context.Context) (*coretypes.ResultStatus, error) {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
	return c.risingLatestClient.Status(ctx)
}

func TestAcceptingATxDoesNotWaitOnAStartingWatcher(t *testing.T) {
	rising := &risingLatestClient{MockClient: &MockClient{}}
	rising.latest.Store(MockHeight8 - 1)
	tmClient := &gatedStatusClient{risingLatestClient: rising, entered: make(chan struct{}), release: make(chan struct{})}
	txAPI := newTxAPIFor(t, tmClient)
	hash := block8Tx
	waitForReceipt(t, Ctx, hash)
	txAPI.AwaitAcceptedForTest(hash, 200*time.Millisecond)

	tmClient.armed.Store(true)
	looked := make(chan struct{})
	go func() {
		defer close(looked)
		_, _ = txAPI.GetTransactionReceipt(context.Background(), hash)
	}()
	<-tmClient.entered

	var other common.Hash
	_, err := rand.Read(other[:])
	require.NoError(t, err)
	remembered := make(chan struct{})
	go func() {
		txAPI.RememberAcceptedForTest(other)
		close(remembered)
	}()
	select {
	case <-remembered:
	case <-time.After(time.Second):
	}
	accepted := false
	select {
	case <-remembered:
		accepted = true
	default:
	}
	close(tmClient.release)
	<-looked
	<-remembered
	require.True(t, accepted, "an accepted send is recorded while the watcher reads its starting height")
}

// mempoolClient is risingLatestClient whose mempool holds pending until the test drops it.
type mempoolClient struct {
	*risingLatestClient
	pending common.Hash
	held    atomic.Bool
}

func (c *mempoolClient) EvmTxByHash(hash common.Hash) (tmtypes.Tx, bool) {
	if c.held.Load() && hash == c.pending {
		return tmtypes.Tx("pending"), true
	}
	return nil, false
}

// newMempoolTxAPI returns a transaction API whose node holds block8Tx in its mempool, as a node
// that received it by gossip does, with the chain's latest height 7.
func newMempoolTxAPI(t *testing.T, bound time.Duration) (*evmrpc.TransactionAPI, *mempoolClient) {
	waitForReceipt(t, Ctx, block8Tx)
	tmClient := &mempoolClient{risingLatestClient: &risingLatestClient{MockClient: &MockClient{}}, pending: block8Tx}
	tmClient.latest.Store(MockHeight8 - 1)
	tmClient.held.Store(true)
	txAPI := newTxAPIFor(t, tmClient)
	txAPI.AwaitPendingForTest(bound)
	return txAPI, tmClient
}

func TestReceiptLookupOfMempoolTxNotSentHereAnswersWhenItBecomesReadable(t *testing.T) {
	txAPI, tmClient := newMempoolTxAPI(t, 4*time.Second)
	unheld, unheldClient, _ := newAwaitTxAPI(t)

	const readableAfter = 200 * time.Millisecond
	go func() {
		time.Sleep(readableAfter)
		tmClient.held.Store(false)
		tmClient.latest.Store(MockHeight8)
	}()
	start := time.Now()
	result, err := txAPI.GetTransactionReceipt(context.Background(), block8Tx)
	elapsed := time.Since(start)

	require.GreaterOrEqual(t, elapsed, readableAfter)
	require.Less(t, elapsed, readableAfter+time.Second)
	unheldClient.latest.Store(MockHeight8)
	want, wantErr := unheld.GetTransactionReceipt(context.Background(), block8Tx)
	require.Equal(t, wantErr, err)
	require.Equal(t, want, result)
	require.False(t, result == nil && err == nil, "once readable the lookup no longer answers null")
}

func TestReceiptLookupOfMempoolTxNeverAnswersAboveTheSafeHeight(t *testing.T) {
	txAPI, _ := newMempoolTxAPI(t, 300*time.Millisecond)

	start := time.Now()
	result, err := txAPI.GetTransactionReceipt(context.Background(), block8Tx)
	require.NoError(t, err)
	require.Nil(t, result, "the receipt is stored but its block is not yet safe to serve")
	require.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)

	start = time.Now()
	result, err = txAPI.GetTransactionReceipt(context.Background(), block8Tx)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Less(t, time.Since(start), 150*time.Millisecond, "a pending tx whose wait reached the bound answers at once")
}

func TestReceiptLookupOfUnknownTxAnswersAtOnce(t *testing.T) {
	txAPI, _ := newMempoolTxAPI(t, 4*time.Second)
	var unknown common.Hash
	_, err := rand.Read(unknown[:])
	require.NoError(t, err)

	start := time.Now()
	result, err := txAPI.GetTransactionReceipt(context.Background(), unknown)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Less(t, time.Since(start), time.Second)
}

// stalledStatusClient is risingLatestClient whose status reads, once stalled, block until their
// context ends.
type stalledStatusClient struct {
	*risingLatestClient
	stalled atomic.Bool
}

func (c *stalledStatusClient) Status(ctx context.Context) (*coretypes.ResultStatus, error) {
	if c.stalled.Load() {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return c.risingLatestClient.Status(ctx)
}

// lookupWithin runs a receipt lookup of hash and fails t unless it returns within limit.
func lookupWithin(t *testing.T, ctx context.Context, txAPI *evmrpc.TransactionAPI, hash common.Hash, limit time.Duration) (map[string]any, error, time.Duration) {
	type answer struct {
		result map[string]any
		err    error
	}
	done := make(chan answer, 1)
	start := time.Now()
	go func() {
		result, err := txAPI.GetTransactionReceipt(ctx, hash)
		done <- answer{result, err}
	}()
	select {
	case a := <-done:
		return a.result, a.err, time.Since(start)
	case <-time.After(limit):
		t.Fatalf("the lookup did not return within %v", limit)
		return nil, nil, 0
	}
}

func TestStartingTheWatcherEndsWithTheWaitBoundAndTheCaller(t *testing.T) {
	rising := &risingLatestClient{MockClient: &MockClient{}}
	rising.latest.Store(MockHeight8 - 1)
	tmClient := &stalledStatusClient{risingLatestClient: rising}
	tmClient.stalled.Store(true)
	txAPI := newTxAPIFor(t, tmClient)

	const bound = 300 * time.Millisecond
	var hash common.Hash
	_, err := rand.Read(hash[:])
	require.NoError(t, err)
	txAPI.AwaitAcceptedForTest(hash, bound)
	result, err, elapsed := lookupWithin(t, context.Background(), txAPI, hash, 2*time.Second)
	require.NoError(t, err)
	require.Nil(t, result)
	require.GreaterOrEqual(t, elapsed, bound)

	_, err = rand.Read(hash[:])
	require.NoError(t, err)
	txAPI.AwaitAcceptedForTest(hash, 4*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	result, err, elapsed = lookupWithin(t, ctx, txAPI, hash, 2*time.Second)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Less(t, elapsed, time.Second, "the starting read ends with the caller")
}

// hungWatcherClient is risingLatestClient whose second status read, the first the watcher makes,
// blocks whatever its context until the test ends.
type hungWatcherClient struct {
	*risingLatestClient
	reads   atomic.Int64
	hung    chan struct{}
	release chan struct{}
}

func (c *hungWatcherClient) Status(ctx context.Context) (*coretypes.ResultStatus, error) {
	if c.reads.Add(1) == 2 {
		close(c.hung)
		<-c.release
	}
	return c.risingLatestClient.Status(ctx)
}

func TestAHungWatcherIsReplacedByTheNextWaitingLookup(t *testing.T) {
	rising := &risingLatestClient{MockClient: &MockClient{}}
	rising.latest.Store(MockHeight8 - 1)
	tmClient := &hungWatcherClient{risingLatestClient: rising, hung: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(tmClient.release) })
	txAPI := newTxAPIFor(t, tmClient)
	waitForReceipt(t, Ctx, block8Tx)

	// A first lookup starts the watcher, whose first read of the safe height hangs.
	var first common.Hash
	_, err := rand.Read(first[:])
	require.NoError(t, err)
	const bound = 3 * time.Second
	txAPI.AwaitAcceptedForTest(first, bound)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = txAPI.GetTransactionReceipt(ctx, first)
	<-tmClient.hung
	time.Sleep(1200 * time.Millisecond)

	// A later lookup is still woken when its block becomes readable.
	txAPI.RememberAcceptedForTest(block8Tx)
	go func() {
		time.Sleep(100 * time.Millisecond)
		rising.latest.Store(MockHeight8)
	}()
	result, err, elapsed := lookupWithin(t, context.Background(), txAPI, block8Tx, 2*bound)
	require.False(t, result == nil && err == nil, "once readable the lookup no longer answers null")
	require.Less(t, elapsed, bound/2)
}

func TestAReacceptedTxWaitsAgainAfterAnEarlierLookupReachedItsBound(t *testing.T) {
	txAPI, tmClient, hash := newAwaitTxAPI(t)
	const bound = 400 * time.Millisecond
	txAPI.AwaitAcceptedForTest(hash, bound)

	// The transaction is sent again while a lookup of its first acceptance waits.
	go func() {
		time.Sleep(bound / 2)
		txAPI.RememberAcceptedForTest(hash)
	}()
	result, err := txAPI.GetTransactionReceipt(context.Background(), hash)
	require.NoError(t, err)
	require.Nil(t, result)

	// The earlier lookup's bound does not end the wait of the new acceptance.
	go func() {
		time.Sleep(100 * time.Millisecond)
		tmClient.latest.Store(MockHeight8)
	}()
	start := time.Now()
	result, err = txAPI.GetTransactionReceipt(context.Background(), hash)
	require.False(t, result == nil && err == nil, "a lookup of the new acceptance waits for the receipt")
	require.Less(t, time.Since(start), bound)
}

func TestLookupsOfOneAcceptanceShareOneWaitWindow(t *testing.T) {
	txAPI, _, hash := newAwaitTxAPI(t)
	const bound = time.Second
	txAPI.AwaitAcceptedForTest(hash, bound)
	opened := time.Now()

	// A lookup whose caller gives up opens the acceptance's window.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result, err := txAPI.GetTransactionReceipt(ctx, hash)
	require.NoError(t, err)
	require.Nil(t, result)

	// A later lookup waits only for the rest of that window.
	time.Sleep(bound/2 - time.Since(opened))
	result, err, elapsed := lookupWithin(t, context.Background(), txAPI, hash, 2*bound)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Less(t, elapsed, bound*3/4, "the window opened by the first lookup ends this one's wait")
	require.GreaterOrEqual(t, time.Since(opened), bound)

	// Once it closes, lookups answer at once until the transaction is accepted again.
	result, err, elapsed = lookupWithin(t, context.Background(), txAPI, hash, 2*bound)
	require.NoError(t, err)
	require.Nil(t, result)
	require.Less(t, elapsed, 100*time.Millisecond)
}
