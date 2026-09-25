package evmrpc

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/sei-protocol/sei-chain/sei-cosmos/client"
)

const (
	// defaultAwaitBound is the longest a receipt lookup waits for a pending transaction's receipt,
	// and the length of the one wait window each acceptance of a transaction opens.
	defaultAwaitBound = 4 * time.Second
	// awaitedTTL is how long after acceptance a transaction's receipt lookups still wait for it, and
	// how long after its wait window opened its lookups answer at once once the window has closed.
	awaitedTTL = time.Minute
	// maxAwaited bounds the transactions remembered at once.
	maxAwaited = 1 << 16
	// maxWaitingLookups bounds the lookups waiting at once; past it a lookup answers at once.
	maxWaitingLookups = 4096
	// awaitPollInterval is how often the watcher rereads the safe latest height while lookups wait.
	awaitPollInterval = 5 * time.Millisecond
	// awaitReadTimeout bounds each read of the safe latest height or of a block by the watcher.
	awaitReadTimeout = 500 * time.Millisecond
	// awaitStallAfter is how long a watcher may go without finishing a read before the next lookup
	// that waits starts another watcher in its place.
	awaitStallAfter = 2 * awaitReadTimeout
)

// awaitedTxs decides which receipt lookups wait for the receipt to become readable instead of
// answering null: those of a transaction this node knows is pending, because it accepted the
// transaction in the last awaitedTTL or the transaction is in its mempool. Each acceptance opens
// one wait window of the await bound, at its first waiting lookup; lookups after the window
// closes answer at once until the transaction is accepted again. It holds the remembered
// transactions, the lookups waiting now and the one watcher that wakes them.
type awaitedTxs struct {
	tmClient         client.LocalClient
	txConfigProvider func(int64) client.TxConfig
	watermarks       *WatermarkManager
	bound            time.Duration

	mu       sync.Mutex
	sent     map[common.Hash]rememberedTx
	order    []acceptedTx
	head     int
	waiting  map[common.Hash][]*awaitingLookup
	nWaiting int
	// watcher is the generation of the running watcher, 0 when none runs; a watcher whose
	// generation is no longer current exits when its read returns, without changing anything.
	watcher  uint64
	watchers uint64
	// beat is when the running watcher started or last finished a read, and scanned the highest
	// height whose lookups it woke.
	beat    time.Time
	scanned int64
}

// acceptedTx is one remembered transaction, in the order it was remembered.
type acceptedTx struct {
	hash common.Hash
	at   time.Time
}

// rememberedTx is when a transaction was remembered and when its wait window closes: zero until
// its first waiting lookup opens the window.
type rememberedTx struct {
	at    time.Time
	until time.Time
}

// awaitingLookup is one lookup waiting for its transaction until deadline; ready is closed when a
// readable height holds the transaction.
type awaitingLookup struct {
	hash     common.Hash
	deadline time.Time
	ready    chan struct{}
}

// sharedAwaited is one node's awaitedTxs and the number of its servers using it.
type sharedAwaited struct {
	awaited *awaitedTxs
	servers int
}

// nodeAwaited holds the awaitedTxs of each node client whose servers are in use, keyed by that
// client.
var nodeAwaited = struct {
	sync.Mutex
	byClient map[client.LocalClient]*sharedAwaited
}{byClient: make(map[client.LocalClient]*sharedAwaited)}

// acquireAwaitedTxs returns the awaitedTxs of tmClient's node, so that the node's HTTP and
// WebSocket servers wait on the same remembered transactions with one watcher, and a release
// that a server calls once it stops. The node's entry is dropped when its last server releases it.
func acquireAwaitedTxs(tmClient client.LocalClient, txConfigProvider func(int64) client.TxConfig, watermarks *WatermarkManager) (*awaitedTxs, func()) {
	if tmClient == nil || !reflect.TypeOf(tmClient).Comparable() {
		return newAwaitedTxs(tmClient, txConfigProvider, watermarks), func() {}
	}
	nodeAwaited.Lock()
	defer nodeAwaited.Unlock()
	shared, ok := nodeAwaited.byClient[tmClient]
	if !ok {
		shared = &sharedAwaited{awaited: newAwaitedTxs(tmClient, txConfigProvider, watermarks)}
		nodeAwaited.byClient[tmClient] = shared
	}
	shared.servers++
	var once sync.Once
	return shared.awaited, func() {
		once.Do(func() {
			nodeAwaited.Lock()
			defer nodeAwaited.Unlock()
			shared.servers--
			if shared.servers == 0 && nodeAwaited.byClient[tmClient] == shared {
				delete(nodeAwaited.byClient, tmClient)
			}
		})
	}
}

func newAwaitedTxs(tmClient client.LocalClient, txConfigProvider func(int64) client.TxConfig, watermarks *WatermarkManager) *awaitedTxs {
	return &awaitedTxs{
		tmClient:         tmClient,
		txConfigProvider: txConfigProvider,
		watermarks:       watermarks,
		bound:            defaultAwaitBound,
		sent:             make(map[common.Hash]rememberedTx),
		waiting:          make(map[common.Hash][]*awaitingLookup),
	}
}

// remember records that this node accepted hash now, with a wait window not yet opened,
// forgetting the oldest remembered transaction when maxAwaited are remembered.
func (a *awaitedTxs) remember(hash common.Hash) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.record(hash, time.Time{})
}

// record remembers hash now with the given wait window end. The caller holds a.mu.
func (a *awaitedTxs) record(hash common.Hash, until time.Time) {
	now := time.Now()
	a.forgetExpired(now)
	if len(a.order)-a.head >= maxAwaited {
		a.forgetOldest()
	}
	a.sent[hash] = rememberedTx{at: now, until: until}
	a.order = append(a.order, acceptedTx{hash: hash, at: now})
}

// forgetExpired drops the transactions remembered longer than awaitedTTL ago, oldest first.
func (a *awaitedTxs) forgetExpired(now time.Time) {
	for a.head < len(a.order) && now.Sub(a.order[a.head].at) > awaitedTTL {
		a.forgetOldest()
	}
}

// forgetOldest drops the oldest entry of the remembered order, and its hash unless the hash was
// remembered again since.
func (a *awaitedTxs) forgetOldest() {
	oldest := a.order[a.head]
	if tx, ok := a.sent[oldest.hash]; ok && tx.at.Equal(oldest.at) {
		delete(a.sent, oldest.hash)
	}
	a.order[a.head] = acceptedTx{}
	a.head++
	if a.head > len(a.order)/2 {
		a.order = append(a.order[:0], a.order[a.head:]...)
		a.head = 0
	}
}

// register starts a wait for hash and returns it, or returns nil when this node neither accepted
// hash in the last awaitedTTL nor holds it in its mempool, the wait window of hash's acceptance
// has closed, the waiting bound is reached, or the watcher cannot be started within the wait.
// The wait, including the read that starts the watcher, ends by the await bound after register
// is called, or at the close of an already open window, and the read ends with ctx. A lookup
// registers before it first reads the receipt, so any height the watcher has already passed was
// readable by that read. A non-nil result is released with unregister.
func (a *awaitedTxs) register(ctx context.Context, hash common.Hash) *awaitingLookup {
	if a == nil {
		return nil
	}
	deadline := time.Now().Add(a.bound)
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, known := a.remembered(hash); !known {
		if a.nWaiting >= maxWaitingLookups {
			return nil
		}
		// The mempool is read without the lock, which every send and lookup takes.
		a.mu.Unlock()
		pending := a.pending(hash)
		a.mu.Lock()
		if !pending {
			return nil
		}
	}
	if _, ok := a.waitUntil(hash, deadline); !ok || !a.startWatcher(ctx, deadline) {
		return nil
	}
	until, ok := a.waitUntil(hash, deadline)
	if !ok {
		return nil
	}
	if tx, known := a.remembered(hash); !known || tx.until.IsZero() {
		// The first waiting lookup of this acceptance opens its window.
		a.record(hash, until)
	}
	w := &awaitingLookup{hash: hash, deadline: until, ready: make(chan struct{})}
	a.waiting[hash] = append(a.waiting[hash], w)
	a.nWaiting++
	return w
}

// remembered returns hash's entry if it was remembered in the last awaitedTTL.
func (a *awaitedTxs) remembered(hash common.Hash) (rememberedTx, bool) {
	tx, ok := a.sent[hash]
	if !ok || time.Since(tx.at) > awaitedTTL {
		return rememberedTx{}, false
	}
	return tx, true
}

// waitUntil returns when a lookup of hash that this node knows is pending and starts waiting now
// stops waiting: at deadline, or at the close of the open window of hash's acceptance. It
// reports false when that window has closed or the waiting bound is reached.
func (a *awaitedTxs) waitUntil(hash common.Hash, deadline time.Time) (time.Time, bool) {
	if a.nWaiting >= maxWaitingLookups {
		return time.Time{}, false
	}
	tx, known := a.remembered(hash)
	if !known || tx.until.IsZero() {
		return deadline, true
	}
	return tx.until, time.Now().Before(tx.until)
}

// startWatcher makes sure a watcher runs, and reports whether one does. A watcher that has not
// finished a read for awaitStallAfter is replaced by one resuming from the height it reached. A
// new watcher starts from the safe latest height, read without the lock, which every send and
// lookup takes, and bounded by ctx and deadline. A watcher another lookup starts meanwhile read
// its own starting height before this one's. The caller holds a.mu.
func (a *awaitedTxs) startWatcher(ctx context.Context, deadline time.Time) bool {
	if a.watcher != 0 {
		if time.Since(a.beat) > awaitStallAfter {
			a.launchWatcher(a.scanned)
		}
		return true
	}
	a.mu.Unlock()
	readCtx, cancel := context.WithDeadline(ctx, deadline)
	scanned, err := a.watermarks.LatestHeight(readCtx)
	cancel()
	a.mu.Lock()
	if err != nil {
		return false
	}
	if a.watcher == 0 {
		a.launchWatcher(scanned)
	}
	return true
}

// launchWatcher starts a watcher of a new generation from the height after scanned. The caller
// holds a.mu.
func (a *awaitedTxs) launchWatcher(scanned int64) {
	a.watchers++
	a.watcher = a.watchers
	a.beat = time.Now()
	a.scanned = scanned
	go a.watch(a.watcher, scanned)
}

// pending reports whether hash is in this node's mempool, read as eth_getTransactionByHash
// reads it.
func (a *awaitedTxs) pending(hash common.Hash) bool {
	if a.tmClient == nil {
		return false
	}
	_, ok := a.tmClient.EvmTxByHash(hash)
	return ok
}

// unregister ends w's wait.
func (a *awaitedTxs) unregister(w *awaitingLookup) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nWaiting--
	lookups := a.waiting[w.hash]
	for i, other := range lookups {
		if other == w {
			lookups = append(lookups[:i], lookups[i+1:]...)
			break
		}
	}
	if len(lookups) == 0 {
		delete(a.waiting, w.hash)
	} else {
		a.waiting[w.hash] = lookups
	}
}

// wait blocks until w's transaction is in a readable height, ctx ends or w's deadline passes, and
// reports whether it became readable.
func (a *awaitedTxs) wait(ctx context.Context, w *awaitingLookup) bool {
	timer := time.NewTimer(time.Until(w.deadline))
	defer timer.Stop()
	select {
	case <-w.ready:
		return true
	case <-ctx.Done():
	case <-timer.C:
	}
	return false
}

// watch wakes the waiting lookups of each height as it becomes safe to serve, from the height
// after scanned, and returns once no lookup waits or a newer watcher replaced it. A height whose
// block cannot be read is read again on the next poll before any later height.
func (a *awaitedTxs) watch(gen uint64, scanned int64) {
	for a.running(gen) {
		time.Sleep(awaitPollInterval)
		latest, err := a.latestHeight()
		if !a.alive(gen) {
			return
		}
		if err != nil {
			continue
		}
		for scanned < latest {
			hashes, ok := a.blockEvmTxHashes(scanned + 1)
			if !ok {
				if !a.alive(gen) {
					return
				}
				break
			}
			if !a.advance(gen, scanned+1, hashes) {
				return
			}
			scanned++
		}
	}
}

// running reports whether the watcher of generation gen is current and a lookup waits, marking
// the watcher stopped when it is current and none waits.
func (a *awaitedTxs) running(gen uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.watcher != gen {
		return false
	}
	if a.nWaiting == 0 {
		a.watcher = 0
		return false
	}
	return true
}

// alive records that the watcher of generation gen finished a read, and reports whether it is
// still current.
func (a *awaitedTxs) alive(gen uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.watcher != gen {
		return false
	}
	a.beat = time.Now()
	return true
}

// advance wakes every lookup waiting for one of hashes, the transactions of height, if the
// watcher of generation gen is current, and reports whether it is.
func (a *awaitedTxs) advance(gen uint64, height int64, hashes []common.Hash) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.watcher != gen {
		return false
	}
	a.beat = time.Now()
	a.scanned = height
	for _, h := range hashes {
		for _, w := range a.waiting[h] {
			close(w.ready)
		}
		delete(a.waiting, h)
	}
	return true
}

// latestHeight reads the safe latest height within awaitReadTimeout.
func (a *awaitedTxs) latestHeight() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), awaitReadTimeout)
	defer cancel()
	return a.watermarks.LatestHeight(ctx)
}

// blockEvmTxHashes returns the Ethereum hashes of height's EVM transactions, and false when the
// block cannot be read within awaitReadTimeout.
func (a *awaitedTxs) blockEvmTxHashes(height int64) ([]common.Hash, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), awaitReadTimeout)
	defer cancel()
	block, err := a.tmClient.Block(ctx, &height)
	if err != nil || block == nil || block.Block == nil {
		return nil, false
	}
	decoder := a.txConfigProvider(height).TxDecoder()
	hashes := make([]common.Hash, 0, len(block.Block.Txs))
	for _, tx := range block.Block.Txs {
		if etx := getEthTxForTxBz(tx, decoder); etx != nil {
			hashes = append(hashes, etx.Hash())
		}
	}
	return hashes, true
}
