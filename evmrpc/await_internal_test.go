package evmrpc

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/require"

	"github.com/sei-protocol/sei-chain/sei-cosmos/client"
)

func acceptedHash(i int) common.Hash {
	var h common.Hash
	binary.BigEndian.PutUint64(h[:8], uint64(i)) //nolint:gosec // G115: test indices are non-negative.
	return h
}

func TestRememberAtTheCapForgetsTheOldestAndKeepsTheNewest(t *testing.T) {
	a := newAwaitedTxs(nil, nil, nil)
	for i := range maxAwaited + 10 {
		a.remember(acceptedHash(i))
	}
	require.Len(t, a.sent, maxAwaited)
	require.NotContains(t, a.sent, acceptedHash(9))
	require.Contains(t, a.sent, acceptedHash(10))
	require.Contains(t, a.sent, acceptedHash(maxAwaited+9))
	require.LessOrEqual(t, len(a.order)-a.head, maxAwaited)
}

func TestRememberForgetsExpiredAcceptances(t *testing.T) {
	a := newAwaitedTxs(nil, nil, nil)
	a.remember(acceptedHash(1))
	a.order[a.head].at = a.order[a.head].at.Add(-2 * awaitedTTL)
	a.sent[acceptedHash(1)] = rememberedTx{at: a.order[a.head].at}
	a.remember(acceptedHash(2))
	require.NotContains(t, a.sent, acceptedHash(1))
	require.Contains(t, a.sent, acceptedHash(2))
}

func TestRememberAgainOutlivesTheEarlierAcceptance(t *testing.T) {
	a := newAwaitedTxs(nil, nil, nil)
	a.remember(acceptedHash(1))
	time.Sleep(time.Millisecond)
	a.remember(acceptedHash(1))
	a.forgetOldest()
	require.Contains(t, a.sent, acceptedHash(1))
}

// BenchmarkRememberAtTheCap is one accepted send with maxAwaited unexpired acceptances remembered.
func BenchmarkRememberAtTheCap(b *testing.B) {
	a := newAwaitedTxs(nil, nil, nil)
	for i := range maxAwaited {
		a.remember(acceptedHash(i))
	}
	b.ResetTimer()
	for i := range b.N {
		a.remember(acceptedHash(maxAwaited + i))
	}
}

func TestRememberAfterOpenedWindowsKeepsTheAcceptanceOrderBounded(t *testing.T) {
	a := newAwaitedTxs(nil, nil, nil)
	for i := range maxAwaited {
		a.remember(acceptedHash(i))
	}
	for i := range 10 {
		a.mu.Lock()
		a.record(acceptedHash(i), time.Now().Add(a.bound))
		a.mu.Unlock()
	}
	for i := range 10 {
		a.remember(acceptedHash(maxAwaited + i))
	}
	require.LessOrEqual(t, len(a.order)-a.head, maxAwaited)
	require.LessOrEqual(t, len(a.sent), maxAwaited)
}

func TestServersOfOneNodeShareTheirAcceptedTxsUntilTheLastStops(t *testing.T) {
	node := &fakeTMClient{}
	httpAwaited, releaseHTTP := acquireAwaitedTxs(node, nil, nil)
	wsAwaited, releaseWS := acquireAwaitedTxs(node, nil, nil)
	require.Same(t, httpAwaited, wsAwaited)
	other, releaseOther := acquireAwaitedTxs(&fakeTMClient{}, nil, nil)
	require.NotSame(t, httpAwaited, other)
	releaseOther()

	releaseHTTP()
	releaseHTTP()
	nodeAwaited.Lock()
	require.Contains(t, nodeAwaited.byClient, client.LocalClient(node), "the WebSocket server still uses the node's set")
	nodeAwaited.Unlock()
	releaseWS()
	nodeAwaited.Lock()
	require.NotContains(t, nodeAwaited.byClient, client.LocalClient(node))
	nodeAwaited.Unlock()

	restarted, releaseRestarted := acquireAwaitedTxs(node, nil, nil)
	defer releaseRestarted()
	require.NotSame(t, httpAwaited, restarted)
}

func TestStoppingAServerReleasesItsAcceptedTxs(t *testing.T) {
	node := &fakeTMClient{}
	_, release := acquireAwaitedTxs(node, nil, nil)
	server := NewHTTPServer(rpc.HTTPTimeouts{})
	server.releaseOnStop(release)
	server.Stop()
	nodeAwaited.Lock()
	defer nodeAwaited.Unlock()
	require.NotContains(t, nodeAwaited.byClient, client.LocalClient(node))
}
