//go:build benchharness

package evmonlyapp

import (
	"testing"

	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/sei-protocol/sei-chain/giga/evmonly"
	abci "github.com/sei-protocol/sei-chain/sei-tendermint/abci/types"
	"github.com/sei-protocol/sei-chain/sei-tendermint/libs/utils/require"
)

// nrForeignSenderKeys derives the sidecar entries CheckTx gives the producer without populating the measured app's cache.
func nrForeignSenderKeys(t *testing.T, app *evmOnlyApplication, w *nrWindow) [][]byte {
	t.Helper()
	keys := make([][]byte, len(w.raw))
	for i, raw := range w.raw {
		var tx ethtypes.Transaction
		require.NoError(t, tx.UnmarshalBinary(raw))
		sender, key, err := evmonly.RecoverSenderKey(&tx, ethtypes.LatestSignerForChainID(app.chainID))
		require.NoError(t, err)
		require.Equal(t, w.senders[i], sender)
		keys[i] = append(append([]byte(nil), key.PubKey...), key.SignatureRY...)
	}
	return keys
}

// nrCarrySenderKeys attaches the same aligned sender-key sidecar the producer carries for a foreign block.
func nrCarrySenderKeys(req *abci.RequestFinalizeBlock, keys [][]byte) {
	req.SenderKeys = keys
}
