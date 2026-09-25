//go:build benchharness

package evmonlyapp

import (
	"testing"

	abci "github.com/sei-protocol/sei-chain/sei-tendermint/abci/types"
)

// nrForeignSenderKeys returns no sidecar for the pre-sidecar application shape.
func nrForeignSenderKeys(_ *testing.T, _ *evmOnlyApplication, _ *nrWindow) [][]byte { return nil }

// nrCarrySenderKeys leaves a pre-sidecar request unchanged.
func nrCarrySenderKeys(_ *abci.RequestFinalizeBlock, _ [][]byte) {}
