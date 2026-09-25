//go:build benchharness

package p2p_test

import (
	"os"
	"runtime/pprof"
)

var node1ProfFile *os.File

// node1ProfStart starts a CPU profile into NODE1_CPUPROFILE when set.
func node1ProfStart() {
	path := os.Getenv("NODE1_CPUPROFILE")
	if path == "" {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		panic(err)
	}
	node1ProfFile = f
}

// node1ProfStop ends what node1ProfStart began.
func node1ProfStop() {
	if node1ProfFile != nil {
		pprof.StopCPUProfile()
		_ = node1ProfFile.Close()
		node1ProfFile = nil
	}
}
