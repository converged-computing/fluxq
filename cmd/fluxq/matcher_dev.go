//go:build !fluxion

package main

import (
	"log"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/matcher"
)

// newMatcher (default build) returns the OFFLINE DEV DOUBLE. It is not Fluxion.
// Build with -tags fluxion in the flux-sched devcontainer to use real Fluxion.
func newMatcher(logger *log.Logger, f *graph.Fleet) matcher.Matcher {
	logger.Println("matcher: DEV DOUBLE (not Fluxion) — build -tags fluxion for real Fluxion")
	return matcher.NewSim(f)
}
