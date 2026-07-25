//go:build fluxion

package main

import (
	"log"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/matcher"
)

// newMatcher (-tags fluxion) returns the REAL Fluxion matcher (flux-sched reapi).
func newMatcher(logger *log.Logger, f *graph.Fleet) matcher.Matcher {
	m, err := matcher.NewFluxion(f)
	if err != nil {
		logger.Fatalf("init real Fluxion: %v", err)
	}
	logger.Println("matcher: REAL Fluxion (flux-sched reapi bindings)")
	return m
}
