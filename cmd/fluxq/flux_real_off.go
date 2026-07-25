//go:build !fluxcore

package main

import "github.com/converged-computing/fluxq/pkg/cluster"

// realFluxDriver returns nil in the default build: Flux is emulated-only unless
// built with -tags fluxcore (real libflux dispatch).
func realFluxDriver() cluster.Driver { return nil }
