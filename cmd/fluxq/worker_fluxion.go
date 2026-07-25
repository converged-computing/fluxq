//go:build fluxion

package main

import "github.com/converged-computing/fluxq/pkg/matcher"

// maybeFluxionWorker turns this process into a reapi worker when invoked with the
// worker sentinel arg (used by the FluxionMatcher supervisor to isolate each
// reapi context in its own process). It returns immediately for normal runs.
func maybeFluxionWorker() { matcher.RunWorkerIfRequested() }
