//go:build !fluxion

package main

// maybeFluxionWorker is a no-op in the offline build (no reapi, no workers).
func maybeFluxionWorker() {}
