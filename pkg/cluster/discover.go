package cluster

// Backend-agnostic discovery core. A driver that can introspect its cluster
// yields backend-neutral NodeFacts (see the Discoverer interface); this file
// turns those facts into descriptive subsystems, uniformly for every backend.
//
// It records ABSOLUTE, per-cluster facts only — architecture, gpu vendor,
// network fabric. Fleet-relative categories (memory buckets) are NOT decided
// here; /v1/vocabulary computes them from raw node memory across the whole fleet.

import (
	"fmt"

	"github.com/converged-computing/fluxq/pkg/graph"
)

// NodeFacts are grounded, absolute facts read off one node, in backend-neutral
// terms. Each backend's Discoverer is responsible for producing these however it
// reaches its nodes; the logic below is shared.
type NodeFacts struct {
	Arch      string // amd64 | arm64
	GPUVendor string // nvidia | amd | "" (none)
	Network   string // efa | ethernet | "" (unknown)
	MemoryGB  int    // allocatable memory, GiB (captured for vocabulary bucketing)
}

// SubsystemsFromFacts aggregates node facts into descriptive subsystem graphs,
// one per category that is present and uniform across the cluster. Returns the
// subsystems to register and the per-node memory sizes (for fleet-level
// bucketing done elsewhere). This is backend-independent.
func SubsystemsFromFacts(nodes []NodeFacts) (map[string]*graph.JGF, []int, error) {
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no nodes discovered")
	}
	subsystems := map[string]*graph.JGF{}
	add := func(sub string, pick func(NodeFacts) string) {
		seen := map[string]bool{}
		for _, n := range nodes {
			if v := pick(n); v != "" {
				seen[v] = true
			}
		}
		if len(seen) == 1 { // uniform + present -> a matchable subsystem
			for v := range seen {
				subsystems[sub] = graph.SingletonSubsystem(sub, v)
			}
		}
		// absent (0) or mixed pool (>1) -> omit; don't guess.
	}
	add("architecture", func(n NodeFacts) string { return n.Arch })
	add("gpu", func(n NodeFacts) string { return n.GPUVendor })
	add("network", func(n NodeFacts) string { return n.Network })

	memoryGB := make([]int, 0, len(nodes))
	for _, n := range nodes {
		memoryGB = append(memoryGB, n.MemoryGB)
	}
	return subsystems, memoryGB, nil
}
