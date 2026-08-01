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
	"github.com/converged-computing/fluxq/pkg/vocabulary"
)

// NodeFacts are grounded, absolute facts read off one node, in backend-neutral
// terms. Each backend's Discoverer is responsible for producing these however it
// reaches its nodes; the logic below is shared.
type NodeFacts struct {
	Arch      string // amd64 | arm64
	GPUVendor string // nvidia | amd | "" (none)
	Network   string // efa | ethernet | "" (unknown)
	MemoryGB  int    // allocatable memory, GiB (captured for vocabulary bucketing)
	Cores     int    // allocatable cpu, whole cores
	GPUs      int    // allocatable gpu count on this node
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
	minMem := 0
	for _, n := range nodes {
		memoryGB = append(memoryGB, n.MemoryGB)
		if n.MemoryGB > 0 && (minMem == 0 || n.MemoryGB < minMem) {
			minMem = n.MemoryGB
		}
	}
	// Memory is advertised as the standard BUCKET the cluster's smallest node
	// falls into — the same absolute buckets a jobspec requires. The raw size is
	// not registered as a subsystem: it is not a value anything matches on, and
	// a second memory dimension could only drift from this one.
	if minMem > 0 {
		if b := vocabulary.StandardRangeFor(minMem); b != "" {
			subsystems["memory"] = graph.SingletonSubsystem("memory", b)
		}
	}
	return subsystems, memoryGB, nil
}

// ContainmentFromFacts builds the CONSUMING graph (cluster -> nodes -> cores/gpus)
// from discovered nodes. Fluxion needs this base graph before any descriptive
// subsystem can be attached — without it the traverser cannot initialize and the
// cluster reports 0 nodes. Identical nodes are collapsed into one NodeSpec group.
func ContainmentFromFacts(clusterID string, m graph.ManagerType, handle string,
	nodes []NodeFacts) *graph.JGF {
	type key struct{ cores, gpus, mem int }
	order := []key{}
	count := map[key]int{}
	for _, n := range nodes {
		k := key{n.Cores, n.GPUs, n.MemoryGB}
		if _, seen := count[k]; !seen {
			order = append(order, k)
		}
		count[k]++
	}
	groups := make([]graph.NodeSpec, 0, len(order))
	for _, k := range order {
		groups = append(groups, graph.NodeSpec{
			Count: count[k], Cores: k.cores, GPUs: k.gpus, MemGB: k.mem,
		})
	}
	// No capability properties: the descriptive subsystems are the single source
	// of truth for what a cluster advertises (see vocabulary.Values).
	return graph.BuildContainment(clusterID, m, handle, groups, nil)
}
