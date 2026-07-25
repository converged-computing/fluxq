package vocabulary

import (
	"sort"
	"strconv"

	"github.com/converged-computing/fluxq/pkg/graph"
)

// MemoryBuckets is how many contiguous memory ranges the vocabulary aims for.
const MemoryBuckets = 3

// Dimension is one queryable axis the agent may classify a container along.
type Dimension struct {
	Name      string   `json:"name"`
	Mechanism string   `json:"mechanism"` // "subsystem" | "containment"
	Values    []string `json:"values"`
}

// Vocabulary is the fleet-derived label set handed to the selector agent. The
// agent reconciles a manifest's own terms against these values, best-effort.
type Vocabulary struct {
	Dimensions []Dimension `json:"dimensions"`
}

// distinctTypes returns the sorted distinct vertex types of a named subsystem
// across all clusters (the values that subsystem advertises fleet-wide).
func distinctTypes(clusters []graph.ClusterGraph, sub string) []string {
	seen := map[string]bool{}
	for _, cg := range clusters {
		g := cg.Subsystems[sub]
		if g == nil {
			continue
		}
		for i := range g.Graph.Nodes {
			if t := g.Graph.Nodes[i].Metadata.Type; t != "" {
				seen[t] = true
			}
		}
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// Derive computes the vocabulary from the registered fleet. A dimension appears
// only when it can actually discriminate: gpu-vendor only if >1 vendor, memory
// only if node sizes vary. Backend-agnostic — it reads whatever discovery (any
// driver) produced.
func Derive(clusters []graph.ClusterGraph) Vocabulary {
	var dims []Dimension

	if v := distinctTypes(clusters, "architecture"); len(v) > 0 {
		dims = append(dims, Dimension{"architecture", "subsystem", v})
	}
	if v := distinctTypes(clusters, "network"); len(v) > 0 {
		dims = append(dims, Dimension{"network", "subsystem", v})
	}
	// gpu vendor is a dimension ONLY if the fleet is heterogeneous; a single
	// vendor collapses to presence, matched in containment.
	if v := distinctTypes(clusters, "gpu"); len(v) > 1 {
		dims = append(dims, Dimension{"gpu", "subsystem", v})
	}
	// memory: contiguous quantile ranges over the raw per-cluster node sizes.
	var gb []int
	for _, raw := range distinctTypes(clusters, "memory-gb") {
		if n, err := strconv.Atoi(raw); err == nil {
			gb = append(gb, n)
		}
	}
	if ranges := MemoryRanges(gb, MemoryBuckets); len(ranges) > 0 {
		dims = append(dims, Dimension{"memory", "subsystem", ranges})
	}
	return Vocabulary{Dimensions: dims}
}
