package vocabulary

import (
	"sort"

	"github.com/converged-computing/fluxq/pkg/graph"
)

// MemoryBuckets is how many contiguous memory ranges the vocabulary aims for.

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

// Values returns the sorted distinct vertex types a single subsystem graph
// advertises — the values a `requires` section can match against. The graph's
// cluster ROOT is skipped: it is structure Fluxion needs, not a value.
//
// This is the ONE place subsystem contents are read, so what /v1/clusters
// displays and what /v1/vocabulary derives can never disagree with what the
// matcher traverses.
func Values(g *graph.JGF) []string {
	if g == nil {
		return nil
	}
	seen := map[string]bool{}
	for i := range g.Graph.Nodes {
		t := g.Graph.Nodes[i].Metadata.Type
		if t == "" || t == "cluster" {
			continue
		}
		seen[t] = true
	}
	out := make([]string, 0, len(seen))
	for t := range seen {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// distinctTypes unions a named subsystem's values across the whole fleet.
func distinctTypes(clusters []graph.ClusterGraph, sub string) []string {
	seen := map[string]bool{}
	for _, cg := range clusters {
		for _, v := range Values(cg.Subsystems[sub]) {
			seen[v] = true
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
	// memory: the standard buckets clusters advertise. Reported like any other
	// dimension — NOT recomputed from raw node sizes, which would produce range
	// strings no cluster actually advertises.
	if v := distinctTypes(clusters, "memory"); len(v) > 0 {
		dims = append(dims, Dimension{"memory", "subsystem", v})
	}
	return Vocabulary{Dimensions: dims}
}
