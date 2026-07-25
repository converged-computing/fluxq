// Package matcher is the boundary between the queue manager and Fluxion. It
// follows the resource model as GENERALIZED subsystems: every subsystem is a
// tree (containment, network, software, quantum, ...). Two independent axes:
//
//   - request set: every subsystem a job requests MUST satisfy for a cluster to
//     be a candidate (feasibility). This is a SATISFY traversal — side-effect
//     free — so Evaluate can fan out across the whole fleet in parallel and be
//     repeated freely (that is what /assess uses).
//   - countable vs descriptive: a subsystem is Descriptive=false (countable:
//     resources are consumed and must be scheduled/accounted, e.g. containment)
//     or Descriptive=true (satisfy-only: just confirm it exists, e.g. network).
//     Only countable subsystems are ALLOCATED, and only on the one cluster we
//     commit to, at dispatch time.
//
// Two implementations: SimMatcher (pure-Go traversal, runs anywhere) and
// FluxionMatcher (fluxion.go, //go:build fluxion; reapi bindings over the same
// JGF trees).
package matcher

import (
	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
)

// Allocation is a successful countable-subsystem match: the selected cluster
// plus the specific containment vertices consumed (so Free releases exactly
// those). The cluster is the root of the allocated containment path.
type Allocation struct {
	ID        string
	ClusterID string
	VertexIDs []string
}

// Candidate is one cluster's evaluation of a job. Evaluate returns one per
// registered cluster (feasible or not) so the manager can tell "no clusters"
// from "clusters but none feasible" from "feasible but full". Score is filled
// in by the Scorer, not the matcher.
type Candidate struct {
	Cluster   string   `json:"cluster"`
	Feasible  bool     `json:"feasible"` // all requested subsystems satisfy AND containment can structurally hold the job
	FreeNow   bool     `json:"free_now"` // countable capacity is available to run right now (best guess; Allocate is authority)
	FreeNodes int      `json:"free_nodes"`
	Matched   []string `json:"matched,omitempty"` // requested subsystems that satisfied
	Missing   []string `json:"missing,omitempty"` // requested subsystems (or "containment") that did not
	Score     float64  `json:"score"`
}

// Matcher is the fleet-level match seam.
type Matcher interface {
	// Evaluate runs satisfy-only across every cluster (in parallel) and reports
	// per-cluster feasibility + capacity facts. It allocates NOTHING, so it is
	// safe to call for /assess and every schedule tick.
	Evaluate(js jobspec.Jobspec) []Candidate
	// Allocate commits: MatchAllocate the countable subsystems (containment) on
	// exactly one chosen cluster. allocated=false with err=nil means "no room on
	// that cluster right now" (the job stays queued / the manager tries the next
	// candidate). err is reserved for genuine matcher/transport failures.
	Allocate(js jobspec.Jobspec, clusterID string) (alloc Allocation, allocated bool, err error)
	// Free releases a countable allocation.
	Free(allocID string) error
	// AddCluster / RemoveCluster manage fleet membership at runtime. A cluster
	// registers with its containment subsystem (Descriptive=false).
	AddCluster(cg graph.ClusterGraph) error
	RemoveCluster(id string) error
	// AddSubsystem / RemoveSubsystem attach a named subsystem tree to an existing
	// cluster. descriptive=true means satisfy-only (never allocated).
	AddSubsystem(clusterID, name string, g *graph.JGF, descriptive bool) error
	RemoveSubsystem(clusterID, name string) error
}
