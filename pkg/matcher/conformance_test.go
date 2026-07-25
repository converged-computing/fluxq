package matcher_test

// Cross-matcher conformance + benchmarks. The behavioral invariants here are
// the CONTRACT every Matcher must honor; runConformance is invoked against the
// SimMatcher (below) and, under -tags fluxion, against the real FluxionMatcher
// (conformance_fluxion_test.go), so the two implementations are proven to agree
// on the same JGF. Cases use CONTAINMENT-only jobspecs (standard resource
// types: cluster/rack/node/socket/core) because that is the countable path both
// matchers implement and — importantly — the only path real reapi will load:
// its JGF reader rejects unknown resource types, so a type-by-package software
// subsystem graph (which the sim accepts) will NOT InitContext. Subsystem-graph
// conformance therefore lives in the sim-only subsystem_test.go.

import (
	"fmt"
	"testing"
	"time"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/matcher"
)

// mkMatcher builds a Matcher over a fleet. Both NewSim and NewFluxion fit it.
type mkMatcher func(*graph.Fleet) (matcher.Matcher, error)

func simMk(f *graph.Fleet) (matcher.Matcher, error) { return matcher.NewSim(f), nil }

// buildFleet makes nClusters homogeneous clusters (standard containment types).
func buildFleet(nClusters, nodesPer, coresPer int) *graph.Fleet {
	f := graph.NewFleet()
	for i := 0; i < nClusters; i++ {
		cg, err := graph.ClusterSpec{
			Name:    fmt.Sprintf("c%03d", i),
			Manager: "flux-operator",
			Nodes:   []graph.NodeSpec{{Count: nodesPer, Cores: coresPer}},
		}.Build()
		if err != nil {
			panic(err)
		}
		f.Add(cg)
	}
	return f
}

// containmentJob is a pure Flux containment request (no requires) so BOTH
// matchers evaluate it identically and real reapi can load everything.
func containmentJob(nodes, coresPerNode int) jobspec.Jobspec {
	// A duration is set because real reapi MatchAllocate drives its planner over
	// time; allocation of a zero-walltime job does not reserve resources.
	return jobspec.New("bench", "", []string{"true"}, nodes, coresPerNode, time.Hour, nil)
}

func feasibleOnCluster(cands []matcher.Candidate, cluster string) (found, feasible bool) {
	for _, c := range cands {
		if c.Cluster == cluster {
			return true, c.Feasible
		}
	}
	return false, false
}

// runConformance asserts the invariants every Matcher must satisfy on the
// containment path. It is implementation-agnostic: it checks feasibility
// decisions and the allocate/free lifecycle, never implementation-specific
// details (exact vertex IDs, scores, or free-node counts).
func runConformance(t *testing.T, mk mkMatcher) {
	t.Helper()

	// Fleet: "small" holds 1x8, "big" holds 4x16, "solo" holds 1x8 (for the
	// allocate/free lifecycle in isolation).
	f := graph.NewFleet()
	for name, spec := range map[string]graph.NodeSpec{
		"small": {Count: 1, Cores: 8},
		"big":   {Count: 4, Cores: 16},
		"solo":  {Count: 1, Cores: 8},
	} {
		cg, err := graph.ClusterSpec{Name: name, Manager: "flux-operator", Nodes: []graph.NodeSpec{spec}}.Build()
		if err != nil {
			t.Fatal(err)
		}
		f.Add(cg)
	}
	m, err := mk(f)
	if err != nil {
		t.Fatalf("construct matcher: %v", err)
	}

	// (1) A job that fits is feasible; the same evaluation twice is stable
	// (Evaluate is side-effect free).
	fit := containmentJob(1, 4)
	c1 := m.Evaluate(fit)
	c2 := m.Evaluate(fit)
	for _, name := range []string{"small", "big", "solo"} {
		if _, f1 := feasibleOnCluster(c1, name); !f1 {
			t.Fatalf("%s should be feasible for 1x4", name)
		}
		if _, f2 := feasibleOnCluster(c2, name); !f2 {
			t.Fatalf("Evaluate not stable: %s feasibility changed on re-evaluate", name)
		}
	}

	// (2) Per-node too large: 1 node x 32 cores fits nowhere (max node is 16).
	for _, c := range m.Evaluate(containmentJob(1, 32)) {
		if c.Feasible {
			t.Fatalf("1x32 must be infeasible on %s (largest node is 16 cores)", c.Cluster)
		}
	}

	// (3) Too many nodes: 8 nodes fits nowhere (largest cluster has 4).
	for _, c := range m.Evaluate(containmentJob(8, 1)) {
		if c.Feasible {
			t.Fatalf("8-node job must be infeasible on %s", c.Cluster)
		}
	}

	// (4) Allocate/free lifecycle on "solo" (1 node): first allocate succeeds,
	// second is full (allocated=false, err=nil), free releases, then it fits
	// again. This is the countable-consumption contract.
	job := containmentJob(1, 8)
	a1, ok1, err := m.Allocate(job, "solo")
	if err != nil || !ok1 {
		t.Fatalf("first allocate on solo: ok=%v err=%v", ok1, err)
	}
	if _, ok2, err := m.Allocate(job, "solo"); err != nil || ok2 {
		t.Fatalf("second allocate must be full: ok=%v err=%v", ok2, err)
	}
	if err := m.Free(a1.ID); err != nil {
		t.Fatalf("free: %v", err)
	}
	if _, ok3, err := m.Allocate(job, "solo"); err != nil || !ok3 {
		t.Fatalf("allocate after free must succeed: ok=%v err=%v", ok3, err)
	}
}

func TestSimConformance(t *testing.T) { runConformance(t, simMk) }

// --- benchmarks: shared body, invoked for sim here and fluxion under the tag ---

func benchEvaluate(b *testing.B, mk mkMatcher) {
	job := containmentJob(4, 16)
	for _, n := range []int{10, 50, 100} {
		f := buildFleet(n, 16, 32)
		m, err := mk(f)
		if err != nil {
			b.Fatalf("construct: %v", err)
		}
		b.Run(fmt.Sprintf("clusters=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = m.Evaluate(job)
			}
		})
	}
}

func BenchmarkSimEvaluate(b *testing.B) { benchEvaluate(b, simMk) }
