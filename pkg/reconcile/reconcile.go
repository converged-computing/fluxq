// Package reconcile handles the case where a job cannot run on the fleet as
// submitted. Instead of only failing it, a Reconciler inspects the intent and
// the fleet's real shapes and proposes a jobspec that WOULD fit.
//
// Safety principle: a reconciler may REPACK (change how work maps to resources
// — the nodes x tasks x cores factorization) but must PRESERVE THE WORK (total
// task count) and the subsystem requirements. It must never silently shrink the
// problem or substitute a fabric; those "relaxations" require judgment and are
// surfaced with caveats (the agent seam), never applied automatically. The
// manager treats a proposal as a SUGGESTION by default — the job still fails,
// carrying the proposed spec for a caller (human or agent) to accept.
package reconcile

import (
	"fmt"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
)

// NodeShape is a group of identical nodes on a cluster.
type NodeShape struct{ Cores, GPUs, MemGB, Count int }

// ClusterView is the reconciler's read-only summary of one cluster.
type ClusterView struct {
	ID     string
	Caps   map[string]bool // resource types present across the cluster's subsystem graphs
	Shapes []NodeShape
}

func (cv ClusterView) satisfies(js jobspec.Jobspec) bool {
	for _, t := range js.RequiredTypes() {
		if !cv.Caps[t] {
			return false
		}
	}
	return true
}

// FleetView is the whole fleet summarized for reconciliation.
type FleetView struct{ Clusters []ClusterView }

// BuildFleetView derives node shapes from containment and an advisory "caps"
// set from the cluster's AUXILIARY subsystem graphs (every vertex type/name in
// software/network/…), so the reconciler can decline missing-software cases
// without a flat capability-property list — everything is JGF here too.
func BuildFleetView(f *graph.Fleet) FleetView {
	var fv FleetView
	for _, cg := range f.Clusters() {
		caps := map[string]bool{}
		for name, g := range cg.Subsystems {
			if name == graph.ContainmentSubsystem || g == nil {
				continue
			}
			byID, _ := g.IndexExported()
			for _, v := range byID {
				if v.Metadata.Type != "" {
					caps[v.Metadata.Type] = true
				}
				if v.Metadata.Name != "" {
					caps[v.Metadata.Name] = true
				}
			}
		}
		fv.Clusters = append(fv.Clusters, ClusterView{ID: cg.ID, Caps: caps, Shapes: shapesOf(cg.Containment())})
	}
	return fv
}

func shapesOf(g *graph.JGF) []NodeShape {
	if g == nil {
		return nil
	}
	byID, children := g.IndexExported()
	type key struct{ c, gp, m int }
	counts := map[key]int{}
	var order []key
	for _, n := range g.VerticesOfTypeExported("node") {
		var c, gp, mem int
		var walk func(id string)
		walk = func(id string) {
			for _, cid := range children[id] {
				v := byID[cid]
				if v == nil {
					continue
				}
				size := v.Metadata.Size
				if size <= 0 {
					size = 1
				}
				switch v.Metadata.Type {
				case "core":
					c += size
				case "gpu":
					gp += size
				case "memory":
					mem += size
				}
				walk(cid)
			}
		}
		walk(n.ID)
		k := key{c, gp, mem}
		if _, seen := counts[k]; !seen {
			order = append(order, k)
		}
		counts[k]++
	}
	var out []NodeShape
	for _, k := range order {
		out = append(out, NodeShape{Cores: k.c, GPUs: k.gp, MemGB: k.m, Count: counts[k]})
	}
	return out
}

// Proposal is a suggested reconfiguration.
type Proposal struct {
	Jobspec     jobspec.Jobspec
	Rationale   string
	Relaxations []string // non-empty means the proposal changed semantics, not just layout
}

// Reconciler proposes a fitting jobspec, or ok=false if it cannot.
type Reconciler interface {
	Propose(js jobspec.Jobspec, view FleetView) (Proposal, bool)
}

// RepackReconciler is the deterministic, WORK-PRESERVING reconciler. It keeps
// total tasks, cores-per-task, GPU need, and all subsystem requirements, and
// only re-factors nodes x tasks-per-node to fit an available node shape. It
// never relaxes semantics, so it can help only capacity/shape impossibility
// (not missing software) — which is correct: relaxations are the agent's job.
type RepackReconciler struct{}

func (RepackReconciler) Propose(js jobspec.Jobspec, view FleetView) (Proposal, bool) {
	totalCores := js.Nodes() * max1(js.CoresPerNode())
	origN := js.Nodes()
	gpn, mpn := js.GPUsPerNode(), js.MemGBPerNode()

	for _, cv := range view.Clusters {
		if !cv.satisfies(js) {
			continue
		}
		for _, s := range cv.Shapes {
			if gpn > 0 && s.GPUs < gpn {
				continue
			}
			if s.MemGB < mpn {
				continue
			}
			if n, cpn, ok := repackOntoShape(totalCores, s); ok {
				np := js
				np.Resources = jobspec.Containment(n, cpn, gpn, mpn)
				return Proposal{
					Jobspec: np,
					Rationale: fmt.Sprintf(
						"repacked %d cores from %d nodes into %d nodes x %d cores/node to fit %q (%d cores/node); work preserved",
						totalCores, origN, n, cpn, cv.ID, s.Cores),
				}, true
			}
		}
	}
	return Proposal{}, false
}

// repackOntoShape spreads totalCores across a node count that DIVIDES it
// exactly (so total cores are preserved, not rounded up) and fits the shape.
// Prefers fewer nodes among feasible divisors. Returns nodes and cores-per-node.
func repackOntoShape(totalCores int, s NodeShape) (nodes, cpn int, ok bool) {
	if totalCores < 1 || s.Cores < 1 {
		return 0, 0, false
	}
	hi := s.Count
	if totalCores < hi {
		hi = totalCores
	}
	for n := 1; n <= hi; n++ {
		if totalCores%n != 0 {
			continue
		}
		if c := totalCores / n; c <= s.Cores {
			return n, c, true
		}
	}
	return 0, 0, false
}

func max1(v int) int {
	if v < 1 {
		return 1
	}
	return v
}

// AgentReconciler is the seam for SEMANTIC reconciliation. Where RepackReconciler
// only relayouts work, an agent would reason about the application to propose
// relaxations a deterministic rule must not take on its own — e.g. substituting
// a fabric (efa -> ethernet) with a performance caveat, choosing an alternate
// software build, or trading GPUs for CPUs — recording each in Proposal.Relaxations
// so the caller sees exactly what changed and why. Implement Propose with a model
// call over the job intent and FleetView.
type AgentReconciler struct{}

func (AgentReconciler) Propose(js jobspec.Jobspec, view FleetView) (Proposal, bool) {
	return Proposal{}, false // wire a model call here
}
