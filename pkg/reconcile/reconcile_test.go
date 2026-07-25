package reconcile_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/matcher"
	"github.com/converged-computing/fluxq/pkg/reconcile"
)

func anyFeasible(m matcher.Matcher, js jobspec.Jobspec) bool {
	for _, c := range m.Evaluate(js) {
		if c.Feasible {
			return true
		}
	}
	return false
}

func fleet(t *testing.T) *graph.Fleet {
	t.Helper()
	root := t.TempDir()
	if err := graph.ExportCluster(root, "big", graph.BuildContainment("big", graph.K8sJob, "ctx", []graph.NodeSpec{{Count: 5, Cores: 64, MemGB: 128}}, []string{"lammps"})); err != nil {
		t.Fatal(err)
	}
	f, err := graph.DirectoryLoader{}.LoadFleet(root)
	if err != nil {
		t.Fatal(err)
	}
	return swFleet(t, f)
}

// A 99-node x 1-task job (99 tasks) fits nowhere as submitted (no cluster has
// 99 nodes), but repacks to 3 nodes x 33 tasks on the 64-core cluster, and the
// proposal both preserves the 99 tasks AND passes Satisfy.
func TestRepackPreservesWorkAndFits(t *testing.T) {
	f := fleet(t)
	js := jobspec.New("too-big", "", []string{"lmp"}, 99, 1, 0,
		map[string][]jobspec.Resource{"software": {{Type: "lammps"}}})
	m := matcher.NewSim(f)
	if anyFeasible(m, js) {
		t.Fatal("precondition: original should be infeasible")
	}
	prop, ok := reconcile.RepackReconciler{}.Propose(js, reconcile.BuildFleetView(f))
	if !ok {
		t.Fatal("expected a repack proposal")
	}
	if got := prop.Jobspec.Nodes() * prop.Jobspec.CoresPerNode(); got != 99 {
		t.Fatalf("work not preserved: proposed %d total cores, want 99", got)
	}
	if !anyFeasible(m, prop.Jobspec) {
		t.Fatalf("proposed spec must be feasible: %d nodes x %d cores", prop.Jobspec.Nodes(), prop.Jobspec.CoresPerNode())
	}
	if len(prop.Relaxations) != 0 {
		t.Fatalf("repack must not relax semantics, got %v", prop.Relaxations)
	}
}

// Missing software can't be repacked away — that needs the agent seam.
func TestRepackDeclinesMissingSoftware(t *testing.T) {
	f := fleet(t)
	js := jobspec.New("nope", "", nil, 1, 1, 0,
		map[string][]jobspec.Resource{"software": {{Type: "nonesuch"}}})
	if _, ok := (reconcile.RepackReconciler{}).Propose(js, reconcile.BuildFleetView(f)); ok {
		t.Fatal("repack should decline when required software is absent")
	}
}

// swFleet converts each cluster's capability set (test fixture sugar) into a
// real "software" subsystem JGF graph and attaches it, so tests exercise the
// JGF-only matching path (no capability-property fallback exists anymore).
func swFleet(t *testing.T, f *graph.Fleet) *graph.Fleet {
	t.Helper()
	for _, cg := range f.Clusters() {
		var nodes []string
		i := 0
		for cap := range cg.Capabilities() {
			i++
			nodes = append(nodes, fmt.Sprintf(`{"id":"sw%d","metadata":{"type":"software","name":%q,"basename":%q,"id":0,"uniq_id":%d,"size":1,"paths":{"software":"/%s"}}}`, i, cap, cap, i, cap))
		}
		var g graph.JGF
		if err := json.Unmarshal([]byte(`{"graph":{"nodes":[`+strings.Join(nodes, ",")+`],"edges":[]}}`), &g); err != nil {
			t.Fatal(err)
		}
		cg.AttachSubsystem("software", &g, true)
		f.Add(cg)
	}
	return f
}
