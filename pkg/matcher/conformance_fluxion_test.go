//go:build fluxion

package matcher_test

// Runs the SAME conformance invariants and benchmark body as the sim
// (conformance_test.go) against the REAL FluxionMatcher, plus an isolation test
// that exercises a CUSTOM-typed subsystem — the case that failed when every
// context shared one process/interner. TestMain lets the test binary act as a
// worker subprocess when the supervisor re-execs it.

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/matcher"
)

func TestMain(m *testing.M) {
	matcher.RunWorkerIfRequested() // returns unless this is a worker subprocess
	os.Exit(m.Run())
}

func fluxionMk(f *graph.Fleet) (matcher.Matcher, error) { return matcher.NewFluxion(f) }

func TestFluxionConformance(t *testing.T) { runConformance(t, fluxionMk) }

func BenchmarkFluxionEvaluate(b *testing.B) { benchEvaluate(b, fluxionMk) }

// a standalone software subsystem graph typed by PACKAGE (custom types), in the
// containment-backbone shape reapi loads as its own context.
const swJGF = `{"graph":{"nodes":[
 {"id":"0","metadata":{"type":"cluster","basename":"cluster","name":"alpha","id":0,"uniq_id":0,"rank":-1,"size":1,"exclusive":false,"unit":"","paths":{"containment":"/alpha"}}},
 {"id":"1","metadata":{"type":"lammps","basename":"lammps","name":"lammps0","id":0,"uniq_id":1,"rank":-1,"size":1,"exclusive":false,"unit":"","paths":{"containment":"/alpha/lammps0"}}},
 {"id":"2","metadata":{"type":"kokkos","basename":"kokkos","name":"kokkos0","id":0,"uniq_id":2,"rank":-1,"size":1,"exclusive":false,"unit":"","paths":{"containment":"/alpha/lammps0/kokkos0"}}}
],"edges":[
 {"source":"0","target":"1","metadata":{"name":"contains","subsystem":"containment"}},
 {"source":"1","target":"2","metadata":{"name":"contains","subsystem":"containment"}}
]}}`

// TestFluxionCustomSubsystemIsolation proves the process-isolation fix: a
// custom-typed software subsystem loads and gates alongside a standard-typed
// containment context — which fails with "interner is finalized" if both share
// one process. Each is its own worker here, so it works.
func TestFluxionCustomSubsystemIsolation(t *testing.T) {
	m, err := matcher.NewFluxion(nil)
	if err != nil {
		t.Fatal(err)
	}
	cg, err := graph.ClusterSpec{Name: "alpha", Manager: "flux-operator",
		Nodes: []graph.NodeSpec{{Count: 1, Cores: 8}}}.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := m.AddCluster(cg); err != nil { // spawns the containment worker
		t.Fatalf("add cluster: %v", err)
	}
	var sw graph.JGF
	if err := json.Unmarshal([]byte(swJGF), &sw); err != nil {
		t.Fatal(err)
	}
	// This is the call that used to fail in-process (custom types after the
	// interner finalized on the containment context).
	if err := m.AddSubsystem("alpha", "software", &sw, true); err != nil {
		t.Fatalf("add custom-typed software subsystem: %v", err)
	}

	need := map[string][]jobspec.Resource{"software": {{Type: "lammps", Count: 1, With: []jobspec.Resource{{Type: "kokkos", Count: 1}}}}}
	haveJob := jobspec.New("has", "", []string{"true"}, 1, 4, time.Hour, need)
	cands := m.Evaluate(haveJob)
	if len(cands) != 1 || !cands[0].Feasible {
		t.Fatalf("alpha should be feasible (containment + software satisfied), got %+v", cands)
	}

	// A package the cluster lacks must gate the cluster out.
	needMissing := map[string][]jobspec.Resource{"software": {{Type: "gromacs", Count: 1}}}
	missJob := jobspec.New("miss", "", []string{"true"}, 1, 4, time.Hour, needMissing)
	for _, c := range m.Evaluate(missJob) {
		if c.Feasible {
			t.Fatalf("cluster must be infeasible for absent package, got feasible: %+v", c)
		}
	}

	// Delete-and-recreate: re-adding the subsystem (worker respawn) still works.
	if err := m.AddSubsystem("alpha", "software", &sw, true); err != nil {
		t.Fatalf("reload software subsystem: %v", err)
	}
	if cands := m.Evaluate(haveJob); len(cands) != 1 || !cands[0].Feasible {
		t.Fatalf("still feasible after reload, got %+v", cands)
	}
}
