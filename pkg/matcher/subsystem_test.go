package matcher_test

import (
	"encoding/json"
	"testing"

	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
	"github.com/converged-computing/fleetq/pkg/matcher"
)

// software subsystem tree: lammps -> kokkos  (no mpi vertex anywhere).
const swTree = `{"graph":{"nodes":[
 {"id":"1","metadata":{"type":"software","basename":"lammps","name":"lammps","id":0,"uniq_id":1,"size":1,"paths":{"software":"/lammps"}}},
 {"id":"2","metadata":{"type":"software","basename":"kokkos","name":"kokkos","id":0,"uniq_id":2,"size":1,"paths":{"software":"/lammps/kokkos"}}}
],"edges":[
 {"source":"1","target":"2","metadata":{"subsystem":"software","name":"depends"}}
]}}`

func oneNodeCluster(t *testing.T, id string) graph.ClusterGraph {
	t.Helper()
	cg, err := graph.ClusterSpec{
		Name: id, Manager: "flux-operator",
		Nodes: []graph.NodeSpec{{Count: 1, Cores: 8}},
	}.Build()
	if err != nil {
		t.Fatal(err)
	}
	return cg
}

func need(sub string, section []jobspec.Resource) jobspec.Jobspec {
	return jobspec.New("j", "", nil, 1, 1, 0, map[string][]jobspec.Resource{sub: section})
}

func feasibleOn(m matcher.Matcher, js jobspec.Jobspec, cluster string) bool {
	for _, c := range m.Evaluate(js) {
		if c.Cluster == cluster {
			return c.Feasible
		}
	}
	return false
}

// The dependency must match structurally: lammps WITH kokkos satisfies the
// lammps->kokkos tree, but lammps WITH mpi does not (mpi is absent). This is the
// exact gate Fluxion's reapi got wrong; the sim is strict by construction.
func TestSubsystemSubtreeGate(t *testing.T) {
	m := matcher.NewSim(nil)
	if err := m.AddCluster(oneNodeCluster(t, "c1")); err != nil {
		t.Fatal(err)
	}
	var tree graph.JGF
	if err := json.Unmarshal([]byte(swTree), &tree); err != nil {
		t.Fatal(err)
	}
	if err := m.AddSubsystem("c1", "software", &tree, true); err != nil {
		t.Fatal(err)
	}

	lammpsKokkos := need("software", []jobspec.Resource{
		{Type: "lammps", With: []jobspec.Resource{{Type: "kokkos"}}},
	})
	if !feasibleOn(m, lammpsKokkos, "c1") {
		t.Fatal("lammps WITH kokkos should satisfy the lammps->kokkos tree")
	}

	lammpsMPI := need("software", []jobspec.Resource{
		{Type: "lammps", With: []jobspec.Resource{{Type: "mpi"}}},
	})
	if feasibleOn(m, lammpsMPI, "c1") {
		t.Fatal("lammps WITH mpi must NOT satisfy (mpi absent from the tree)")
	}
}

// A descriptive subsystem gates feasibility but is never consumed: Satisfy
// allocates nothing, and Allocate only touches containment.
func TestDescriptiveSubsystemNotConsumed(t *testing.T) {
	m := matcher.NewSim(nil)
	if err := m.AddCluster(oneNodeCluster(t, "c1")); err != nil {
		t.Fatal(err)
	}
	var tree graph.JGF
	_ = json.Unmarshal([]byte(swTree), &tree)
	_ = m.AddSubsystem("c1", "software", &tree, true)

	js := need("software", []jobspec.Resource{{Type: "lammps"}})

	// Evaluate twice: side-effect free, so both see the node free.
	if !m.Evaluate(js)[0].FreeNow || !m.Evaluate(js)[0].FreeNow {
		t.Fatal("Evaluate must not consume capacity")
	}
	// Allocate consumes only containment (one node); a second allocate is full.
	alloc, ok, err := m.Allocate(js, "c1")
	if err != nil || !ok {
		t.Fatalf("allocate failed: ok=%v err=%v", ok, err)
	}
	if len(alloc.VertexIDs) != 1 {
		t.Fatalf("expected 1 containment vertex consumed, got %d", len(alloc.VertexIDs))
	}
	if _, ok, _ := m.Allocate(js, "c1"); ok {
		t.Fatal("second allocate should be full (one node)")
	}
}

// network subsystem tree with a single provider vertex (efa).
const netTreeEFA = `{"graph":{"nodes":[
 {"id":"1","metadata":{"type":"network","basename":"efa","name":"efa","id":0,"uniq_id":1,"size":1,"paths":{"network":"/efa"}}}
],"edges":[]}}`

// anyof: a section is satisfied if ANY alternative is. A libfabric-style build
// that can run over several interconnects requests anyof(efa, infiniband,
// ethernet); a cluster offering efa satisfies it, one offering only ethernet
// (here: nothing but efa registered, request without efa) does not.
func TestSubsystemAnyOf(t *testing.T) {
	m := matcher.NewSim(nil)
	if err := m.AddCluster(oneNodeCluster(t, "c1")); err != nil {
		t.Fatal(err)
	}
	var net graph.JGF
	if err := json.Unmarshal([]byte(netTreeEFA), &net); err != nil {
		t.Fatal(err)
	}
	if err := m.AddSubsystem("c1", "network", &net, true); err != nil {
		t.Fatal(err)
	}

	anyEFAorIB := need("network", []jobspec.Resource{
		{Type: jobspec.AnyOfType, With: []jobspec.Resource{{Type: "efa"}, {Type: "infiniband"}}},
	})
	if !feasibleOn(m, anyEFAorIB, "c1") {
		t.Fatal("anyof(efa, infiniband) must satisfy a cluster that has efa")
	}

	anyIBorEth := need("network", []jobspec.Resource{
		{Type: jobspec.AnyOfType, With: []jobspec.Resource{{Type: "infiniband"}, {Type: "ethernet"}}},
	})
	if feasibleOn(m, anyIBorEth, "c1") {
		t.Fatal("anyof(infiniband, ethernet) must NOT satisfy an efa-only cluster")
	}

	// AND still holds alongside anyof: software lammps->kokkos AND net anyof.
	var sw graph.JGF
	_ = json.Unmarshal([]byte(swTree), &sw)
	_ = m.AddSubsystem("c1", "software", &sw, true)
	combo := jobspec.New("j", "", nil, 1, 1, 0, map[string][]jobspec.Resource{
		"software": {{Type: "lammps", With: []jobspec.Resource{{Type: "kokkos"}}}},
		"network":  {{Type: jobspec.AnyOfType, With: []jobspec.Resource{{Type: "efa"}, {Type: "infiniband"}}}},
	})
	if !feasibleOn(m, combo, "c1") {
		t.Fatal("software(lammps->kokkos) AND network anyof(efa,ib) should satisfy")
	}
}

func TestExpandSectionCartesian(t *testing.T) {
	// two anyof groups expand to the cross product; plain resources stay in each.
	section := []jobspec.Resource{
		{Type: "lammps"},
		{Type: jobspec.AnyOfType, With: []jobspec.Resource{{Type: "efa"}, {Type: "ib"}}},
	}
	got := jobspec.ExpandSection(section)
	if len(got) != 2 {
		t.Fatalf("want 2 concrete alternatives, got %d", len(got))
	}
	for _, alt := range got {
		if len(alt) != 2 || alt[0].Type != "lammps" {
			t.Fatalf("each alternative keeps the AND resource + one choice: %+v", alt)
		}
	}
}
