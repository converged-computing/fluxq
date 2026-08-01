package vocabulary

import "github.com/converged-computing/fluxq/pkg/graph"
import "testing"

func cluster(id string, subs map[string]string) graph.ClusterGraph {
	m := map[string]*graph.JGF{}
	for name, val := range subs {
		m[name] = graph.SingletonSubsystem(name, val)
	}
	return graph.ClusterGraph{ID: id, Subsystems: m}
}

func find(v Vocabulary, name string) *Dimension {
	for i := range v.Dimensions {
		if v.Dimensions[i].Name == name {
			return &v.Dimensions[i]
		}
	}
	return nil
}

func TestDeriveHeterogeneousFleet(t *testing.T) {
	fleet := []graph.ClusterGraph{
		cluster("gke-arm", map[string]string{"architecture": "arm64", "network": "ethernet", "memory": "64-192GB"}),
		cluster("eks-nvidia", map[string]string{"architecture": "amd64", "network": "efa", "gpu": "nvidia", "memory": "192GB+"}),
		cluster("eks-amd", map[string]string{"architecture": "amd64", "network": "efa", "gpu": "amd", "memory": "192GB+"}),
	}
	v := Derive(fleet)

	if d := find(v, "architecture"); d == nil || len(d.Values) != 2 {
		t.Fatalf("architecture should list {amd64,arm64}: %+v", d)
	}
	if d := find(v, "network"); d == nil || len(d.Values) != 2 {
		t.Fatalf("network should list {efa,ethernet}: %+v", d)
	}
	// gpu heterogeneous (nvidia + amd) -> dimension present
	if d := find(v, "gpu"); d == nil || len(d.Values) != 2 {
		t.Fatalf("gpu vendor dimension should appear when heterogeneous: %+v", d)
	}
	// memory ranges from {128,256,512}
	if d := find(v, "memory"); d == nil || len(d.Values) == 0 || d.Mechanism != "subsystem" {
		t.Fatalf("memory should be contiguous ranges: %+v", d)
	}
	t.Logf("memory ranges: %v", find(v, "memory").Values)
}

func TestGpuDroppedWhenHomogeneous(t *testing.T) {
	fleet := []graph.ClusterGraph{
		cluster("a", map[string]string{"gpu": "nvidia", "memory": "192GB+"}),
		cluster("b", map[string]string{"gpu": "nvidia", "memory": "192GB+"}),
	}
	v := Derive(fleet)
	if find(v, "gpu") != nil {
		t.Fatal("single-vendor fleet must NOT expose a gpu dimension")
	}
	// Memory buckets are absolute, so a uniform fleet still advertises its
	// bucket — it simply offers one value rather than several.
	if d := find(v, "memory"); d == nil || len(d.Values) != 1 {
		t.Fatalf("uniform fleet should advertise exactly one memory bucket: %+v", d)
	}
}
