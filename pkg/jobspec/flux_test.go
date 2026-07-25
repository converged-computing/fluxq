package jobspec_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/converged-computing/fluxq/pkg/jobspec"
)

// The containment render is the Flux document minus `requires` (subsystems are
// matched against their own graphs, never as containment constraints), so no
// constraints/requires leak into it.
func TestToFluxSpecResourcesOnly(t *testing.T) {
	js := jobspec.New("lammps", "lammps:latest", []string{"lmp", "-i", "in.reaxff"}, 5, 64, time.Hour,
		map[string][]jobspec.Resource{
			"software": {{Type: "lammps", With: []jobspec.Resource{{Type: "kokkos"}}}},
			"network":  {{Type: "efa"}},
		})
	out, err := js.ToFluxSpec()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("not valid json: %v", err)
	}
	if m["version"].(float64) != 1 {
		t.Fatalf("want version 1, got %v", m["version"])
	}
	if _, leaked := m["requires"]; leaked {
		t.Fatal("containment render must NOT include the requires extension")
	}
	res := m["resources"].([]any)[0].(map[string]any)
	if res["type"] != "node" {
		t.Fatalf("root resource must be node, got %v", res["type"])
	}
	sys := m["attributes"].(map[string]any)["system"].(map[string]any)
	if sys["duration"].(float64) != 3600 {
		t.Fatalf("want duration 3600s, got %v", sys["duration"])
	}
	if _, hasConstraints := sys["constraints"]; hasConstraints {
		t.Fatal("containment jobspec must NOT carry subsystem constraints")
	}
}

// A subsystem section renders as slot -> typed subtree, matched by type against
// the subsystem graph. lammps WITH kokkos nests kokkos under lammps.
func TestSubsystemFluxSpecNests(t *testing.T) {
	section := []jobspec.Resource{
		{Type: "lammps", With: []jobspec.Resource{{Type: "kokkos"}}},
	}
	out, err := jobspec.SubsystemFluxSpec(section)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("not valid json: %v", err)
	}
	slot := m["resources"].([]any)[0].(map[string]any)
	if slot["type"] != "slot" {
		t.Fatalf("want slot root, got %v", slot["type"])
	}
	lammps := slot["with"].([]any)[0].(map[string]any)
	if lammps["type"] != "lammps" {
		t.Fatalf("want lammps under slot, got %v", lammps["type"])
	}
	if _, ok := lammps["with"]; !ok {
		t.Fatal("kokkos child must nest under lammps")
	}
}

// Accessors read the Flux document rather than duplicating fields.
func TestAccessors(t *testing.T) {
	js := jobspec.New("job1", "img:1", []string{"a", "b"}, 3, 8, 2*time.Hour, nil)
	if js.Name() != "job1" || js.Image() != "img:1" {
		t.Fatalf("name/image: %q %q", js.Name(), js.Image())
	}
	if js.Nodes() != 3 || js.CoresPerNode() != 8 {
		t.Fatalf("counts: %d nodes, %d cores/node", js.Nodes(), js.CoresPerNode())
	}
	if js.Duration() != 2*time.Hour {
		t.Fatalf("duration: %v", js.Duration())
	}
}
