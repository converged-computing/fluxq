package jobspec_test

import (
	"encoding/json"
	"strings"
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

func TestWholeNodeRequestIsExclusiveToFluxion(t *testing.T) {
	// A node-level request with no cores: the Fluxion query must ask for the NODE
	// exclusively, not for one core on each node.
	js := jobspec.Jobspec{
		Version: 1,
		Resources: []jobspec.Resource{{
			Type: "node", Count: 3, Exclusive: true,
			With: []jobspec.Resource{{Type: "slot", Count: 1, Label: "default"}},
		}},
		Tasks: []jobspec.Task{{
			Command: []string{"lmp"}, Slot: "default",
			Count: map[string]int{"per_slot": 1},
		}},
	}
	if !js.Exclusive() {
		t.Fatal("a node marked exclusive must report Exclusive()")
	}
	out, err := js.ToFluxSpec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "exclusive") {
		t.Fatalf("fluxion query dropped exclusivity:\n%s", out)
	}

	// no cores authored at all -> still whole nodes
	js2 := jobspec.Jobspec{Version: 1, Resources: []jobspec.Resource{{
		Type: "node", Count: 2,
		With: []jobspec.Resource{{Type: "slot", Count: 1, Label: "default"}},
	}}}
	if !js2.Exclusive() {
		t.Fatal("a node request with no cores means whole nodes")
	}
}

func TestSlotAboveNodeCountsAndStaysExclusive(t *testing.T) {
	// The authored shape: slot(count=N) -> node(count=1, exclusive). Nodes must
	// still count as N, and the whole-node intent must survive to Fluxion.
	js := jobspec.Jobspec{
		Version: 1,
		Resources: []jobspec.Resource{{
			Type: "slot", Count: 3, Label: "default",
			With: []jobspec.Resource{{Type: "node", Count: 1, Exclusive: true}},
		}},
		Tasks: []jobspec.Task{{
			Command: []string{"lmp"}, Slot: "default",
			Count: map[string]int{"per_slot": 1},
		}},
	}
	if n := js.Nodes(); n != 3 {
		t.Fatalf("slot-above-node should count 3 nodes, got %d", n)
	}
	if !js.Exclusive() {
		t.Fatal("exclusive node under a slot must report Exclusive()")
	}
	if got := js.TasksTotal(); got != 3 {
		t.Fatalf("one task per node -> 3, got %d", got)
	}
	out, err := js.ToFluxSpec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "exclusive") {
		t.Fatalf("fluxion query dropped exclusivity:\n%s", out)
	}
}

func TestSubsystemQueryNeverAsksForZero(t *testing.T) {
	// A requires entry names a capability and carries no count. Fluxion reads
	// count literally, so a zero count silently matches nothing — every type in
	// the rendered query must ask for at least one.
	for _, section := range [][]jobspec.Resource{
		{{Type: "amd64"}},
		{{Type: "lammps", With: []jobspec.Resource{{Type: "kokkos"}}}},
	} {
		out, err := jobspec.SubsystemFluxSpec(section)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out, `"count":0`) {
			t.Fatalf("subsystem query asks for zero resources:\n%s", out)
		}
	}
}
