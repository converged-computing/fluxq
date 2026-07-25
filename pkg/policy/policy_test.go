package policy_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/matcher"
	"github.com/converged-computing/fluxq/pkg/policy"
	"github.com/converged-computing/fluxq/pkg/queue"
)

// exportSmall writes a two-cluster JGF fleet and loads it back through the
// Loader, so tests exercise the real graph-load + traversal path.
func loadSmall(t *testing.T) *graph.Fleet {
	t.Helper()
	root := t.TempDir()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(graph.ExportCluster(root, "a", graph.BuildContainment("a", graph.K8sJob, "ctx-a", []graph.NodeSpec{{Count: 2, Cores: 64}}, []string{"lammps"})))
	must(graph.ExportCluster(root, "b", graph.BuildContainment("b", graph.K8sJob, "ctx-b", []graph.NodeSpec{{Count: 4, Cores: 64}}, []string{"amg"})))
	f, err := graph.DirectoryLoader{}.LoadFleet(root)
	must(err)
	_ = filepath.Base(root)
	return swFleet(t, f)
}

func job(name, sw string, nodes int) queue.Job {
	return queue.Job{
		ID: name, State: queue.Submitted, SubmittedAt: time.Now(),
		Spec: jobspec.New(name, "", nil, nodes, 1, 0,
			map[string][]jobspec.Resource{"software": {{Type: sw}}}),
	}
}

// feasible lists the cluster ids a matcher reports feasible for a job.
func feasible(m matcher.Matcher, spec jobspec.Jobspec) []string {
	var out []string
	for _, c := range m.Evaluate(spec) {
		if c.Feasible {
			out = append(out, c.Cluster)
		}
	}
	return out
}

func TestEvaluateAndAllocate(t *testing.T) {
	m := matcher.NewSim(loadSmall(t))
	if cs := feasible(m, job("j", "lammps", 2).Spec); len(cs) != 1 || cs[0] != "a" {
		t.Fatalf("expected only cluster a feasible for lammps, got %v", cs)
	}
	if cs := feasible(m, job("j", "nonesuch", 1).Spec); len(cs) != 0 {
		t.Fatalf("expected infeasible for unknown software, got %v", cs)
	}
	alloc, ok, err := m.Allocate(job("j", "amg", 4).Spec, "b")
	if err != nil || !ok || alloc.ClusterID != "b" {
		t.Fatalf("expected allocate on b, got alloc=%+v ok=%v err=%v", alloc, ok, err)
	}
	if len(alloc.VertexIDs) != 4 {
		t.Fatalf("expected 4 allocated node vertices, got %d", len(alloc.VertexIDs))
	}
	if _, ok, err := m.Allocate(job("j2", "amg", 4).Spec, "b"); ok || err != nil {
		t.Fatalf("expected full (not error), got ok=%v err=%v", ok, err)
	}
	if err := m.Free(alloc.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := m.Allocate(job("j3", "amg", 4).Spec, "b"); !ok {
		t.Fatal("expected allocate after free")
	}
}

// The policy now only orders and declares head-of-line blocking; placement is
// the manager's Evaluate/Allocate loop.
func TestPolicyContract(t *testing.T) {
	if !(policy.FCFS{}).HeadOfLineBlock() {
		t.Fatal("FCFS must head-of-line block")
	}
	if (policy.Backfill{}).HeadOfLineBlock() {
		t.Fatal("Backfill must not head-of-line block")
	}
	prov := []queue.Job{job("first", "amg", 1), job("second", "lammps", 1)}
	if got := (policy.FCFS{}).Order(prov); len(got) != 2 || got[0].ID != "first" {
		t.Fatalf("FCFS.Order must preserve submit order, got %+v", got)
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
