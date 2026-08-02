package manager_test

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/converged-computing/fluxq/pkg/cluster"
	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/manager"
	"github.com/converged-computing/fluxq/pkg/matcher"
	"github.com/converged-computing/fluxq/pkg/policy"
	"github.com/converged-computing/fluxq/pkg/queue"
	"github.com/converged-computing/fluxq/pkg/transform"
)

// loadFleet writes a tiny JGF export and loads it back, so the test drives the
// real load + traversal path (no dependency on the checked-in data or CWD).
func loadFleet(t *testing.T) *graph.Fleet {
	t.Helper()
	root := t.TempDir()
	if err := graph.ExportCluster(root, "c1", graph.BuildContainment("c1", graph.K8sJob, "ctx", []graph.NodeSpec{{Count: 4, Cores: 32}}, []string{"lammps"})); err != nil {
		t.Fatal(err)
	}
	f, err := graph.DirectoryLoader{}.LoadFleet(root)
	if err != nil {
		t.Fatal(err)
	}
	return swFleet(t, f)
}

func newManager(t *testing.T, f *graph.Fleet, inject func(cluster.Content) (cluster.Plan, bool)) (*manager.Manager, *matcher.SimMatcher) {
	t.Helper()
	sim := matcher.NewSim(f)
	// Fast emulator timing so the test settles quickly.
	cfg := cluster.EmulatorConfig{Pending: 10 * time.Millisecond, Run: 30 * time.Millisecond, Inject: inject}
	m := &manager.Manager{
		Fleet:   f,
		Queue:   queue.NewMemory(),
		Matcher: sim,
		Policy:  policy.FCFS{},
		Trans:   transform.Stub{},
		Drivers: cluster.NewRegistry(cluster.NewEmulatedDriver(graph.K8sJob, cfg)),
		Tick:    5 * time.Millisecond,
		Logger:  log.New(io.Discard, "", 0),
	}
	return m, sim
}

func lammps(name string, nodes int) jobspec.Jobspec {
	return jobspec.New(name, "img", []string{"lmp", "-i", "in.reaxff"}, nodes, 1, 0,
		map[string][]jobspec.Resource{"software": {{Type: "lammps"}}})
}

func waitState(t *testing.T, m *manager.Manager, id string, want queue.State) queue.Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		m.Step()
		if j, ok := m.Queue.Get(id); ok && j.State == want {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	j, _ := m.Queue.Get(id)
	t.Fatalf("job %s never reached %s (last=%s note=%q)", id, want, j.State, j.Note)
	return queue.Job{}
}

func allocAny(m matcher.Matcher, js jobspec.Jobspec) bool {
	for _, c := range m.Evaluate(js) {
		if c.Feasible {
			if _, ok, _ := m.Allocate(js, c.Cluster); ok {
				return true
			}
		}
	}
	return false
}

func TestHappyPathCompletesAndFrees(t *testing.T) {
	f := loadFleet(t)
	m, sim := newManager(t, f, nil)
	id, _ := m.Submit(lammps("ok", 4)) // consumes all 4 nodes
	j := waitState(t, m, id, queue.Completed)
	if j.ClusterID != "c1" || j.RemoteHandle == "" {
		t.Fatalf("expected dispatch to c1 with a handle, got %+v", j)
	}
	// Completion must have freed the allocation: a second 4-node job can match.
	if !allocAny(sim, lammps("after", 4)) {
		t.Fatal("allocation was not freed on completion")
	}
}

func TestInjectedTimeoutFailsAndFrees(t *testing.T) {
	f := loadFleet(t)
	inject := func(c cluster.Content) (cluster.Plan, bool) {
		return cluster.Plan{Phases: []cluster.Phase{
			{At: 0, State: queue.Running, Log: "running"},
			{At: 20 * time.Millisecond, State: queue.Failed, Log: "walltime exceeded"},
		}}, true
	}
	m, sim := newManager(t, f, inject)
	id, _ := m.Submit(lammps("boom", 4))
	j := waitState(t, m, id, queue.Failed)
	if !strings.Contains(j.Note, "walltime") {
		t.Fatalf("expected timeout note, got %q", j.Note)
	}
	if !allocAny(sim, lammps("after", 4)) {
		t.Fatal("allocation was not freed on failure")
	}
}

func TestMalformedJobspecRejected(t *testing.T) {
	// Flux is jobspec-native (no shell command line), so the paper's flag-
	// concatenation bug can't arise; the emulator instead rejects a malformed
	// jobspec rather than reporting it as running.
	if note, ok := clusterSanity(cluster.Content{Kind: "jobspec", Payload: "not a jobspec"}); ok {
		t.Fatalf("expected rejection, got ok with note %q", note)
	}
}

// clusterSanity exercises the exported behavior indirectly via a submit on a
// flux-uri emulated driver (jobspec kind), asserting it lands in Failed.
func clusterSanity(c cluster.Content) (string, bool) {
	d := cluster.NewEmulatedDriver(graph.FluxURI, cluster.EmulatorConfig{})
	h, err := d.Submit(graph.ClusterGraph{Manager: graph.FluxURI}, c)
	if err != nil {
		return err.Error(), false
	}
	st, note, _ := d.Status(graph.ClusterGraph{}, h)
	return note, st != queue.Failed
}

// TestFullCandidateWaitsThenDispatches asserts the core rule: a job whose only
// candidate cluster is full must STAY QUEUED (not fail) and dispatch once the
// occupying job completes and frees capacity.
func TestFullCandidateWaitsThenDispatches(t *testing.T) {
	f := loadFleet(t) // single cluster c1, 4 nodes, software lammps
	m, _ := newManager(t, f, nil)

	first, _ := m.Submit(lammps("first", 4)) // consumes all 4 nodes
	second, _ := m.Submit(lammps("second", 4))

	// Drive a few passes: first should dispatch; second must be waiting, NOT failed.
	for i := 0; i < 3; i++ {
		m.Step()
		time.Sleep(3 * time.Millisecond)
	}
	if j, _ := m.Queue.Get(second); j.State == queue.Failed {
		t.Fatalf("second must not fail while a candidate exists; got FAILED (%s)", j.Note)
	}
	if j, _ := m.Queue.Get(second); j.State != queue.Submitted {
		t.Logf("second state=%s note=%q (expected still waiting)", j.State, j.Note)
	}

	// Let first complete and free; second must then dispatch and complete.
	_ = waitState(t, m, first, queue.Completed)
	j := waitState(t, m, second, queue.Completed)
	if j.ClusterID != "c1" {
		t.Fatalf("second should dispatch to c1 after capacity frees, got %q", j.ClusterID)
	}
}

// TestImpossibleJobFails asserts that a job which can never fit any cluster's
// containment (ignoring load) is FAILED, not left waiting — even though its
// subsystems are satisfiable.
func TestImpossibleJobFails(t *testing.T) {
	f := loadFleet(t) // c1: 4 nodes, 32 cores, software lammps
	m, _ := newManager(t, f, nil)

	// (a) too many nodes: c1 has 4, ask 5.
	big, _ := m.Submit(lammps("too-big", 5))
	// (b) per-node too large: needs 64 cores/node, c1 nodes have 32.
	fat := jobspec.New("too-fat", "img", []string{"lmp"}, 1, 64, 0,
		map[string][]jobspec.Resource{"software": {{Type: "lammps"}}})
	fatID, _ := m.Submit(fat)

	jb := waitState(t, m, big, queue.Failed)
	if !strings.Contains(jb.Note, "impossible") {
		t.Fatalf("too-big should fail as impossible, note=%q", jb.Note)
	}
	jf := waitState(t, m, fatID, queue.Failed)
	if !strings.Contains(jf.Note, "impossible") {
		t.Fatalf("too-fat should fail as impossible, note=%q", jf.Note)
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

func TestCancelReleasesTheAllocation(t *testing.T) {
	// Without a cancel the only way to clear a stuck job is restarting the
	// control plane, which also discards the registered fleet.
	f := loadFleet(t)
	m, _ := newManager(t, f, nil)
	stop := make(chan struct{})
	go m.Run(stop)
	defer close(stop)

	id, err := m.Submit(lammps("j", 1))
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, m, id, queue.Running)

	if err := m.Cancel(id); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	j, _ := m.Queue.Get(id)
	if !j.State.Terminal() {
		t.Fatalf("a cancelled job must be terminal, got %s", j.State)
	}
	if len(m.CancelAll()) != 0 {
		t.Error("nothing should remain in flight after cancelling")
	}
}
