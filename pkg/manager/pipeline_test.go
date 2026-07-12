package manager

import (
	"io"
	"log"
	"sync"
	"testing"

	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
	"github.com/converged-computing/fleetq/pkg/matcher"
	"github.com/converged-computing/fleetq/pkg/queue"
)

// fakeMatcher counts Free per allocation id so tests can assert free-exactly-once.
type fakeMatcher struct {
	mu    sync.Mutex
	freed map[string]int
}

func newFakeMatcher() *fakeMatcher                                  { return &fakeMatcher{freed: map[string]int{}} }
func (f *fakeMatcher) Evaluate(jobspec.Jobspec) []matcher.Candidate { return nil }
func (f *fakeMatcher) Allocate(_ jobspec.Jobspec, c string) (matcher.Allocation, bool, error) {
	return matcher.Allocation{ID: "alloc-" + c, ClusterID: c}, true, nil
}
func (f *fakeMatcher) Free(id string) error {
	f.mu.Lock()
	f.freed[id]++
	f.mu.Unlock()
	return nil
}
func (f *fakeMatcher) AddCluster(graph.ClusterGraph) error                 { return nil }
func (f *fakeMatcher) RemoveCluster(string) error                          { return nil }
func (f *fakeMatcher) AddSubsystem(string, string, *graph.JGF, bool) error { return nil }
func (f *fakeMatcher) RemoveSubsystem(string, string) error                { return nil }
func (f *fakeMatcher) count(id string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.freed[id]
}

func newTestManager() (*Manager, *fakeMatcher) {
	fm := newFakeMatcher()
	m := &Manager{
		Fleet:   graph.NewFleet(graph.ClusterGraph{ID: "c1", Manager: graph.K8sJob}),
		Queue:   queue.NewMemory(),
		Matcher: fm,
		Logger:  log.New(io.Discard, "", 0),
	}
	return m, fm
}

func seed(m *Manager, id, alloc, handle string) {
	_ = m.Queue.Enqueue(queue.Job{
		ID: id, State: queue.Matched, AllocID: alloc, ClusterID: "c1", RemoteHandle: handle,
	})
}

func stateOf(m *Manager, id string) queue.Job { j, _ := m.Queue.Get(id); return j }

func TestDispatchSuccessThenMonitorFrees(t *testing.T) {
	m, fm := newTestManager()
	m.attempt = func(queue.Job) attemptResult {
		return attemptResult{outcome: outcomeSuccess, handle: "h1", note: "dispatched"}
	}
	m.statusFn = func(graph.ClusterGraph, string) (queue.State, string, error) {
		return queue.Completed, "done", nil
	}
	seed(m, "j1", "a1", "")

	next, retry := m.handleDispatch("j1")
	if next != qMonitor || retry {
		t.Fatalf("dispatch success => (%q,%v), want (monitor,false)", next, retry)
	}
	if j := stateOf(m, "j1"); j.State != queue.Running || j.RemoteHandle != "h1" {
		t.Fatalf("after dispatch: state=%v handle=%q", j.State, j.RemoteHandle)
	}
	if fm.count("a1") != 0 {
		t.Fatalf("must not free before terminal; freed=%d", fm.count("a1"))
	}
	done, _ := m.handleMonitor("j1")
	if !done || stateOf(m, "j1").State != queue.Completed {
		t.Fatalf("monitor => done=%v state=%v", done, stateOf(m, "j1").State)
	}
	// monitor idempotency: a second poll after terminal must not double-free.
	_, _ = m.handleMonitor("j1")
	if fm.count("a1") != 1 {
		t.Fatalf("free-exactly-once violated: freed=%d", fm.count("a1"))
	}
}

func TestDispatchHardFailFrees(t *testing.T) {
	m, fm := newTestManager()
	m.attempt = func(queue.Job) attemptResult {
		return attemptResult{outcome: outcomeHardFail, handle: "", note: "boom"}
	}
	seed(m, "j1", "a1", "")
	next, _ := m.handleDispatch("j1")
	if next != "" || stateOf(m, "j1").State != queue.Failed || fm.count("a1") != 1 {
		t.Fatalf("hardfail => next=%q state=%v freed=%d", next, stateOf(m, "j1").State, fm.count("a1"))
	}
}

func TestDispatchTransientRetriesKeepsAllocation(t *testing.T) {
	m, fm := newTestManager()
	m.attempt = func(queue.Job) attemptResult {
		return attemptResult{outcome: outcomeTransient, handle: "", note: "blip"}
	}
	seed(m, "j1", "a1", "")
	_, retry := m.handleDispatch("j1")
	if !retry || fm.count("a1") != 0 {
		t.Fatalf("transient => retry=%v freed=%d (want retry, no free)", retry, fm.count("a1"))
	}
}

func TestRepairableToRepairThenFixedReDispatches(t *testing.T) {
	m, fm := newTestManager()
	m.attempt = func(queue.Job) attemptResult {
		return attemptResult{outcome: outcomeRepairable, handle: "", note: "bad field"}
	}
	m.repairFn = func(queue.Job, string) attemptResult {
		return attemptResult{outcome: outcomeSuccess, handle: "", note: "fixed"}
	}
	seed(m, "j1", "a1", "")

	next, _ := m.handleDispatch("j1")
	if next != qRepair || fm.count("a1") != 0 {
		t.Fatalf("repairable => next=%q freed=%d (want repair, no free)", next, fm.count("a1"))
	}
	next, _ = m.handleRepair("j1", "bad field", 1, repairMaxAttempts)
	if next != qMonitor || fm.count("a1") != 0 {
		t.Fatalf("repair-fixed => next=%q freed=%d (want monitor, keep alloc)", next, fm.count("a1"))
	}
	if stateOf(m, "j1").State != queue.Running {
		t.Fatalf("after repair-fix, state=%v want Running", stateOf(m, "j1").State)
	}
}

func TestRepairExhaustedFrees(t *testing.T) {
	m, fm := newTestManager()
	m.repairFn = func(queue.Job, string) attemptResult {
		return attemptResult{outcome: outcomeRepairable, handle: "", note: "still bad"}
	}
	seed(m, "j1", "a1", "")
	// last attempt: attempt == max, so no more retries -> terminal
	next, retry := m.handleRepair("j1", "still bad", repairMaxAttempts, repairMaxAttempts)
	if next != "" || retry || stateOf(m, "j1").State != queue.Failed || fm.count("a1") != 1 {
		t.Fatalf("exhausted => next=%q retry=%v state=%v freed=%d", next, retry, stateOf(m, "j1").State, fm.count("a1"))
	}
}

func TestWrongTargetCancelsFreesReschedules(t *testing.T) {
	m, fm := newTestManager()
	var canceled string
	m.cancelFn = func(_ graph.ClusterGraph, h string) error { canceled = h; return nil }
	m.attempt = func(queue.Job) attemptResult {
		return attemptResult{outcome: outcomeWrongTarget, handle: "", note: "CRD absent"}
	}
	seed(m, "j1", "a1", "remote-h") // has a remote handle => must be canceled

	next, _ := m.handleDispatch("j1")
	j := stateOf(m, "j1")
	if next != "" || canceled != "remote-h" || j.State != queue.Submitted || j.Reschedules != 1 || j.ClusterID != "" {
		t.Fatalf("wrong-target => next=%q canceled=%q state=%v resched=%d cluster=%q",
			next, canceled, j.State, j.Reschedules, j.ClusterID)
	}
	if fm.count("a1") != 1 {
		t.Fatalf("wrong-target must free once; freed=%d", fm.count("a1"))
	}
}

func TestRescheduleBounded(t *testing.T) {
	m, fm := newTestManager()
	m.attempt = func(queue.Job) attemptResult {
		return attemptResult{outcome: outcomeWrongTarget, handle: "", note: "nope"}
	}
	seed(m, "j1", "a1", "")
	// maxReschedules successful bounces, then a hard fail on the next.
	for i := 0; i < maxReschedules; i++ {
		m.handleDispatch("j1")
		if s := stateOf(m, "j1").State; s != queue.Submitted {
			t.Fatalf("bounce %d: state=%v want Submitted", i, s)
		}
		// scheduler would re-place it; simulate by restoring a cluster+alloc.
		j := stateOf(m, "j1")
		j.State, j.ClusterID, j.AllocID = queue.Matched, "c1", "a1"
		_ = m.Queue.Update(j)
	}
	m.handleDispatch("j1")
	if stateOf(m, "j1").State != queue.Failed {
		t.Fatalf("after %d reschedules, want Failed; got %v", maxReschedules, stateOf(m, "j1").State)
	}
	if got := fm.count("a1"); got != maxReschedules+1 {
		t.Fatalf("freed=%d, want %d (once per bounce + final fail)", got, maxReschedules+1)
	}
}

// TestConcurrentDispatchFreeExactlyOnce stresses the mutex/state discipline:
// many jobs pushed through handleDispatch in parallel, each terminal freeing
// its own allocation exactly once. Run with -race.
func TestConcurrentDispatchFreeExactlyOnce(t *testing.T) {
	m, fm := newTestManager()
	m.attempt = func(j queue.Job) attemptResult {
		if j.ID[len(j.ID)-1]%2 == 0 {
			return attemptResult{outcome: outcomeHardFail, handle: "", note: "boom"}
		}
		return attemptResult{outcome: outcomeWrongTarget, handle: "", note: "nope"}
	}
	const n = 200
	for i := 0; i < n; i++ {
		id := "j" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		seed(m, id, "alloc-"+id, "")
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id := "j" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		wg.Add(1)
		go func() { defer wg.Done(); m.handleDispatch(id) }()
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		id := "j" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		if got := fm.count("alloc-" + id); got != 1 {
			t.Fatalf("%s freed %d times, want exactly 1", id, got)
		}
	}
}
