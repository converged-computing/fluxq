// Package manager runs the pipeline: submit -> (select via policy+matcher) ->
// transform -> dispatch -> monitor -> free. It owns two loops. The schedule
// loop asks the policy which provisional jobs to place this tick, then for each
// placement transforms and dispatches. The monitor loop reconciles active jobs
// against their drivers and, on a terminal state, frees the Fluxion allocation
// so the resources return to the fleet.
package manager

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/converged-computing/fleetq/pkg/cluster"
	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
	"github.com/converged-computing/fleetq/pkg/matcher"
	"github.com/converged-computing/fleetq/pkg/policy"
	"github.com/converged-computing/fleetq/pkg/queue"
	"github.com/converged-computing/fleetq/pkg/reconcile"
	"github.com/converged-computing/fleetq/pkg/score"
	"github.com/converged-computing/fleetq/pkg/transform"
)

type Manager struct {
	Fleet   *graph.Fleet
	Queue   queue.Queue
	Matcher matcher.Matcher
	Policy  policy.Policy
	Scorer  score.Scorer
	Trans   transform.Transformer
	Drivers *cluster.Registry
	// RealDrivers (optional) holds backend-specific drivers that reach clusters
	// for real. A cluster is dispatched via RealDrivers only when it carries
	// dispatch Config (backend metadata); otherwise Drivers (emulated) is used.
	// When nil, every cluster uses Drivers — keeping offline/tests unchanged.
	RealDrivers *cluster.Registry

	Tick   time.Duration
	Logger *log.Logger

	// Optional: when a job is impossible on the fleet, ask the Reconciler for a
	// work-preserving reconfiguration. By default the suggestion is attached to
	// the failed job (suggest, don't act); set AutoResubmit to submit it.
	Reconciler   reconcile.Reconciler
	AutoResubmit bool

	// Dispatcher performs the matched-job -> running-handle step. Default is an
	// inline dispatcher (synchronous, used by tests and the offline demo). The
	// river dispatcher (NewRiverEngine) enqueues durable, retried dispatch jobs.
	Dispatcher Dispatcher

	// pipeline seams (nil => the real implementation); overridden in tests to
	// drive each transition edge deterministically.
	attempt  func(queue.Job) attemptResult
	repairFn func(j queue.Job, lastErr string) attemptResult
	statusFn func(graph.ClusterGraph, string) (queue.State, string, error)
	cancelFn func(graph.ClusterGraph, string) error
	validate func(graph.ClusterGraph, cluster.Content) (verdict, string)

	// riverInsert is set by NewRiverEngine* so stage workers can enqueue the
	// next stage. nil on the inline/memory path.
	riverInsert riverInserter

	mu  sync.Mutex
	seq int
}

// Submit places an agnostic jobspec into the provisional queue and returns its
// job id (the receipt). This is the entry point for both the HTTP API and the
// eventual "text prompt -> jobspec" front end.
func (m *Manager) Submit(js jobspec.Jobspec) (string, error) {
	m.mu.Lock()
	m.seq++
	id := jobID(m.seq)
	m.mu.Unlock()
	j := queue.Job{ID: id, Spec: js, State: queue.Submitted, SubmittedAt: time.Now()}
	if err := m.Queue.Enqueue(j); err != nil {
		return "", err
	}
	m.logf("submit %s (%s) -> provisional", id, js.Name())
	return id, nil
}

// ManagerSupport reports, per manager type, whether this server can dispatch to
// it for real (a backend driver is registered) and/or emulate it.
type ManagerSupport struct {
	Manager  graph.ManagerType
	Real     bool
	Emulated bool
}

// SupportedManagers lists every known manager with its real/emulated capability
// on this server. Real depends on which drivers were registered at serve time.
func (m *Manager) SupportedManagers() []ManagerSupport {
	out := make([]ManagerSupport, 0, len(graph.KnownManagers()))
	for _, mt := range graph.KnownManagers() {
		out = append(out, ManagerSupport{
			Manager:  mt,
			Real:     m.RealDrivers.Has(mt),
			Emulated: m.Drivers.Has(mt),
		})
	}
	return out
}

// setNote updates a job's note if it changed (used for the waiting reason).
func (m *Manager) setNote(j queue.Job, note string) {
	if j.Note != note {
		j.Note = note
		_ = m.Queue.Update(j)
		m.logf("wait %s: %s", j.ID, note)
	}
}

// RegisterCluster adds a cluster to the fleet AND the matcher. Order matters:
// add to the fleet first so the matcher never references a cluster the fleet
// lacks (dispatch/monitor look clusters up by id in the fleet).
// ErrNotImplemented is returned when a cluster asks to dispatch to a real
// backend for which no real driver exists yet. Register with config
// emulate=true to simulate the backend's dialect instead.
var ErrNotImplemented = errors.New("not implemented")

func (m *Manager) RegisterCluster(cg graph.ClusterGraph) error {
	// Asking for a real backend we don't have a driver for is an explicit error,
	// not a silent fall-back to the emulator. emulate=true opts into simulation.
	if !cg.Emulated() && m.RealDrivers != nil {
		if _, err := m.RealDrivers.For(cg.Manager); err != nil {
			return fmt.Errorf("%w: no real driver for manager %q yet — register with --config emulate=true to simulate it", ErrNotImplemented, cg.Manager)
		}
	}
	m.Fleet.Add(cg)
	if err := m.Matcher.AddCluster(cg); err != nil {
		m.Fleet.Remove(cg.ID)
		return err
	}
	m.logf("register cluster %s (%s)", cg.ID, cg.Manager)
	return nil
}

// UnregisterCluster removes a cluster from the matcher first (freeing its
// allocations), then the fleet. Returns whether it was present.
func (m *Manager) UnregisterCluster(id string) bool {
	_ = m.Matcher.RemoveCluster(id)
	ok := m.Fleet.Remove(id)
	if ok {
		m.logf("unregister cluster %s", id)
	}
	return ok
}

// Clusters returns the registered clusters (for the API listing).
func (m *Manager) Clusters() []graph.ClusterGraph {
	if m.Fleet == nil {
		return nil
	}
	return m.Fleet.Clusters()
}

// Run drives both loops until ctx-like stop channel closes.
func (m *Manager) Run(stop <-chan struct{}) {
	if m.Tick == 0 {
		m.Tick = 500 * time.Millisecond
	}
	t := time.NewTicker(m.Tick)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			m.Step()
		}
	}
}

// Step runs exactly one schedule pass then one monitor pass. Exported so tests
// (and callers wanting manual control) can drive the pipeline deterministically
// without the ticker.
func (m *Manager) Step() {
	m.scheduleOnce()
	// The staged river pipeline runs its own monitor stage; running the inline
	// monitor too would double-free allocations and race the stage. Skip it
	// when the dispatcher owns monitoring. The inline/memory path (no such
	// dispatcher) monitors here as before.
	if !m.dispatcherOwnsMonitoring() {
		m.monitorOnce()
	}
}

// pipelineOwner is implemented by dispatchers that run their own monitor stage
// (the staged river pipeline), so the manager knows to skip inline monitoring.
type pipelineOwner interface{ ownsMonitoring() bool }

func (m *Manager) dispatcherOwnsMonitoring() bool {
	po, ok := m.Dispatcher.(pipelineOwner)
	return ok && po.ownsMonitoring()
}

// scheduleOnce runs one pass: order the queue (policy), then for each job
// Evaluate the fleet (satisfy-only), fail it if no cluster is feasible, else
// rank the feasible clusters (scorer, random tie-break) and MatchAllocate the
// best that has room. A feasible-but-full job stays queued and is retried.
func (m *Manager) scheduleOnce() {
	prov := m.Queue.Provisional()
	if m.Fleet == nil || m.Fleet.Len() == 0 {
		// No clusters registered yet: nothing is impossible, the fleet is not
		// ready. Jobs wait until a cluster appears.
		for _, j := range prov {
			m.setNote(j, "waiting: no clusters registered")
		}
		return
	}
	scorer := m.scorer()
	block := m.Policy.HeadOfLineBlock()
	for _, ordered := range m.Policy.Order(prov) {
		j, ok := m.Queue.Get(ordered.ID)
		if !ok || j.State != queue.Submitted {
			continue
		}
		cands := m.Matcher.Evaluate(j.Spec)
		feasible := feasibleOnly(cands)
		if len(feasible) == 0 {
			m.failInfeasible(j) // impossible on this (non-empty) fleet
			continue
		}
		// Emulated clusters are satisfy-only: they appear in /satisfy and in
		// scoring, but are never dispatch targets — emulating a real submit is
		// meaningless. Place only on clusters that can actually dispatch.
		ranked := m.dispatchable(feasible)
		if len(ranked) == 0 {
			m.setNote(j, fmt.Sprintf("waiting: feasible only on %d emulated (satisfy-only) cluster(s) — register a real dispatch target", len(feasible)))
			if block {
				break
			}
			continue
		}
		if scorer.NeedsFullSet() {
			ranked = m.dispatchable(score.Rank(scorer, j.Spec, cands))
		}
		placed := false
		for _, c := range ranked {
			alloc, okA, err := m.Matcher.Allocate(j.Spec, c.Cluster)
			if err != nil {
				continue // genuine matcher error on this cluster; try the next
			}
			if okA {
				j.State = queue.Matched
				j.AllocID = alloc.ID
				j.ClusterID = alloc.ClusterID
				j.Note = ""
				_ = m.Queue.Update(j)
				m.logf("match %s -> cluster %s (alloc %s)", j.ID, j.ClusterID, j.AllocID)
				_ = m.dispatcher().Dispatch(j)
				placed = true
				break
			}
			// okA=false: cluster full right now — fall through to next candidate
		}
		if !placed {
			m.setNote(j, fmt.Sprintf("waiting: %d feasible cluster(s), none with free capacity", len(feasible)))
			if block {
				break // strict FCFS: do not place jobs behind a blocked one
			}
		}
	}
}

// dispatchable filters candidates to clusters that can really dispatch —
// dropping explicitly-emulated (satisfy-only) clusters. Emulated clusters still
// participate in /satisfy and scoring; they just aren't placement targets.
func (m *Manager) dispatchable(cands []matcher.Candidate) []matcher.Candidate {
	out := cands[:0:0]
	for _, c := range cands {
		if cg, ok := m.Fleet.Get(c.Cluster); ok && !cg.Emulated() {
			out = append(out, c)
		}
	}
	return out
}

func feasibleOnly(cands []matcher.Candidate) []matcher.Candidate {
	out := cands[:0:0]
	for _, c := range cands {
		if c.Feasible {
			out = append(out, c)
		}
	}
	return out
}

func (m *Manager) scorer() score.Scorer {
	if m.Scorer != nil {
		return m.Scorer
	}
	return score.Default{}
}

// Satisfy evaluates the fleet for a job and returns the ranked feasible
// candidates WITHOUT allocating anything (the /assess dry-run).
func (m *Manager) Satisfy(js jobspec.Jobspec) []matcher.Candidate {
	return score.Rank(m.scorer(), js, m.Matcher.Evaluate(js))
}

func (m *Manager) anyFeasible(js jobspec.Jobspec) bool {
	for _, c := range m.Matcher.Evaluate(js) {
		if c.Feasible {
			return true
		}
	}
	return false
}

// RegisterSubsystem attaches a named subsystem tree to an existing cluster in
// both the fleet and the matcher. descriptive=true means satisfy-only.
func (m *Manager) RegisterSubsystem(clusterID, name string, g *graph.JGF, descriptive bool) error {
	cg, ok := m.Fleet.Get(clusterID)
	if !ok {
		return fmt.Errorf("cluster %q not registered", clusterID)
	}
	if err := m.Matcher.AddSubsystem(clusterID, name, g, descriptive); err != nil {
		return err
	}
	cg.AttachSubsystem(name, g, descriptive)
	m.Fleet.Add(cg)
	m.logf("register subsystem %s on %s (descriptive=%v)", name, clusterID, descriptive)
	return nil
}

// UnregisterSubsystem detaches a named subsystem from a cluster.
func (m *Manager) UnregisterSubsystem(clusterID, name string) error {
	if err := m.Matcher.RemoveSubsystem(clusterID, name); err != nil {
		return err
	}
	if cg, ok := m.Fleet.Get(clusterID); ok {
		cg.DetachSubsystem(name)
		m.Fleet.Add(cg)
	}
	m.logf("unregister subsystem %s on %s", name, clusterID)
	return nil
}

// Dispatcher drives a matched job toward a running remote handle. It may run
// synchronously (inline) or enqueue durable work (river).
type Dispatcher interface {
	Dispatch(job queue.Job) error
}

// inlineDispatcher is the synchronous default.
type inlineDispatcher struct{ m *Manager }

func (d inlineDispatcher) Dispatch(j queue.Job) error { d.m.dispatchNow(j.ID); return nil }

func (m *Manager) dispatcher() Dispatcher {
	if m.Dispatcher != nil {
		return m.Dispatcher
	}
	return inlineDispatcher{m}
}

// dispatchNow transforms the agnostic intent for the chosen cluster and submits.
// It reloads the job by id so it is safe to call from a river worker after a
// crash/retry. Exported-ish via the river worker in river.go.
func (m *Manager) dispatchNow(id string) {
	j, ok := m.Queue.Get(id)
	if !ok {
		return
	}
	c, ok := m.Fleet.Get(j.ClusterID)
	if !ok {
		m.failJob(j, "assigned cluster not found")
		return
	}
	j.State = queue.Dispatching
	_ = m.Queue.Update(j)

	content, err := m.Trans.Transform(j.Spec, c)
	if err != nil {
		m.failJob(j, "transform: "+err.Error())
		return
	}
	drv, err := m.driverFor(c)
	if err != nil {
		m.failJob(j, err.Error())
		return
	}
	handle, err := drv.Submit(c, contentToDriver(content))
	if err != nil {
		m.failJob(j, "submit: "+err.Error())
		return
	}
	j.RemoteHandle = handle
	j.State = queue.Running
	j.Note = "dispatched"
	_ = m.Queue.Update(j)
	m.logf("dispatch %s -> %s:%s as %s", j.ID, c.Manager, c.ID, handle)
}

// driverFor selects the dispatch driver for a cluster. Clusters that provide
// backend dispatch Config (a flux URI, a kubeconfig, …) go to the real driver
// for their manager; clusters without it (the offline default) are emulated.
func (m *Manager) driverFor(c graph.ClusterGraph) (cluster.Driver, error) {
	if c.Emulated() {
		return m.Drivers.For(c.Manager) // explicit simulation of this manager's dialect
	}
	if m.RealDrivers == nil {
		return m.Drivers.For(c.Manager) // offline/tests: no real registry wired
	}
	d, err := m.RealDrivers.For(c.Manager)
	if err != nil {
		return nil, fmt.Errorf("%w: no real driver for manager %q yet — register with --config emulate=true to simulate it", ErrNotImplemented, c.Manager)
	}
	return d, nil
}

// DispatchMode reports how a cluster will dispatch: "emulated" (explicit
// simulation), "real" (a real backend driver exists), or "not-implemented"
// (asked for a real backend with no driver yet).
func (m *Manager) DispatchMode(c graph.ClusterGraph) string {
	if c.Emulated() {
		return "emulated"
	}
	if m.RealDrivers == nil {
		return "emulated"
	}
	if _, err := m.RealDrivers.For(c.Manager); err == nil {
		return "real"
	}
	return "not-implemented"
}

// monitorOnce reconciles every active job against its driver.
func (m *Manager) monitorOnce() {
	for _, j := range m.Queue.Active() {
		if j.RemoteHandle == "" {
			continue // matched but not yet dispatched
		}
		c, ok := m.Fleet.Get(j.ClusterID)
		if !ok {
			continue
		}
		drv, err := m.driverFor(c)
		if err != nil {
			continue
		}
		st, note, err := drv.Status(c, j.RemoteHandle)
		if err != nil {
			continue
		}
		if st != j.State || note != j.Note {
			j.State, j.Note = st, note
			_ = m.Queue.Update(j)
			m.logf("status %s -> %s (%s)", j.ID, st, note)
		}
		if st.Terminal() {
			if err := m.Matcher.Free(j.AllocID); err == nil {
				m.logf("free %s (alloc %s) back to cluster %s", j.ID, j.AllocID, j.ClusterID)
			}
		}
	}
}

func (m *Manager) failJob(j queue.Job, note string) {
	if j.AllocID != "" {
		_ = m.Matcher.Free(j.AllocID)
	}
	j.State = queue.Failed
	j.Note = note
	_ = m.Queue.Update(j)
	m.logf("fail %s: %s", j.ID, note)
}

// failInfeasible fails a job that can never run, first asking an optional
// Reconciler for a work-preserving suggestion. Default is suggest-not-act: the
// job fails but carries the proposed spec; AutoResubmit submits it as a new job.
func (m *Manager) failInfeasible(j queue.Job) {
	note := "impossible: no cluster can ever satisfy this job (subsystems or capacity)"
	if m.Reconciler != nil {
		if prop, ok := m.Reconciler.Propose(j.Spec, reconcile.BuildFleetView(m.Fleet)); ok {
			if m.anyFeasible(prop.Jobspec) {
				sp := prop.Jobspec
				j.SuggestedSpec = &sp
				j.Suggestion = prop.Rationale
				note = "impossible as submitted; suggested: " + prop.Rationale
				if m.AutoResubmit {
					nid, _ := m.Submit(prop.Jobspec)
					note += " (auto-resubmitted as " + nid + ")"
				}
			}
		}
	}
	j.State = queue.Failed
	j.Note = note
	_ = m.Queue.Update(j)
	m.logf("fail %s: %s", j.ID, note)
}

func (m *Manager) logf(format string, args ...any) {
	if m.Logger != nil {
		m.Logger.Printf(format, args...)
	}
}

func contentToDriver(c cluster.Content) cluster.Content { return c }

func jobID(n int) string { return "job-" + pad(n) }

func pad(n int) string {
	s := []byte{'0', '0', '0', '0'}
	i := len(s) - 1
	for n > 0 && i >= 0 {
		s[i] = byte('0' + n%10)
		n /= 10
		i--
	}
	return string(s)
}

// Logs returns the target-side log for a job by resolving its driver+handle.
func (m *Manager) Logs(jobID string) (string, error) {
	j, ok := m.Queue.Get(jobID)
	if !ok {
		return "", fmt.Errorf("job %s not found", jobID)
	}
	c, ok := m.Fleet.Get(j.ClusterID)
	if !ok {
		return "", fmt.Errorf("cluster %s not found", j.ClusterID)
	}
	drv, err := m.driverFor(c)
	if err != nil {
		return "", err
	}
	return drv.Logs(c, j.RemoteHandle)
}
