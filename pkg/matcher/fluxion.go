//go:build fluxion

// Package matcher — REAL Fluxion matcher using the flux-sched reapi Go bindings.
//
// Built only with `-tags fluxion` inside the flux-sched environment. It links
// against /opt/flux-sched via CGO; the dev SimMatcher is the offline stand-in.
//
// PROCESS-ISOLATED CONTEXTS. flux-sched's string interner is a process-global
// singleton that finalizes after the first InitContext, so a second in-process
// InitContext that introduces a NEW resource type fails ("interner is
// finalized"). fleetq's subsystems are custom-typed and discovered dynamically,
// so we cannot know all types up front and cannot prime. Instead, EVERY reapi
// context runs in its OWN worker subprocess (fluxion_worker.go) with its OWN
// interner. A cluster holds:
//   - one ALLOCATABLE containment worker (nil until a containment subsystem is
//     added — an empty registration is legal, it just can't schedule), and
//   - one SATISFY-ONLY worker per auxiliary subsystem graph.
//
// Editing a subsystem is delete-and-recreate: AddSubsystem kills the existing
// worker for that name and spawns a fresh one, which — being a new process —
// can load new types without hitting the finalized interner.
//
//   - Evaluate  -> satisfy the containment worker (structural feasibility) AND
//     satisfy each requested subsystem worker against ITS graph, each gated on
//     sat && err==nil (a jobspec referencing a type absent from a subsystem
//     graph is correctly unsatisfiable).
//   - Allocate  -> allocate on the chosen cluster's containment worker only.
//   - Free      -> cancel(jobid) on the owning cluster's containment worker.
package matcher

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"sync"

	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
)

const fluxOpts = `{"matcher_policy": "first", "load_format": "jgf", "match_format": "jgf"}`

// fworker is a handle to one reapi context running in a worker subprocess.
type fworker struct {
	mu   sync.Mutex
	cmd  *exec.Cmd
	enc  *json.Encoder
	scan *bufio.Scanner
}

// spawnWorker starts a worker subprocess (this binary re-executed as a worker),
// InitContexts it with the given JGF body, and returns once it is ready.
func spawnWorker(graphBody string) (*fworker, error) {
	self, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("locate worker binary: %w", err)
	}
	cmd := exec.Command(self, workerArg)
	cmd.Stderr = os.Stderr // worker diagnostics surface on the server's stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start worker: %w", err)
	}
	w := &fworker{cmd: cmd, enc: json.NewEncoder(stdin)}
	w.scan = bufio.NewScanner(stdout)
	w.scan.Buffer(make([]byte, 1<<20), 64<<20)
	resp, err := w.call(wreq{Op: "init", Graph: graphBody})
	if err != nil {
		w.close()
		return nil, err
	}
	if resp.Err != "" {
		w.close()
		return nil, fmt.Errorf("init fluxion context: %s", resp.Err)
	}
	return w, nil
}

// call sends one request and reads one response line. The error return is for
// transport failures only (worker gone); reapi-level outcomes ride in wresp.
func (w *fworker) call(r wreq) (wresp, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.enc.Encode(r); err != nil {
		return wresp{}, fmt.Errorf("worker write: %w", err)
	}
	if !w.scan.Scan() {
		if err := w.scan.Err(); err != nil {
			return wresp{}, fmt.Errorf("worker read: %w", err)
		}
		return wresp{}, fmt.Errorf("worker exited")
	}
	var resp wresp
	if err := json.Unmarshal(w.scan.Bytes(), &resp); err != nil {
		return wresp{}, fmt.Errorf("worker decode: %w", err)
	}
	return resp, nil
}

// satisfy returns true only when reapi reports satisfiable with no error, which
// matches the in-process semantics (an absent type surfaces as an error).
func (w *fworker) satisfy(spec string) bool {
	resp, err := w.call(wreq{Op: "satisfy", Spec: spec})
	return err == nil && resp.Err == "" && resp.Sat
}

func (w *fworker) close() {
	if w == nil || w.cmd == nil {
		return
	}
	_ = w.cmd.Process.Kill()
	_ = w.cmd.Wait()
}

type subWorker struct {
	w           *fworker
	descriptive bool
}

type clusterCtx struct {
	id         string
	cont       *fworker // containment (allocatable); nil until registered
	subsystems map[string]*subWorker
}

type FluxionMatcher struct {
	mu       sync.Mutex
	clusters map[string]*clusterCtx
	order    []string
	owner    map[string]*clusterCtx // allocID -> cluster
}

// workerFromJGF spawns a worker for a subsystem graph. A nil graph yields a nil
// worker (an empty cluster with no containment yet).
func workerFromJGF(jgf *graph.JGF) (*fworker, error) {
	if jgf == nil {
		return nil, nil
	}
	body, err := jgf.JSON()
	if err != nil {
		return nil, err
	}
	return spawnWorker(body)
}

func NewFluxion(fleet *graph.Fleet) (*FluxionMatcher, error) {
	m := &FluxionMatcher{clusters: map[string]*clusterCtx{}, owner: map[string]*clusterCtx{}}
	if fleet == nil {
		return m, nil
	}
	for _, cg := range fleet.Clusters() {
		if err := m.AddCluster(cg); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// AddCluster registers a cluster. It may be empty (no containment yet); any
// subsystems already attached to the graph become their own worker processes.
func (m *FluxionMatcher) AddCluster(cg graph.ClusterGraph) error {
	cont, err := workerFromJGF(cg.Containment())
	if err != nil {
		return fmt.Errorf("cluster %s containment: %w", cg.ID, err)
	}
	cc := &clusterCtx{id: cg.ID, cont: cont, subsystems: map[string]*subWorker{}}
	for name, g := range cg.Subsystems {
		if name == graph.ContainmentSubsystem {
			continue
		}
		w, err := workerFromJGF(g)
		if err != nil {
			cc.closeAll()
			return fmt.Errorf("cluster %s subsystem %s: %w", cg.ID, name, err)
		}
		cc.subsystems[name] = &subWorker{w: w, descriptive: cg.IsDescriptive(name)}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.clusters[cg.ID]; ok {
		old.closeAll()
	} else {
		m.order = append(m.order, cg.ID)
	}
	m.clusters[cg.ID] = cc
	return nil
}

func (cc *clusterCtx) closeAll() {
	cc.cont.close()
	for _, s := range cc.subsystems {
		s.w.close()
	}
}

func (m *FluxionMatcher) RemoveCluster(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cc, ok := m.clusters[id]; ok {
		cc.closeAll()
	}
	delete(m.clusters, id)
	for i, c := range m.order {
		if c == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	for allocID, c := range m.owner {
		if c.id == id {
			delete(m.owner, allocID)
		}
	}
	return nil
}

// AddSubsystem instantiates a subsystem's JGF as its own worker process.
// Re-adding an existing subsystem is delete-and-recreate: the old worker is
// killed and a fresh one spawned, so a changed graph (even with new types)
// loads cleanly. Containment (name=containment) becomes the allocatable worker.
func (m *FluxionMatcher) AddSubsystem(clusterID, name string, g *graph.JGF, descriptive bool) error {
	w, err := workerFromJGF(g)
	if err != nil {
		return fmt.Errorf("subsystem %s on %s: %w", name, clusterID, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cc, ok := m.clusters[clusterID]
	if !ok {
		w.close()
		return fmt.Errorf("cluster %q not registered", clusterID)
	}
	if name == graph.ContainmentSubsystem {
		cc.cont.close() // delete-and-recreate
		cc.cont = w
		return nil
	}
	if old, ok := cc.subsystems[name]; ok {
		old.w.close() // delete-and-recreate
	}
	cc.subsystems[name] = &subWorker{w: w, descriptive: descriptive}
	return nil
}

func (m *FluxionMatcher) RemoveSubsystem(clusterID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cc, ok := m.clusters[clusterID]
	if !ok {
		return fmt.Errorf("cluster %q not registered", clusterID)
	}
	if name == graph.ContainmentSubsystem {
		cc.cont.close()
		cc.cont = nil
		return nil
	}
	s, ok := cc.subsystems[name]
	if !ok {
		return fmt.Errorf("subsystem %q not found on %q", name, clusterID)
	}
	s.w.close()
	delete(cc.subsystems, name)
	return nil
}

func (m *FluxionMatcher) Evaluate(js jobspec.Jobspec) []Candidate {
	spec, specErr := js.ToFluxSpec()
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Candidate, 0, len(m.order))
	for _, id := range m.order {
		cc := m.clusters[id]
		c := Candidate{Cluster: id}

		// containment feasibility (structural). No containment => not feasible.
		contOK := cc.cont != nil && specErr == nil && cc.cont.satisfy(spec)
		if !contOK {
			c.Missing = append(c.Missing, graph.ContainmentSubsystem)
		}

		// each requested subsystem satisfied against its OWN worker.
		subsOK := true
		for sub := range js.Requires {
			ok := false
			if ctx := cc.subsystems[sub]; ctx != nil {
				// OR across concrete alternatives (anyof): satisfy if any renders
				// and matches against this subsystem's graph.
				for _, concrete := range jobspec.ExpandSection(js.Requires[sub]) {
					if ss, err := jobspec.SubsystemFluxSpec(concrete); err == nil && ctx.w.satisfy(ss) {
						ok = true
						break
					}
				}
			}
			if ok {
				c.Matched = append(c.Matched, sub)
			} else {
				c.Missing = append(c.Missing, sub)
				subsOK = false
			}
		}

		c.Feasible = contOK && subsOK
		c.FreeNow = c.Feasible // optimistic; Allocate is the authority on capacity
		out = append(out, c)
	}
	return out
}

func (m *FluxionMatcher) Allocate(js jobspec.Jobspec, clusterID string) (Allocation, bool, error) {
	spec, err := js.ToFluxSpec()
	if err != nil {
		return Allocation{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cc, ok := m.clusters[clusterID]
	if !ok {
		return Allocation{}, false, fmt.Errorf("cluster %q not registered", clusterID)
	}
	if cc.cont == nil {
		return Allocation{}, false, nil // no containment: nothing to allocate
	}
	resp, err := cc.cont.call(wreq{Op: "allocate", Spec: spec})
	if err != nil {
		return Allocation{}, false, err // genuine transport/worker failure
	}
	if resp.Err != "" || resp.Allocated == "" {
		return Allocation{}, false, nil // no room now; feasibility is Evaluate's job
	}
	id := strconv.FormatUint(resp.Jobid, 10)
	m.owner[id] = cc
	return Allocation{ID: id, ClusterID: clusterID}, true, nil
}

func (m *FluxionMatcher) Free(allocID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cc, ok := m.owner[allocID]
	if !ok {
		return nil
	}
	jobid, err := strconv.ParseUint(allocID, 10, 64)
	if err != nil {
		return err
	}
	if cc.cont != nil {
		if resp, err := cc.cont.call(wreq{Op: "cancel", Jobid: jobid}); err != nil {
			return err
		} else if resp.Err != "" {
			return fmt.Errorf("cancel: %s", resp.Err)
		}
	}
	delete(m.owner, allocID)
	return nil
}
