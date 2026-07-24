package matcher

import (
	"fmt"
	"sync"

	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
)

// SimMatcher traverses the loaded JGF trees the way Fluxion would, without the
// CGO/flux-sched toolchain. Containment is walked and allocated at node
// granularity (fleet-level placement is node-count, not node-identity);
// auxiliary subsystems are satisfy-only and never consumed. Allocation state
// lives here, not in the JGF, so graphs stay pristine and Free is exact.
type SimMatcher struct {
	mu        sync.Mutex
	clusters  map[string]graph.ClusterGraph
	order     []string // stable iteration order for determinism before shuffle
	allocated map[string]map[string]bool
	live      map[string]liveAlloc
	nextID    int
}

type liveAlloc struct {
	cluster   string
	vertexIDs []string
}

func NewSim(fleet *graph.Fleet) *SimMatcher {
	m := &SimMatcher{clusters: map[string]graph.ClusterGraph{}, allocated: map[string]map[string]bool{}, live: map[string]liveAlloc{}}
	if fleet != nil {
		for _, c := range fleet.Clusters() {
			_ = m.AddCluster(c)
		}
	}
	return m
}

func (m *SimMatcher) AddCluster(cg graph.ClusterGraph) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.clusters[cg.ID]; !ok {
		m.order = append(m.order, cg.ID)
		m.allocated[cg.ID] = map[string]bool{}
	}
	m.clusters[cg.ID] = cg
	return nil
}

func (m *SimMatcher) RemoveCluster(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clusters, id)
	for i, c := range m.order {
		if c == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	for allocID, rec := range m.live {
		if rec.cluster == id {
			delete(m.live, allocID)
		}
	}
	delete(m.allocated, id)
	return nil
}

func (m *SimMatcher) AddSubsystem(clusterID, name string, g *graph.JGF, descriptive bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cg, ok := m.clusters[clusterID]
	if !ok {
		return fmt.Errorf("cluster %q not registered", clusterID)
	}
	cg.AttachSubsystem(name, g, descriptive)
	m.clusters[clusterID] = cg
	return nil
}

func (m *SimMatcher) RemoveSubsystem(clusterID, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cg, ok := m.clusters[clusterID]
	if !ok {
		return fmt.Errorf("cluster %q not registered", clusterID)
	}
	if !cg.DetachSubsystem(name) {
		return fmt.Errorf("subsystem %q not found on %q", name, clusterID)
	}
	m.clusters[clusterID] = cg
	return nil
}

// nodeCapacity sums a node's resources by type, recursing the containment
// subtree (cores nest under a socket).
func nodeCapacity(g *graph.JGF, nodeID string) (cores, gpus, mem int) {
	byID, children := g.IndexExported()
	var walk func(id string)
	walk = func(id string) {
		for _, cid := range children[id] {
			v := byID[cid]
			if v == nil {
				continue
			}
			size := v.Metadata.Size
			if size <= 0 {
				size = 1
			}
			switch v.Metadata.Type {
			case "core":
				cores += size
			case "gpu":
				gpus += size
			case "memory":
				mem += size
			}
			walk(cid)
		}
	}
	walk(nodeID)
	return
}

// matchVertex reports whether a graph vertex matches a requested Flux resource,
// by TYPE (the Fluxion-native way). It is tolerant: a subsystem may type a
// vertex by its package name (type "lammps") or as type "software" with
// name/basename "lammps"; either matches a request for type "lammps".
func matchVertex(v *graph.Vertex, r jobspec.Resource) bool {
	if v == nil {
		return false
	}
	t := r.Type
	return v.Metadata.Type == t || v.Metadata.Basename == t || v.Metadata.Name == t
}

// satisfyResource does a subtree-containment match: some vertex matches r, and
// every child resource is satisfied UNDER it (recursively). This is what makes
// lammps-WITH-kokkos different from lammps.
func satisfyResource(g *graph.JGF, r jobspec.Resource) bool {
	byID, children := g.IndexExported()
	var childrenSat func(parent string, reqs []jobspec.Resource) bool
	childrenSat = func(parent string, reqs []jobspec.Resource) bool {
		for _, cr := range reqs {
			ok := false
			for _, cid := range children[parent] {
				if matchVertex(byID[cid], cr) && childrenSat(cid, cr.With) {
					ok = true
					break
				}
			}
			if !ok {
				return false
			}
		}
		return true
	}
	for id, v := range byID {
		if matchVertex(v, r) && childrenSat(id, r.With) {
			return true
		}
	}
	return false
}

// subsystemSatisfies gates one requested subsystem section against a cluster by
// traversing that subsystem's JGF graph. Everything is JGF: a subsystem the
// cluster has not registered as a graph is not satisfiable here (no flat
// capability-property fallback — constraints prune, and can't express structure
// like lammps->kokkos). Every top-level resource in the section must satisfy.
func (m *SimMatcher) subsystemSatisfies(cg graph.ClusterGraph, sub string, section []jobspec.Resource) bool {
	g := cg.Subsystems[sub]
	if g == nil {
		return false
	}
	// OR across the section's concrete alternatives (anyof); AND within each.
	for _, concrete := range jobspec.ExpandSection(section) {
		all := true
		for _, r := range concrete {
			if !satisfyResource(g, r) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

func (m *SimMatcher) fittingNodes(cg graph.ClusterGraph, js jobspec.Jobspec, onlyFree bool) []string {
	g := cg.Containment()
	if g == nil {
		return nil
	}
	needCores, needGPU, needMem := js.CoresPerNode(), js.GPUsPerNode(), js.MemGBPerNode()
	var fit []string
	for _, n := range g.VerticesOfTypeExported("node") {
		if onlyFree && m.allocated[cg.ID][n.ID] {
			continue
		}
		c, gp, mem := nodeCapacity(g, n.ID)
		if c >= needCores && gp >= needGPU && mem >= needMem {
			fit = append(fit, n.ID)
		}
	}
	return fit
}

// Evaluate: satisfy-only, one Candidate per cluster. No allocation.
func (m *SimMatcher) Evaluate(js jobspec.Jobspec) []Candidate {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Candidate, 0, len(m.order))
	for _, id := range m.order {
		cg := m.clusters[id]
		c := Candidate{Cluster: id}
		subsOK := true
		for sub, section := range js.Requires {
			if m.subsystemSatisfies(cg, sub, section) {
				c.Matched = append(c.Matched, sub)
			} else {
				c.Missing = append(c.Missing, sub)
				subsOK = false
			}
		}
		needNodes := js.Nodes()
		structOK := len(m.fittingNodes(cg, js, false)) >= needNodes
		if !structOK {
			c.Missing = append(c.Missing, graph.ContainmentSubsystem)
		}
		c.Feasible = subsOK && structOK
		c.FreeNodes = len(m.fittingNodes(cg, js, true))
		c.FreeNow = c.Feasible && c.FreeNodes >= needNodes
		out = append(out, c)
	}
	return out
}

// Allocate consumes containment (the only countable subsystem today) on the
// chosen cluster. allocated=false, err=nil means "no room right now".
func (m *SimMatcher) Allocate(js jobspec.Jobspec, clusterID string) (Allocation, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cg, ok := m.clusters[clusterID]
	if !ok {
		return Allocation{}, false, fmt.Errorf("cluster %q not registered", clusterID)
	}
	needNodes := js.Nodes()
	free := m.fittingNodes(cg, js, true)
	if len(free) < needNodes {
		return Allocation{}, false, nil // full now
	}
	chosen := free[:needNodes]
	id := m.mint()
	for _, v := range chosen {
		m.allocated[clusterID][v] = true
	}
	m.live[id] = liveAlloc{cluster: clusterID, vertexIDs: chosen}
	return Allocation{ID: id, ClusterID: clusterID, VertexIDs: chosen}, true, nil
}

func (m *SimMatcher) Free(allocID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.live[allocID]
	if !ok {
		return nil
	}
	for _, v := range rec.vertexIDs {
		delete(m.allocated[rec.cluster], v)
	}
	delete(m.live, allocID)
	return nil
}

func (m *SimMatcher) mint() string {
	m.nextID++
	return fmt.Sprintf("alloc-%d", m.nextID)
}
