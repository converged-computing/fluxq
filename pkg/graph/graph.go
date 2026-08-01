// Package graph models the fleet as a set of clusters, each described by
// Fluxion JGF graphs — one per subsystem. Containment is the consuming space
// (nodes are allocated); auxiliary spaces (software, network) are satisfy-only.
// Clusters load from a simple on-disk export (a directory of JGF files) via a
// Loader, so the same files feed both the sim matcher and real Fluxion.
package graph

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type ManagerType string

const (
	FluxOperator  ManagerType = "flux-operator"
	SlurmOperator ManagerType = "slurm-operator"
	K8sJob        ManagerType = "k8s-job"
	FluxURI       ManagerType = "flux-uri"
)

const ContainmentSubsystem = "containment"

// KnownManagers is the canonical, ordered set of manager types fluxq understands.
// Every manager can be emulated; whether it can dispatch for real depends on
// which drivers a server has registered (see `fluxq managers`).
func KnownManagers() []ManagerType {
	return []ManagerType{FluxOperator, SlurmOperator, K8sJob, FluxURI}
}

// ClusterGraph is one selectable environment. Subsystems always includes
// "containment"; others are present only if exported. Manager and Handle are
// read from the containment root vertex properties.
type ClusterGraph struct {
	ID      string
	Manager ManagerType
	Handle  string
	// Config is backend-interpreted dispatch metadata provided at registration
	// (the core is agnostic to its keys). Flux reads "uri" (local | ssh://…);
	// Kubernetes will read "kubeconfig". Absent/empty means emulated dispatch.
	Config     map[string]string
	Subsystems map[string]*JGF
	// Descriptive[sub]=true means satisfy-only (never allocated). Containment is
	// always countable (false). Absent key defaults to descriptive for auxiliary
	// subsystems.
	Descriptive map[string]bool
}

// Capabilities returns the non-reserved property keys on the containment root.
//
// This is a raw JGF accessor for hand-authored graphs that carry RFC-31
// key-presence properties; it is NOT an advertised capability set and nothing in
// matching consults it. What a cluster offers is its SUBSYSTEMS (see
// vocabulary.Values) — the graphs the matcher actually traverses.
func (c ClusterGraph) Capabilities() map[string]bool {
	out := map[string]bool{}
	g := c.Containment()
	if g == nil {
		return out
	}
	root := g.find(func(v Vertex) bool { return v.Metadata.Type == "cluster" })
	if root == nil {
		return out
	}
	for k := range root.Metadata.Properties {
		if k == "manager" || k == "handle" {
			continue
		}
		out[k] = true
	}
	return out
}

// Containment returns the consuming graph.
func (c ClusterGraph) Containment() *JGF { return c.Subsystems[ContainmentSubsystem] }

// Cfg returns a backend dispatch-config value (nil-safe).
// CoresPerNode is the SMALLEST core count across the cluster's node vertices,
// or 0 when containment does not say. A transform sizing a job to the cluster
// must fit the smallest node, since any node may be allocated. This is the
// level-2 fact a jobspec cannot carry: the selector chooses a node count without
// knowing which cluster it will land on.
func (c ClusterGraph) CoresPerNode() int {
	g := c.Containment()
	if g == nil {
		return 0
	}
	byID, children := g.IndexExported()
	minCores := 0
	for _, n := range g.VerticesOfTypeExported("node") {
		cores := 0
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
				if v.Metadata.Type == "core" {
					cores += size
				}
				walk(cid)
			}
		}
		walk(n.ID)
		if cores > 0 && (minCores == 0 || cores < minCores) {
			minCores = cores
		}
	}
	return minCores
}

func (c ClusterGraph) Cfg(key string) string { return c.Config[key] }

// Emulated reports whether this cluster is an explicit simulation (config
// emulate=true) rather than a request to dispatch to a real backend. This is a
// core dispatch-mode selector — the one reserved config key the core reads;
// everything else in Config is backend-interpreted.
func (c ClusterGraph) Emulated() bool { return c.Cfg("emulate") == "true" }

// Fleet is a concurrency-safe registry of clusters. Clusters can be added and
// removed at runtime (via the API), so it is always used as *Fleet and must not
// be copied.
type Fleet struct {
	mu       sync.RWMutex
	clusters []ClusterGraph
}

// NewFleet builds a fleet from zero or more clusters.
func NewFleet(cgs ...ClusterGraph) *Fleet { return &Fleet{clusters: cgs} }

func (f *Fleet) Get(id string) (ClusterGraph, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, c := range f.clusters {
		if c.ID == id {
			return c, true
		}
	}
	return ClusterGraph{}, false
}

// Clusters returns a snapshot slice (safe to range without holding the lock).
func (f *Fleet) Clusters() []ClusterGraph {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]ClusterGraph, len(f.clusters))
	copy(out, f.clusters)
	return out
}

// Names returns the registered cluster ids.
func (f *Fleet) Names() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]string, 0, len(f.clusters))
	for _, c := range f.clusters {
		out = append(out, c.ID)
	}
	return out
}

func (f *Fleet) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.clusters)
}

// Add registers a cluster, replacing any existing one with the same id.
func (f *Fleet) Add(cg ClusterGraph) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, c := range f.clusters {
		if c.ID == cg.ID {
			f.clusters[i] = cg
			return
		}
	}
	f.clusters = append(f.clusters, cg)
}

// Remove unregisters a cluster by id; returns whether it was present.
func (f *Fleet) Remove(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, c := range f.clusters {
		if c.ID == id {
			f.clusters = append(f.clusters[:i], f.clusters[i+1:]...)
			return true
		}
	}
	return false
}

// Loader turns an export into a Fleet. DirectoryLoader is the reference
// implementation; a resource-secretary export loader would satisfy the same
// interface, keeping discovery off the request path.
type Loader interface {
	LoadFleet(path string) (*Fleet, error)
}

// DirectoryLoader reads <path>/<clusterID>/<subsystem>.json JGF files. Each
// cluster's containment.json is required; other files become named subsystems.
type DirectoryLoader struct{}

func (DirectoryLoader) LoadFleet(path string) (*Fleet, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	fleet := NewFleet()
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cg, err := loadClusterDir(filepath.Join(path, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("cluster %q: %w", e.Name(), err)
		}
		fleet.Add(cg)
	}
	return fleet, nil
}

func loadClusterDir(dir string) (ClusterGraph, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return ClusterGraph{}, err
	}
	cg := ClusterGraph{Subsystems: map[string]*JGF{}}
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		sub := strings.TrimSuffix(f.Name(), ".json")
		g, err := loadJGF(filepath.Join(dir, f.Name()))
		if err != nil {
			return ClusterGraph{}, err
		}
		cg.Subsystems[sub] = g
	}
	cont := cg.Containment()
	if cont == nil {
		return ClusterGraph{}, fmt.Errorf("missing %s.json", ContainmentSubsystem)
	}
	root := cont.find(func(v Vertex) bool { return v.Metadata.Type == "cluster" })
	if root == nil {
		return ClusterGraph{}, fmt.Errorf("containment has no cluster root vertex")
	}
	cg.ID = firstNonEmpty(root.Metadata.Name, root.Metadata.Basename)
	cg.Manager = ManagerType(root.Metadata.Properties["manager"])
	cg.Handle = root.Metadata.Properties["handle"]
	return cg, nil
}

func firstNonEmpty(xs ...string) string {
	for _, x := range xs {
		if x != "" {
			return x
		}
	}
	return ""
}

// IsDescriptive reports whether a subsystem is satisfy-only (never allocated).
// Containment is always countable; unknown auxiliary subsystems default to
// descriptive.
func (c ClusterGraph) IsDescriptive(sub string) bool {
	if sub == ContainmentSubsystem {
		return false
	}
	if c.Descriptive != nil {
		if d, ok := c.Descriptive[sub]; ok {
			return d
		}
	}
	return true
}

// AttachSubsystem adds/replaces a named subsystem tree on the cluster.
func (c *ClusterGraph) AttachSubsystem(name string, g *JGF, descriptive bool) {
	if c.Subsystems == nil {
		c.Subsystems = map[string]*JGF{}
	}
	if c.Descriptive == nil {
		c.Descriptive = map[string]bool{}
	}
	c.Subsystems[name] = g
	c.Descriptive[name] = descriptive
}

// DetachSubsystem removes a named subsystem (containment cannot be removed).
func (c *ClusterGraph) DetachSubsystem(name string) bool {
	if name == ContainmentSubsystem || c.Subsystems == nil {
		return false
	}
	if _, ok := c.Subsystems[name]; !ok {
		return false
	}
	delete(c.Subsystems, name)
	delete(c.Descriptive, name)
	return true
}
