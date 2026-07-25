// Package cluster is the target boundary: how work is SUBMITTED to a chosen
// cluster and how its status is later READ BACK. Dispatch and monitoring are
// two methods of ONE driver per manager type, because they share the same
// connection and handle (a kube-context, or a flux:// URI). This is the answer
// to "do we need a whole separate interface to monitor?" — no, it is Status()
// on the same driver that did Submit().
//
// The architecture is Flux-native up to this boundary (jobspec, Fluxion match,
// receipt spine) and only becomes foreign here, at the exact point translation
// happens. Each driver returns a NATIVE handle; the manager keeps the mapping
// receipt -> (driver, handle) and reconciles Status on a loop. Push where it is
// free (k8s watch, flux events), poll where it is not (slurmrestd).
package cluster

import (
	"fmt"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/queue"
)

// Content is the manager-native artifact transform produced: a manifest to
// apply, or a command to run.
type Content struct {
	Kind    string // "manifest" | "command"
	Payload string // YAML manifest, or a shell command line
}

// Driver submits and monitors work for one ManagerType.
type Driver interface {
	Type() graph.ManagerType
	// Submit applies/runs content on the target and returns a native handle.
	Submit(target graph.ClusterGraph, c Content) (handle string, err error)
	// Status maps the native state back onto our lifecycle.
	Status(target graph.ClusterGraph, handle string) (queue.State, string, error)
	// Cancel best-effort tears down the remote job (used on timeout/cleanup).
	Cancel(target graph.ClusterGraph, handle string) error
	// Logs returns the (start of the) job's output log — the dispatch paper's
	// success signal, and how a checker/recovery agent inspects a run.
	Logs(target graph.ClusterGraph, handle string) (string, error)
}

// Discoverer is an OPTIONAL driver capability: introspect a cluster's nodes into
// backend-neutral NodeFacts. A backend that can't introspect simply does not
// implement it — discovery is dispatched by type-assertion, never by manager
// name, so adding Flux/Slurm discovery later is just implementing this interface.
type Discoverer interface {
	Discover(target graph.ClusterGraph) ([]NodeFacts, error)
}

// Registry resolves a driver by manager type.
type Registry struct {
	drivers map[graph.ManagerType]Driver
}

func NewRegistry(ds ...Driver) *Registry {
	r := &Registry{drivers: map[graph.ManagerType]Driver{}}
	for _, d := range ds {
		r.drivers[d.Type()] = d
	}
	return r
}

func (r *Registry) For(t graph.ManagerType) (Driver, error) {
	d, ok := r.drivers[t]
	if !ok {
		return nil, fmt.Errorf("no dispatch driver registered for manager %q", t)
	}
	return d, nil
}

// Has reports whether the registry has a driver for manager t.
func (r *Registry) Has(t graph.ManagerType) bool {
	if r == nil {
		return false
	}
	_, ok := r.drivers[t]
	return ok
}
