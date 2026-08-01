package cluster

import (
	"testing"

	"github.com/converged-computing/fluxq/pkg/graph"
)

func TestK8sDriverServesFluxOperator(t *testing.T) {
	// The experiment runs MiniClusters: a flux-operator cluster MUST resolve to
	// the Kubernetes driver. Without this the transform emits a MiniCluster that
	// no driver will dispatch, and registering as k8s-job instead yields a
	// single-pod batch/v1 Job — silently breaking every multi-node job.
	r := NewRegistry(NewK8sDriver())
	for _, mt := range []graph.ManagerType{graph.K8sJob, graph.FluxOperator} {
		d, err := r.For(mt)
		if err != nil {
			t.Fatalf("no driver for %q: %v", mt, err)
		}
		if _, ok := d.(*K8sDriver); !ok {
			t.Fatalf("%q resolved to %T, want *K8sDriver", mt, d)
		}
	}
	if _, err := r.For(graph.FluxURI); err == nil {
		t.Error("flux-uri must NOT resolve to the k8s driver")
	}
}
