package cluster_test

import (
	"testing"

	"github.com/converged-computing/fluxq/pkg/cluster"
	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/queue"
)

func submitStatus(t *testing.T, m graph.ManagerType, c cluster.Content) (queue.State, string) {
	t.Helper()
	d := cluster.NewEmulatedDriver(m, cluster.EmulatorConfig{})
	h, err := d.Submit(graph.ClusterGraph{Manager: m}, c)
	if err != nil {
		return queue.Failed, err.Error()
	}
	st, note, _ := d.Status(graph.ClusterGraph{Manager: m}, h)
	return st, note
}

// Each backend validates its OWN dialect: valid artifacts are accepted, and
// artifacts malformed for that dialect are rejected at submit time.
func TestPerBackendValidation(t *testing.T) {
	cases := []struct {
		name   string
		mgr    graph.ManagerType
		c      cluster.Content
		reject bool
	}{
		{"minicluster ok", graph.FluxOperator, cluster.Content{Kind: "manifest", Payload: "kind: MiniCluster\n  size: 2\n  image: img\n"}, false},
		{"minicluster wrong kind", graph.FluxOperator, cluster.Content{Kind: "manifest", Payload: "kind: Job\n image: img\n"}, true},
		{"k8s job ok", graph.K8sJob, cluster.Content{Kind: "manifest", Payload: "kind: Job\n image: img\n command: [x]\n"}, false},
		{"k8s job no image", graph.K8sJob, cluster.Content{Kind: "manifest", Payload: "kind: Job\n command: [x]\n"}, true},
		{"slurm ok", graph.SlurmOperator, cluster.Content{Kind: "manifest", Payload: "kind: SlurmJob\n nodes: 2\n ntasks: 4\n image: img\n"}, false},
		{"flux jobspec ok", graph.FluxURI, cluster.Content{Kind: "jobspec", Payload: `{"version":1,"resources":[{"type":"node","count":1}],"tasks":[{"command":["hostname"]}]}`}, false},
		{"flux jobspec not json", graph.FluxURI, cluster.Content{Kind: "jobspec", Payload: "flux submit lmp"}, true},
		{"flux jobspec missing tasks", graph.FluxURI, cluster.Content{Kind: "jobspec", Payload: `{"version":1,"resources":[{"type":"node","count":1}]}`}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st, note := submitStatus(t, tc.mgr, tc.c)
			rejected := st == queue.Failed
			if rejected != tc.reject {
				t.Fatalf("reject=%v want %v (note=%q)", rejected, tc.reject, note)
			}
		})
	}
}

// Wrong content KIND for a backend is refused by Submit itself.
func TestBackendRefusesWrongKind(t *testing.T) {
	d := cluster.NewEmulatedDriver(graph.K8sJob, cluster.EmulatorConfig{})
	if _, err := d.Submit(graph.ClusterGraph{}, cluster.Content{Kind: "jobspec", Payload: "{}"}); err == nil {
		t.Fatal("k8s backend must refuse a command")
	}
}
