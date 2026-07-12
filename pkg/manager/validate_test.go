package manager

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/converged-computing/fleetq/pkg/cluster"
	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
	"github.com/converged-computing/fleetq/pkg/queue"
	"github.com/converged-computing/fleetq/pkg/transform"
)

const (
	goodJob = "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: t\nspec:\n  template:\n    spec:\n      containers:\n        - name: a\n          image: img\n"
	miniCl  = "apiVersion: flux-framework.org/v1alpha2\nkind: MiniCluster\nmetadata:\n  name: t\n"
	noKind  = "apiVersion: batch/v1\nmetadata:\n  name: t\n"
	hostPod = "apiVersion: batch/v1\nkind: Job\nspec:\n  template:\n    spec:\n      volumes:\n        - hostPath:\n            path: /\n"
)

func manifest(s string) cluster.Content { return cluster.Content{Kind: "manifest", Payload: s} }

func TestDefaultValidateVerdicts(t *testing.T) {
	k8s := graph.ClusterGraph{ID: "c1", Manager: graph.K8sJob}
	flux := graph.ClusterGraph{ID: "f1", Manager: graph.FluxURI}
	cases := []struct {
		name   string
		target graph.ClusterGraph
		c      cluster.Content
		want   verdict
	}{
		{"valid job", k8s, manifest(goodJob), verdictValid},
		{"missing kind", k8s, manifest(noKind), verdictRepairable},
		{"minicluster on k8s-job", k8s, manifest(miniCl), verdictWrongTarget},
		{"hostpath policy", k8s, manifest(hostPod), verdictRepairable},
		{"empty", k8s, manifest("  "), verdictRepairable},
		{"valid jobspec", flux, cluster.Content{Kind: "jobspec", Payload: `{"tasks":[]}`}, verdictValid},
		{"bad jobspec json", flux, cluster.Content{Kind: "jobspec", Payload: "not json"}, verdictRepairable},
	}
	for _, tc := range cases {
		if got, detail := defaultValidate(tc.target, tc.c); got != tc.want {
			t.Errorf("%s: verdict=%d want=%d (%s)", tc.name, got, tc.want, detail)
		}
	}
}

// progTransform is a programmable Transformer + repairer for wiring tests.
type progTransform struct {
	gen, fix cluster.Content
}

func (p progTransform) Transform(jobspec.Jobspec, graph.ClusterGraph) (cluster.Content, error) {
	return p.gen, nil
}
func (p progTransform) Repair(jobspec.Jobspec, graph.ClusterGraph, string, string) (cluster.Content, error) {
	return p.fix, nil
}

func newAttemptManager(tr transform.Transformer) (*Manager, *fakeMatcher) {
	m, fm := newTestManager()
	m.Trans = tr
	m.Drivers = cluster.NewRegistry(cluster.NewEmulatedDriver(graph.K8sJob, cluster.EmulatorConfig{}))
	return m, fm
}

func TestRunAttemptValidManifestSubmits(t *testing.T) {
	m, fm := newAttemptManager(progTransform{gen: manifest(goodJob)})
	seed(m, "j1", "a1", "")
	next, _ := m.handleDispatch("j1")
	j := stateOf(m, "j1")
	if next != qMonitor || j.State != queue.Running || j.RemoteHandle == "" {
		t.Fatalf("valid => next=%q state=%v handle=%q", next, j.State, j.RemoteHandle)
	}
	if j.Artifact != goodJob {
		t.Fatalf("artifact not carried")
	}
	if fm.count("a1") != 0 {
		t.Fatalf("must not free a running job")
	}
}

func TestRunAttemptWrongTargetReschedulesNoSubmit(t *testing.T) {
	m, fm := newAttemptManager(progTransform{gen: manifest(miniCl)}) // MiniCluster to a K8sJob cluster
	seed(m, "j1", "a1", "")
	next, _ := m.handleDispatch("j1")
	j := stateOf(m, "j1")
	if next != "" || j.State != queue.Submitted || j.Reschedules != 1 || fm.count("a1") != 1 {
		t.Fatalf("wrong-target => next=%q state=%v resched=%d freed=%d", next, j.State, j.Reschedules, fm.count("a1"))
	}
}

func TestRunAttemptRepairableThenRepairSubmits(t *testing.T) {
	// First generation is missing kind (repairable); repair returns a good Job.
	m, fm := newAttemptManager(progTransform{gen: manifest(noKind), fix: manifest(goodJob)})
	seed(m, "j1", "a1", "")

	next, _ := m.handleDispatch("j1")
	if next != qRepair || stateOf(m, "j1").Artifact != noKind {
		t.Fatalf("repairable => next=%q artifact carried?=%v", next, stateOf(m, "j1").Artifact == noKind)
	}
	next, _ = m.handleRepair("j1", stateOf(m, "j1").Note, 1, repairMaxAttempts)
	j := stateOf(m, "j1")
	if next != qMonitor || j.State != queue.Running || j.Artifact != goodJob || fm.count("a1") != 0 {
		t.Fatalf("repair-fix => next=%q state=%v artifact=fixed?%v freed=%d",
			next, j.State, j.Artifact == goodJob, fm.count("a1"))
	}
}

// TestAgentThroughValidatorSubmits proves the real chain: AgentTransformer calls
// (mock) Claude, the reply passes the validator, and the driver submits it.
func TestAgentThroughValidatorSubmits(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"content": []map[string]string{{"type": "text", "text": goodJob}},
		})
	}))
	defer srv.Close()

	agent := &transform.AgentTransformer{
		APIKey: "test-key", Model: "test", Endpoint: srv.URL,
		Version: "2023-06-01", MaxTokens: 512, HTTP: http.DefaultClient,
	}
	m, _ := newAttemptManager(agent)
	seed(m, "j1", "a1", "")
	next, _ := m.handleDispatch("j1")
	if j := stateOf(m, "j1"); next != qMonitor || j.State != queue.Running {
		t.Fatalf("agent chain => next=%q state=%v note=%q", next, j.State, j.Note)
	}
}
