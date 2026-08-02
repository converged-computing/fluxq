package cluster

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	kfake "k8s.io/client-go/kubernetes/fake"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/queue"
)

// testMapper knows two kinds: a core-group Job and a custom MiniCluster CRD —
// enough to prove apply is generic (same path, no per-kind code).
func testMapper() meta.RESTMapper {
	jobGV := schema.GroupVersion{Group: "batch", Version: "v1"}
	mcGV := schema.GroupVersion{Group: "flux-framework.org", Version: "v1alpha2"}
	m := meta.NewDefaultRESTMapper([]schema.GroupVersion{jobGV, mcGV})
	m.Add(jobGV.WithKind("Job"), meta.RESTScopeNamespace)
	m.Add(mcGV.WithKind("MiniCluster"), meta.RESTScopeNamespace)
	return m
}

func fakeDriver(objs ...runtime.Object) (*K8sDriver, dynamic.Interface) {
	scheme := runtime.NewScheme()
	gvrToList := map[schema.GroupVersionResource]string{
		{Group: "batch", Version: "v1", Resource: "jobs"}:                            "JobList",
		{Group: "flux-framework.org", Version: "v1alpha2", Resource: "miniclusters"}: "MiniClusterList",
	}
	dyn := dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrToList, objs...)
	typed := kfake.NewSimpleClientset()
	d := &K8sDriver{Timeout: 5 * time.Second}
	d.connect = func(_ graph.ClusterGraph) (dynamic.Interface, kubernetes.Interface, meta.RESTMapper, error) {
		return dyn, typed, testMapper(), nil
	}
	return d, dyn
}

const jobManifest = `apiVersion: batch/v1
kind: Job
metadata:
  name: hostname
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: app
          image: busybox:latest
          command: ["hostname"]
`

const miniClusterManifest = `apiVersion: flux-framework.org/v1alpha2
kind: MiniCluster
metadata:
  name: lammps
spec:
  size: 2
`

func TestSubmitGenericManifest(t *testing.T) {
	target := graph.ClusterGraph{ID: "k8s", Manager: graph.K8sJob, Config: map[string]string{"context": "kind-fluxq"}}

	// A Job and a CRD go through the identical code path.
	for _, tc := range []struct {
		name, manifest, wantHandle string
		gvr                        schema.GroupVersionResource
	}{
		{"job", jobManifest, "jobs.batch/default/hostname",
			schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}},
		{"minicluster", miniClusterManifest, "miniclusters.flux-framework.org/default/lammps",
			schema.GroupVersionResource{Group: "flux-framework.org", Version: "v1alpha2", Resource: "miniclusters"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, dyn := fakeDriver()
			h, err := d.Submit(target, Content{Kind: "manifest", Payload: tc.manifest})
			if err != nil {
				t.Fatalf("Submit: %v", err)
			}
			if h != tc.wantHandle {
				t.Fatalf("handle = %q, want %q", h, tc.wantHandle)
			}
			// object really landed in the (fake) cluster
			_, _, _, nm := parseHandle(tc.wantHandle)
			if _, err := dyn.Resource(tc.gvr).Namespace("default").Get(context.Background(), nm, metav1.GetOptions{}); err != nil {
				t.Fatalf("object not created: %v", err)
			}
		})
	}
}

func TestStatusJob(t *testing.T) {
	target := graph.ClusterGraph{ID: "k8s", Manager: graph.K8sJob, Config: map[string]string{"context": "x"}}
	mk := func(status map[string]interface{}) runtime.Object {
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]interface{}{"name": "hostname", "namespace": "default"},
			"status":   status,
		}}
	}
	for _, tc := range []struct {
		name   string
		status map[string]interface{}
		want   queue.State
	}{
		{"succeeded", map[string]interface{}{"succeeded": int64(1)}, queue.Completed},
		{"failed", map[string]interface{}{"failed": int64(1)}, queue.Failed},
		{"active", map[string]interface{}{"active": int64(1)}, queue.Running},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := fakeDriver(mk(tc.status))
			st, _, err := d.Status(target, "jobs.batch/default/hostname")
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if st != tc.want {
				t.Fatalf("state = %v, want %v", st, tc.want)
			}
		})
	}
}

func TestStatusGenericConditions(t *testing.T) {
	target := graph.ClusterGraph{ID: "k8s", Manager: graph.K8sJob, Config: map[string]string{"context": "x"}}
	mc := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "flux-framework.org/v1alpha2", "kind": "MiniCluster",
		"metadata": map[string]interface{}{"name": "lammps", "namespace": "default"},
		"status": map[string]interface{}{"conditions": []interface{}{
			map[string]interface{}{"type": "Complete", "status": "True"},
		}},
	}}
	d, _ := fakeDriver(mc)
	st, note, err := d.Status(target, "miniclusters.flux-framework.org/default/lammps")
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st != queue.Completed {
		t.Fatalf("state = %v (%s), want Completed", st, note)
	}
}

func TestHandleRoundTrip(t *testing.T) {
	for _, h := range []string{
		"jobs.batch/default/hostname",
		"miniclusters.flux-framework.org/ns1/lammps",
		"pods/default/p1",
	} {
		res, grp, ns, name := parseHandle(h)
		if got := encodeHandle(res, grp, ns, name); got != h {
			t.Fatalf("round-trip %q -> %q", h, got)
		}
	}
}

func TestConnectRequiresConfig(t *testing.T) {
	_, _, _, err := realConnect(graph.ClusterGraph{ID: "k8s", Manager: graph.K8sJob})
	if err == nil {
		t.Fatal("expected error when neither kubeconfig nor context is set")
	}
}

func TestStatusTreatsMissingObjectAsTerminal(t *testing.T) {
	// The operator reaps finished MiniClusters and harnesses delete objects after
	// collecting logs. If Status reports an error for a vanished object, the
	// manager's status loop skips it and NEVER frees the allocation, so the
	// cluster reports no free capacity forever.
	d, _ := fakeDriver()
	st, note, err := d.Status(graph.ClusterGraph{ID: "c1"},
		"miniclusters.flux-framework.org/default/gone")
	if err != nil {
		t.Fatalf("a missing object must not be an error: %v", err)
	}
	if !st.Terminal() {
		t.Fatalf("missing object should be terminal, got %q (%s)", st, note)
	}
}
