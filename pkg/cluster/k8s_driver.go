package cluster

// K8sDriver dispatches to Kubernetes programmatically via client-go — no shelling
// out to kubectl. It applies WHATEVER manifest the transform (or agent) produced:
// the kind is read from the manifest text and resolved through a discovery-backed
// RESTMapper, then created with the dynamic client. So a batch/v1 Job, a
// flux-framework.org MiniCluster, a JobSet, or any other kind the cluster's API
// knows all go through the SAME path — there is no per-CRD backend.
//
// Connection metadata is per-cluster dispatch Config (the core stays agnostic):
//   kubeconfig=<path>   explicit kubeconfig file   -> clientcmd loading rules
//   context=<name>      a context to select        -> clientcmd overrides
//   namespace=<ns>      default namespace          -> used when a manifest omits one
// At least one of kubeconfig / context is required. kind names its context
// `kind-<clustername>`.
//
// Apply is fully generic. The only kind-specific bit is reading "is it done?"
// (a Job's .status vs a CRD's), which cannot be generic: Status uses a Job
// fast-path, then a generic .status.conditions (Complete/Failed) heuristic that
// fits most controllers, then an "applied" fallback.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/queue"
)

type K8sDriver struct {
	Timeout time.Duration
	// connect is the seam that makes this testable without a cluster: it yields
	// the dynamic client (generic apply), a typed client (pod logs) and a
	// RESTMapper (kind -> resource). Real code builds them from the kubeconfig;
	// tests inject fakes.
	connect func(target graph.ClusterGraph) (dynamic.Interface, kubernetes.Interface, meta.RESTMapper, error)
}

var _ Discoverer = (*K8sDriver)(nil)
var _ MultiTypeDriver = (*K8sDriver)(nil)

func NewK8sDriver() *K8sDriver {
	d := &K8sDriver{Timeout: 30 * time.Second}
	d.connect = realConnect
	return d
}

func (d *K8sDriver) Type() graph.ManagerType { return graph.K8sJob }

// Types reports every manager this driver serves. Dispatch is manifest-kind
// agnostic, so one driver covers both a batch/v1 Job and a MiniCluster.
func (d *K8sDriver) Types() []graph.ManagerType {
	return []graph.ManagerType{graph.K8sJob, graph.FluxOperator}
}

func (d *K8sDriver) timeout() time.Duration {
	if d.Timeout <= 0 {
		return 30 * time.Second
	}
	return d.Timeout
}

func (d *K8sDriver) namespace(target graph.ClusterGraph) string {
	if ns := target.Cfg("namespace"); ns != "" {
		return ns
	}
	return "default"
}

// realConnect builds the clients from the cluster's kubeconfig/context config.
func realConnect(target graph.ClusterGraph) (dynamic.Interface, kubernetes.Interface, meta.RESTMapper, error) {
	kubeconfig, kctx := target.Cfg("kubeconfig"), target.Cfg("context")
	if kubeconfig == "" && kctx == "" {
		return nil, nil, nil, fmt.Errorf("k8s cluster %q needs dispatch config: kubeconfig=<path> or context=<name>", target.ID)
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if kctx != "" {
		overrides.CurrentContext = kctx
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("kubeconfig for %q: %w", target.ID, err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	typed, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	grs, err := restmapper.GetAPIGroupResources(dc)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("discover cluster resources for %q: %w", target.ID, err)
	}
	return dyn, typed, restmapper.NewDiscoveryRESTMapper(grs), nil
}

// decodeManifests splits a (possibly multi-document) YAML/JSON manifest into
// unstructured objects.
func decodeManifests(payload string) ([]*unstructured.Unstructured, error) {
	dec := yaml.NewYAMLOrJSONDecoder(strings.NewReader(payload), 4096)
	var out []*unstructured.Unstructured
	for {
		m := map[string]interface{}{}
		if err := dec.Decode(&m); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("decode manifest: %w", err)
		}
		if len(m) == 0 {
			continue
		}
		out = append(out, &unstructured.Unstructured{Object: m})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("manifest contained no objects")
	}
	return out, nil
}

func (d *K8sDriver) Submit(target graph.ClusterGraph, c Content) (string, error) {
	if c.Kind != "manifest" {
		return "", fmt.Errorf("k8s expects a manifest, got %q", c.Kind)
	}
	objs, err := decodeManifests(c.Payload)
	if err != nil {
		return "", err
	}
	dyn, _, mapper, err := d.connect(target)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()

	var handle string
	for i, obj := range objs {
		gvk := obj.GroupVersionKind()
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return "", fmt.Errorf("unknown kind %s (is its CRD installed?): %w", gvk, err)
		}
		ri, ns := d.resourceFor(dyn, mapping, obj.GetNamespace(), target)
		if ns != "" {
			obj.SetNamespace(ns)
		}
		created, err := ri.Create(ctx, obj, metav1.CreateOptions{FieldManager: "fluxq"})
		if err != nil {
			return "", fmt.Errorf("apply %s: %w", gvk.Kind, err)
		}
		if i == 0 {
			handle = encodeHandle(mapping.Resource.Resource, mapping.Resource.Group, ns, created.GetName())
		}
	}
	return handle, nil
}

// resourceFor returns the dynamic client scoped correctly (namespaced or not)
// and the namespace it will use.
func (d *K8sDriver) resourceFor(dyn dynamic.Interface, mapping *meta.RESTMapping, objNS string, target graph.ClusterGraph) (dynamic.ResourceInterface, string) {
	if mapping.Scope.Name() != meta.RESTScopeNameNamespace {
		return dyn.Resource(mapping.Resource), ""
	}
	ns := objNS
	if ns == "" {
		ns = d.namespace(target)
	}
	return dyn.Resource(mapping.Resource).Namespace(ns), ns
}

func (d *K8sDriver) Status(target graph.ClusterGraph, handle string) (queue.State, string, error) {
	res, group, ns, name := parseHandle(handle)
	dyn, _, mapper, err := d.connect(target)
	if err != nil {
		return "", "", err
	}
	gvr, err := mapper.ResourceFor(schema.GroupVersionResource{Group: group, Resource: res})
	if err != nil {
		return "", "", fmt.Errorf("resolve %s.%s: %w", res, group, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()
	var ri dynamic.ResourceInterface = dyn.Resource(gvr)
	if ns != "" {
		ri = dyn.Resource(gvr).Namespace(ns)
	}
	u, err := ri.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		// A vanished object is terminal, not an error: reporting one here would
		// leave the allocation held forever.
		if apierrors.IsNotFound(err) {
			return queue.Completed, res + " no longer present (reaped or deleted)", nil
		}
		return "", "", fmt.Errorf("get %s: %w", handle, err)
	}
	st, note := readState(u)
	if st.Terminal() {
		return st, note, nil
	}
	// A MiniCluster does not report completion itself, so ask the Job it owns.
	if s, n, ok := ownedJobState(d, target, ns, u.GetUID()); ok {
		return s, n, nil
	}
	return st, note, nil
}

// readState maps an object's status onto our lifecycle. Job is exact; other
// kinds fall back to a conditions heuristic, then "applied".
func readState(u *unstructured.Unstructured) (queue.State, string) {
	if u.GetKind() == "Job" {
		s, _, _ := unstructured.NestedInt64(u.Object, "status", "succeeded")
		f, _, _ := unstructured.NestedInt64(u.Object, "status", "failed")
		a, _, _ := unstructured.NestedInt64(u.Object, "status", "active")
		switch {
		case f > 0:
			return queue.Failed, fmt.Sprintf("k8s job failed (%d)", f)
		case s > 0:
			return queue.Completed, "k8s job succeeded"
		case a > 0:
			return queue.Running, fmt.Sprintf("k8s job active (%d)", a)
		default:
			return queue.Running, "k8s job pending"
		}
	}
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if s, _ := m["status"].(string); s != "True" {
			continue
		}
		switch t, _ := m["type"].(string); t {
		case "Failed":
			return queue.Failed, u.GetKind() + " condition Failed"
		case "Complete", "Completed", "Succeeded":
			return queue.Completed, u.GetKind() + " condition " + t
		}
	}
	return queue.Running, u.GetKind() + " applied"
}

func (d *K8sDriver) Cancel(target graph.ClusterGraph, handle string) error {
	res, group, ns, name := parseHandle(handle)
	dyn, _, mapper, err := d.connect(target)
	if err != nil {
		return err
	}
	gvr, err := mapper.ResourceFor(schema.GroupVersionResource{Group: group, Resource: res})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()
	bg := metav1.DeletePropagationBackground
	var ri dynamic.ResourceInterface = dyn.Resource(gvr)
	if ns != "" {
		ri = dyn.Resource(gvr).Namespace(ns)
	}
	return ri.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &bg})
}

func (d *K8sDriver) Logs(target graph.ClusterGraph, handle string) (string, error) {
	res, _, ns, name := parseHandle(handle)
	// Logs are pod-scoped; today we resolve them for Jobs (pods carry the
	// job-name label). Other kinds would need their own pod selector.
	if res != "jobs" {
		return "", nil
	}
	_, typed, _, err := d.connect(target)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pods, err := typed.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + name})
	if err != nil {
		return "", err
	}
	if len(pods.Items) == 0 {
		return "", nil
	}
	tail := int64(200)
	rc, err := typed.CoreV1().Pods(ns).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{TailLines: &tail}).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, rc)
	return buf.String(), nil
}

// handle format: "<resource>.<group>/<namespace>/<name>" (group omitted for the
// core group; namespace omitted for cluster-scoped). Examples:
//
//	jobs.batch/default/hostname
//	miniclusters.flux-framework.org/default/lammps
func encodeHandle(resource, group, ns, name string) string {
	rg := resource
	if group != "" {
		rg = resource + "." + group
	}
	if ns == "" {
		return rg + "/" + name
	}
	return rg + "/" + ns + "/" + name
}

func parseHandle(h string) (resource, group, ns, name string) {
	parts := strings.Split(h, "/")
	rg := parts[0]
	if dot := strings.Index(rg, "."); dot >= 0 {
		resource, group = rg[:dot], rg[dot+1:]
	} else {
		resource = rg
	}
	switch len(parts) {
	case 3:
		ns, name = parts[1], parts[2]
	case 2:
		name = parts[1]
	}
	return resource, group, ns, name
}

// Discover implements Discoverer for Kubernetes: list nodes through the SAME
// connect seam dispatch uses, and read backend-neutral facts off each.
func (d *K8sDriver) Discover(target graph.ClusterGraph) ([]NodeFacts, error) {
	_, typed, _, err := d.connect(target)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()
	nl, err := typed.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list nodes for %q: %w", target.ID, err)
	}
	facts := make([]NodeFacts, 0, len(nl.Items))
	for _, n := range nl.Items {
		facts = append(facts, factsFromNode(n))
	}
	return facts, nil
}

// factsFromNode maps a k8s node's labels + allocatable onto backend-neutral
// NodeFacts. This is the only k8s-specific part of discovery.
func factsFromNode(n corev1.Node) NodeFacts {
	lbl := n.Labels
	f := NodeFacts{Arch: lbl["kubernetes.io/arch"]}
	switch {
	case lbl["nvidia.com/gpu.product"] != "" || nodeHasResource(n, "nvidia.com/gpu"):
		f.GPUVendor = "nvidia"
	case lbl["amd.com/gpu.device-id"] != "" || nodeHasResource(n, "amd.com/gpu"):
		f.GPUVendor = "amd"
	}
	if nodeHasResource(n, "vpc.amazonaws.com/efa") {
		f.Network = "efa"
	} else {
		f.Network = "ethernet"
	}
	if mem, ok := n.Status.Allocatable[corev1.ResourceMemory]; ok {
		f.MemoryGB = int(mem.Value() / (1 << 30))
	}
	if cpu, ok := n.Status.Allocatable[corev1.ResourceCPU]; ok {
		f.Cores = int(cpu.Value())
	}
	for _, r := range []corev1.ResourceName{"nvidia.com/gpu", "amd.com/gpu"} {
		if q, ok := n.Status.Allocatable[r]; ok && q.Value() > 0 {
			f.GPUs = int(q.Value())
		}
	}
	return f
}

func nodeHasResource(n corev1.Node, name corev1.ResourceName) bool {
	if q, ok := n.Status.Allocatable[name]; ok {
		return q.Value() > 0
	}
	return false
}

// ownedJobState reads completion from a batch/v1 Job owned by uid. A MiniCluster
// does not report completion itself.
func ownedJobState(d *K8sDriver, target graph.ClusterGraph, ns string,
	uid types.UID) (queue.State, string, bool) {
	if ns == "" {
		ns = d.namespace(target)
	}
	_, typed, _, err := d.connect(target)
	if err != nil || typed == nil {
		return "", "", false
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.timeout())
	defer cancel()
	jobs, err := typed.BatchV1().Jobs(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", "", false
	}
	for i := range jobs.Items {
		j := &jobs.Items[i]
		owned := false
		for _, o := range j.GetOwnerReferences() {
			if o.UID == uid {
				owned = true
				break
			}
		}
		if !owned {
			continue
		}
		if j.Status.Succeeded > 0 {
			return queue.Completed, fmt.Sprintf("job %s succeeded", j.Name), true
		}
		if j.Status.Failed > 0 {
			return queue.Failed, fmt.Sprintf("job %s failed", j.Name), true
		}
	}
	return "", "", false
}
