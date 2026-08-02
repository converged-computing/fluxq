package transform_test

import (
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
	"time"

	"github.com/converged-computing/fluxq/pkg/cluster"
	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/transform"
)

func TestMiniClusterTasksComeFromTheTargetCluster(t *testing.T) {
	// A MiniCluster needs size AND tasks. The jobspec cannot supply tasks: the
	// selector picked a node count without knowing the cluster, so it cannot know
	// a node's cores. The transform must read them from the target.
	facts := []cluster.NodeFacts{
		{Arch: "amd64", Cores: 4, MemoryGB: 12},
		{Arch: "amd64", Cores: 4, MemoryGB: 12},
	}
	cg := graph.ClusterGraph{
		ID: "c1", Manager: graph.FluxOperator, Handle: "h",
		Subsystems: map[string]*graph.JGF{
			graph.ContainmentSubsystem: cluster.ContainmentFromFacts("c1", graph.FluxOperator, "h", facts),
		},
	}
	js := jobspec.New("osu", "img", []string{"osu_allreduce"}, 5, 0, time.Hour, nil)
	out, err := transform.Stub{}.Transform(js, cg)
	if err != nil {
		t.Fatal(err)
	}
	body := string(out.Payload)
	if !strings.Contains(body, "size: 5") {
		t.Errorf("want size 5:\n%s", body)
	}
	// tasks: 0 -> the operator emits `-N 5` with no `-n`, so Flux sizes the job
	// from what it actually sees. A task count computed out here is a guess.
	if !strings.Contains(body, "tasks: 0") {
		t.Errorf("want tasks 0 (let flux size it), got:\n%s", body)
	}
	if strings.Contains(body, "arch:") {
		t.Errorf("amd64 target must not request the arm flux view:\n%s", body)
	}
}

func TestMiniClusterUsesArmFluxViewOnArmTargets(t *testing.T) {
	// An arm64 cluster needs the arm flux view or the operator installs x86
	// binaries into the view.
	facts := []cluster.NodeFacts{{Arch: "arm64", Cores: 2, MemoryGB: 3}}
	subs, _, err := cluster.SubsystemsFromFacts(facts)
	if err != nil {
		t.Fatal(err)
	}
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c1", graph.FluxOperator, "h", facts)
	cg := graph.ClusterGraph{ID: "c1", Manager: graph.FluxOperator, Handle: "h", Subsystems: subs}

	js := jobspec.New("osu", "img", []string{"hostname"}, 3, 0, time.Hour, nil)
	out, err := transform.Stub{}.Transform(js, cg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out.Payload), `arch: "arm"`) {
		t.Errorf("arm64 target should request the arm flux view:\n%s", out.Payload)
	}
}

func TestMiniClusterWrapsTheCommandInFluxSecretary(t *testing.T) {
	// The operator must not add its own submit: flux-secretary owns launching,
	// because only something inside the allocation can size it.
	facts := []cluster.NodeFacts{{Arch: "amd64", Cores: 4, MemoryGB: 12}}
	subs, _, err := cluster.SubsystemsFromFacts(facts)
	if err != nil {
		t.Fatal(err)
	}
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c1", graph.FluxOperator, "h", facts)
	cg := graph.ClusterGraph{ID: "c1", Manager: graph.FluxOperator, Handle: "h", Subsystems: subs}

	js := jobspec.New("osu", "img", []string{"osu_allreduce", "-m", "8:1048576"},
		5, 0, time.Hour, nil)
	out, err := transform.Stub{}.Transform(js, cg)
	if err != nil {
		t.Fatal(err)
	}
	body := string(out.Payload)
	for _, want := range []string{
		"launcher: true",
		"tasks: 0",
		"flux python -m fluxsecretary.cli run --nodes 5 -- osu_allreduce -m 8:1048576",
		"pip install --user --quiet flux-secretary",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
	// no token registered, so no secret volume
	if strings.Contains(body, "secretName") {
		t.Errorf("a cluster with no secretary_secret must not mount one:\n%s", body)
	}
}

func TestSecretaryTokenMountedWhenRegistered(t *testing.T) {
	facts := []cluster.NodeFacts{{Arch: "amd64", Cores: 4, MemoryGB: 12}}
	subs, _, _ := cluster.SubsystemsFromFacts(facts)
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c1", graph.FluxOperator, "h", facts)
	cg := graph.ClusterGraph{ID: "c1", Manager: graph.FluxOperator, Handle: "h",
		Subsystems: subs, Config: map[string]string{"secretary_secret": "tok"}}

	out, err := transform.Stub{}.Transform(
		jobspec.New("osu", "img", []string{"hostname"}, 2, 0, time.Hour, nil), cg)
	if err != nil {
		t.Fatal(err)
	}
	body := string(out.Payload)
	// declared at the spec level and mounted under the container: both are needed
	if strings.Count(body, "secretName: tok") != 2 {
		t.Errorf("token must be declared in spec.volumes AND mounted in the container:\n%s", body)
	}
	var doc struct {
		Spec struct {
			Volumes    map[string]map[string]string `yaml:"volumes"`
			Containers []struct {
				Volumes map[string]map[string]string `yaml:"volumes"`
			} `yaml:"containers"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(out.Payload), &doc); err != nil {
		t.Fatalf("manifest is not valid yaml: %v", err)
	}
	if doc.Spec.Volumes["secretary-token"]["secretName"] != "tok" {
		t.Error("spec.volumes is missing the token")
	}
	if doc.Spec.Containers[0].Volumes["secretary-token"]["path"] != "/etc/flux-secretary" {
		t.Error("the container does not mount the token")
	}
}

// gpuJobspec is what the selector emits: slot -> node(exclusive) -> gpu.
func gpuJobspec(nodes int, gpu, efa bool) jobspec.Jobspec {
	node := jobspec.Resource{Type: "node", Count: 1, Exclusive: true}
	if gpu {
		node.With = []jobspec.Resource{{Type: "gpu", Count: 1}}
	}
	req := map[string][]jobspec.Resource{}
	if efa {
		req["network"] = []jobspec.Resource{{Type: "anyof", With: []jobspec.Resource{
			{Type: "efa"}, {Type: "ethernet"}}}}
	}
	js := jobspec.New("app", "img", []string{"run"}, nodes, 0, time.Hour, req)
	js.Resources = []jobspec.Resource{{Type: "slot", Count: nodes, Label: "default",
		With: []jobspec.Resource{node}}}
	return js
}

func deviceCluster(id, network string, gpus int) graph.ClusterGraph {
	f := []cluster.NodeFacts{{Arch: "amd64", Network: network, Cores: 8, GPUs: gpus,
		MemoryGB: 32}}
	if gpus > 0 {
		f[0].GPUVendor = "nvidia"
	}
	subs, _, _ := cluster.SubsystemsFromFacts(f)
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts(id, graph.FluxOperator, "h", f)
	return graph.ClusterGraph{ID: id, Manager: graph.FluxOperator, Handle: "h", Subsystems: subs}
}

func TestDevicesAreRequestedOrDispatchIsRefused(t *testing.T) {
	// A pod only receives a device it requests. Emitting a manifest without the
	// limit means the job runs with no GPU, or with MPI silently on TCP instead
	// of EFA, and still produces numbers. That is the failure this guards.
	for _, tc := range []struct {
		name    string
		js      jobspec.Jobspec
		target  graph.ClusterGraph
		want    string
		refused bool
	}{
		{"gpu on a 4 gpu node", gpuJobspec(2, true, false),
			deviceCluster("gpu4", "ethernet", 4), "nvidia.com/gpu: 4", false},
		{"gpu on a cpu cluster", gpuJobspec(2, true, false),
			deviceCluster("cpu", "ethernet", 0), "", true},
		{"efa on an efa cluster", gpuJobspec(2, false, true),
			deviceCluster("efa", "efa", 0), "vpc.amazonaws.com/efa: 1", false},
		{"efa on an ethernet cluster", gpuJobspec(2, false, true),
			deviceCluster("cpu", "ethernet", 0), "", true},
		{"plain job", gpuJobspec(2, false, false),
			deviceCluster("cpu", "ethernet", 0), "", false},
	} {
		out, err := transform.Stub{}.Transform(tc.js, tc.target)
		if tc.refused {
			if err == nil {
				t.Errorf("%s: must refuse rather than dispatch without the device:\n%s",
					tc.name, out.Payload)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error %v", tc.name, err)
			continue
		}
		if tc.want != "" && !strings.Contains(out.Payload, tc.want) {
			t.Errorf("%s: missing %q in:\n%s", tc.name, tc.want, out.Payload)
		}
		if tc.want == "" && strings.Contains(out.Payload, "resources:") {
			t.Errorf("%s: should not request devices:\n%s", tc.name, out.Payload)
		}
	}
}
