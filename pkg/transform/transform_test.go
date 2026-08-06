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

// Fields at the wrong level are PRUNED by CRD validation without any error, so
// the manifest applies and quietly does the wrong thing. v1alpha2 puts launcher
// and volumes on the CONTAINER, and has no spec.volumes at all.
func TestMiniClusterFieldsAreAtTheLevelsTheCRDDefines(t *testing.T) {
	facts := []cluster.NodeFacts{{Arch: "amd64", Network: "ethernet", Cores: 4, MemoryGB: 12}}
	subs, _, _ := cluster.SubsystemsFromFacts(facts)
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c1", graph.FluxOperator, "h", facts)
	cg := graph.ClusterGraph{ID: "c1", Manager: graph.FluxOperator, Handle: "h",
		Subsystems: subs, Config: map[string]string{"secretary_secret": "tok"}}

	out, err := transform.Stub{}.Transform(
		jobspec.New("osu", "img", []string{"hostname"}, 2, 0, time.Hour, nil), cg)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Spec struct {
			Launcher   *bool                        `yaml:"launcher"`
			Volumes    map[string]map[string]string `yaml:"volumes"`
			Containers []struct {
				Launcher *bool                        `yaml:"launcher"`
				Volumes  map[string]map[string]string `yaml:"volumes"`
			} `yaml:"containers"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal([]byte(out.Payload), &doc); err != nil {
		t.Fatalf("not valid yaml: %v", err)
	}
	if doc.Spec.Launcher != nil {
		t.Error("launcher must NOT be at spec level: the CRD prunes it there")
	}
	if doc.Spec.Volumes != nil {
		t.Error("v1alpha2 has no spec.volumes: it would be pruned")
	}
	c := doc.Spec.Containers[0]
	if c.Launcher == nil || !*c.Launcher {
		t.Error("launcher must be on the container, or the operator wraps the " +
			"command in its own flux submit")
	}
	if c.Volumes["secretary-token"]["secretName"] != "tok" {
		t.Errorf("token must be mounted on the container: %+v", c.Volumes)
	}
}

// The view image ships flux-secretary, so there is nothing to install and the
// command just calls it.
// The install uses the view's own interpreter, looked up from the view that was
// chosen: each view ships one python3.N and no python3.
func TestInstallUsesTheViewInterpreter(t *testing.T) {
	facts := []cluster.NodeFacts{{Arch: "amd64", Cores: 4, MemoryGB: 12}}
	subs, _, _ := cluster.SubsystemsFromFacts(facts)
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c", graph.FluxOperator, "h", facts)
	cg := graph.ClusterGraph{ID: "c", Manager: graph.FluxOperator, Handle: "h", Subsystems: subs}

	out, err := transform.Stub{}.Transform(
		jobspec.New("app", "img", []string{"run"}, 2, 0, time.Hour, nil), cg)
	if err != nil {
		t.Fatal(err)
	}
	body := string(out.Payload)
	// unknown glibc takes the jammy view, which ships python3.11
	if !strings.Contains(body, "/mnt/flux/view/bin/python3.14 -m pip install --no-cache-dir flux-secretary[all]") {
		t.Errorf("want the view's interpreter installing:\n%s", body)
	}
	if !strings.Contains(body, "flux-secretary run --nodes 2 --attempts") {
		t.Errorf("want the console script:\n%s", body)
	}
	if strings.Contains(body, "$(") || strings.Contains(body, "command -v") {
		t.Errorf("nothing should be derived at run time:\n%s", body)
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
		"flux-secretary run --nodes 5 --attempts 10 --model us.anthropic.claude-opus-5 -- osu_allreduce -m 8:1048576",
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
	// Exactly once, on the container: v1alpha2 has no spec.volumes, and a block
	// placed there is pruned without error.
	if n := strings.Count(string(out.Payload), "secretName: tok"); n != 1 {
		t.Errorf("want the token mounted once on the container, found %d:\n%s",
			n, out.Payload)
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
		// anyof[efa,ethernet]: the matcher accepted this cluster, so run on what
		// it has rather than refusing. Unconditional efa is covered separately.
		{"anyof fabric on an ethernet cluster", gpuJobspec(2, false, true),
			deviceCluster("cpu", "ethernet", 0), "", false},
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

func TestObjectNameIsUniquePerJob(t *testing.T) {
	// The base and subsystem runs of one jobspec must not produce the same
	// object. A reused name makes the second apply fail as already existing, or
	// land on the completed remains of the first, so the second condition never
	// really runs and the comparison is meaningless.
	facts := []cluster.NodeFacts{{Arch: "amd64", Cores: 4, MemoryGB: 12}}
	subs, _, _ := cluster.SubsystemsFromFacts(facts)
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c1", graph.FluxOperator, "h", facts)
	cg := graph.ClusterGraph{ID: "c1", Manager: graph.FluxOperator, Handle: "h", Subsystems: subs}
	base := jobspec.New("metric-osu-cpu", "img", []string{"run"}, 2, 0, time.Hour, nil)

	seen := map[string]bool{}
	for _, id := range []string{"job-0001", "job-0002"} {
		out, err := transform.Stub{}.Transform(base.WithJobID(id), cg)
		if err != nil {
			t.Fatal(err)
		}
		want := "name: metric-osu-cpu-" + id
		if !strings.Contains(string(out.Payload), want) {
			t.Fatalf("expected %q in:\n%s", want, out.Payload)
		}
		seen[want] = true
	}
	if len(seen) != 2 {
		t.Error("two jobs produced the same object name")
	}
}

func TestFluxViewCarriesTheSecretary(t *testing.T) {
	// The secretary runs against the view's Python bindings, so it has to be in
	// the view. Per job pip installs resolved against whatever interpreter the
	// view happened to provide and left jobs silently without an agent.
	mk := func(arch string, cfg map[string]string) graph.ClusterGraph {
		facts := []cluster.NodeFacts{{Arch: arch, Network: "ethernet", Cores: 4, MemoryGB: 12}}
		subs, _, _ := cluster.SubsystemsFromFacts(facts)
		subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c", graph.FluxOperator, "h", facts)
		return graph.ClusterGraph{ID: "c", Manager: graph.FluxOperator, Handle: "h",
			Subsystems: subs, Config: cfg}
	}
	js := jobspec.New("osu", "img", []string{"run"}, 2, 0, time.Hour, nil)

	for _, tc := range []struct {
		name, arch string
		cfg        map[string]string
		wantImage  string
		wantArm    bool
	}{
		{"amd64 default", "amd64", nil, "flux-view-ubuntu:jammy", false},
		{"arm64 default", "arm64", nil, "flux-view-ubuntu:jammy-arm", true},
		{"explicit view", "amd64", map[string]string{"secretary_view": "ghcr.io/me/v:1"},
			"ghcr.io/me/v:1", false},
	} {
		out, err := transform.Stub{}.Transform(js, mk(tc.arch, tc.cfg))
		if err != nil {
			t.Fatal(err)
		}
		body := string(out.Payload)
		if !strings.Contains(body, tc.wantImage) {
			t.Errorf("%s: want view %q in:\n%s", tc.name, tc.wantImage, body)
		}
		if got := strings.Contains(body, `arch: "arm"`); got != tc.wantArm {
			t.Errorf("%s: arm view %v, want %v", tc.name, got, tc.wantArm)
		}
	}

	// opting out leaves the operator's own view alone
	out, err := transform.Stub{}.Transform(js, mk("amd64", map[string]string{"secretary_view": "none"}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out.Payload), "flux:") {
		t.Errorf("secretary_view=none should emit no flux block:\n%s", out.Payload)
	}
}

func TestSecretaryPythonOverridesTheInstallInterpreter(t *testing.T) {
	facts := []cluster.NodeFacts{{Arch: "amd64", Cores: 4, MemoryGB: 12}}
	subs, _, _ := cluster.SubsystemsFromFacts(facts)
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c1", graph.FluxOperator, "h", facts)
	js := jobspec.New("osu", "img", []string{"run"}, 2, 0, time.Hour, nil)
	cg := graph.ClusterGraph{ID: "c1", Manager: graph.FluxOperator, Handle: "h",
		Subsystems: subs, Config: map[string]string{"secretary_package": "flux-secretary[all]", "secretary_python": "python3.12"}}

	out, err := transform.Stub{}.Transform(js, cg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out.Payload), "python3.12 -m pip install") {
		t.Errorf("secretary_python should choose the install interpreter:\n%s", out.Payload)
	}
}

func TestSecretaryModelIsNamed(t *testing.T) {
	facts := []cluster.NodeFacts{{Arch: "amd64", Cores: 4, MemoryGB: 12}}
	subs, _, _ := cluster.SubsystemsFromFacts(facts)
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c1", graph.FluxOperator, "h", facts)
	js := jobspec.New("osu", "img", []string{"run"}, 2, 0, time.Hour, nil)
	base := graph.ClusterGraph{ID: "c1", Manager: graph.FluxOperator, Handle: "h", Subsystems: subs}

	out, err := transform.Stub{}.Transform(js, base)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out.Payload), "--model us.anthropic.claude-opus-5") {
		t.Errorf("want the model named:\n%s", out.Payload)
	}

	pinned := base
	pinned.Config = map[string]string{"secretary_model": "us.anthropic.claude-sonnet-5"}
	out, err = transform.Stub{}.Transform(js, pinned)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out.Payload), "--model us.anthropic.claude-sonnet-5") {
		t.Errorf("secretary_model should override:\n%s", out.Payload)
	}

	off := base
	off.Config = map[string]string{"secretary_model": "none"}
	out, err = transform.Stub{}.Transform(js, off)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out.Payload), "--model") {
		t.Errorf("secretary_model=none should pass no model:\n%s", out.Payload)
	}
}

func TestUnknownGlibcTakesTheJammyView(t *testing.T) {
	// glibc is backward compatible: a view built against 2.35 loads in a 2.39
	// container, while the reverse aborts before flux starts. With no recorded
	// libc the conservative choice is the one that runs anywhere.
	facts := []cluster.NodeFacts{{Arch: "amd64", Cores: 4, MemoryGB: 12}}
	subs, _, _ := cluster.SubsystemsFromFacts(facts)
	subs[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c", graph.FluxOperator, "h", facts)
	cg := graph.ClusterGraph{ID: "c", Manager: graph.FluxOperator, Handle: "h", Subsystems: subs}

	out, err := transform.Stub{}.Transform(
		jobspec.New("osu", "img", []string{"run"}, 2, 0, time.Hour, nil), cg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out.Payload), "flux-view-ubuntu:jammy") {
		t.Errorf("an unrecorded libc should take the jammy view:\n%s", out.Payload)
	}
}

func TestTransformDoesNotSecondGuessTheMatcher(t *testing.T) {
	// Whether a cluster satisfies requires is Fluxion's answer, given before the
	// transform runs. Re-deciding it here is a second matcher, and when the two
	// disagree the transform wins by refusing a job the scheduler accepted.
	mk := func(net string) graph.ClusterGraph {
		f := []cluster.NodeFacts{{Arch: "amd64", Network: net, Cores: 4, MemoryGB: 12}}
		s, _, _ := cluster.SubsystemsFromFacts(f)
		s[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c", graph.FluxOperator, "h", f)
		return graph.ClusterGraph{ID: "c-" + net, Manager: graph.FluxOperator, Handle: "h", Subsystems: s}
	}
	spec := func(req []jobspec.Resource) jobspec.Jobspec {
		return jobspec.New("osu", "img", []string{"run"}, 2, 0, time.Hour,
			map[string][]jobspec.Resource{"network": req})
	}
	anyof := []jobspec.Resource{{Type: "anyof", With: []jobspec.Resource{
		{Type: "efa"}, {Type: "ethernet"}}}}
	only := []jobspec.Resource{{Type: "efa"}}

	// the matcher accepted this cluster, so run on what it has
	for _, req := range [][]jobspec.Resource{anyof, only} {
		out, err := transform.Stub{}.Transform(spec(req), mk("ethernet"))
		if err != nil {
			t.Fatalf("the transform must not re-judge the placement: %v", err)
		}
		if strings.Contains(string(out.Payload), "vpc.amazonaws.com/efa") {
			t.Errorf("no efa here to claim:\n%s", out.Payload)
		}
	}

	// the node is held exclusively, so its fabric is claimed where it exists
	for _, req := range [][]jobspec.Resource{anyof, only} {
		out, err := transform.Stub{}.Transform(spec(req), mk("efa"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(out.Payload), "vpc.amazonaws.com/efa: 1") {
			t.Errorf("efa present must be claimed or MPI falls back to TCP:\n%s", out.Payload)
		}
	}
}

// flux R encode is emitted only when the cluster has GPUs, is best effort, and
// goes where the broker reads it. A CPU cluster gets no pre block at all.

func TestRCountsCoresInTheContainerAndAddsGpus(t *testing.T) {
	// Kubernetes allocatable.cpu counts hardware threads; a resource set counts
	// what the container can schedule on. Declaring the node's 8 for a g5.2xlarge
	// drained rank 0 ("missing resources: core[4-7]") and no allocation could then
	// be satisfied. sched_getaffinity respects the cpuset, so the count is taken
	// there. Only the device count comes from us, because flux cannot see it.
	mk := func(cores, gpus int) graph.ClusterGraph {
		f := []cluster.NodeFacts{{Arch: "amd64", Network: "ethernet",
			Cores: cores, GPUs: gpus, MemoryGB: 29}}
		if gpus > 0 {
			f[0].GPUVendor = "nvidia"
		}
		sub, _, _ := cluster.SubsystemsFromFacts(f)
		sub[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c", graph.FluxOperator, "h", f)
		return graph.ClusterGraph{ID: "c", Manager: graph.FluxOperator, Handle: "h", Subsystems: sub}
	}
	js := jobspec.New("app", "img", []string{"run"}, 1, 0, time.Hour, nil)
	path := "/mnt/flux/config/etc/flux/system/R"

	out, err := transform.Stub{}.Transform(js, mk(8, 4))
	if err != nil {
		t.Fatal(err)
	}
	body := string(out.Payload)

	// cores are counted in the container, never declared from node facts
	if !strings.Contains(body, "sched_getaffinity") {
		t.Errorf("cores must be counted in the container:\n%s", body)
	}
	for _, wrong := range []string{"--cores=0-7", "--cores=0-3", "--cores=0-31"} {
		if strings.Contains(body, wrong) {
			t.Errorf("a declared core count drains the rank: %q\n%s", wrong, body)
		}
	}
	// one id must be bare, since 0-0 produces no children
	if !strings.Contains(body, `if n>1 else '0'`) {
		t.Errorf("a single core must be '0', not '0-0':\n%s", body)
	}
	// the device count is ours
	if !strings.Contains(body, "--gpus=0-3") {
		t.Errorf("four gpus is 0-3:\n%s", body)
	}
	if !strings.Contains(body, "cat "+path) {
		t.Errorf("the resulting resource set must be printed:\n%s", body)
	}
	if strings.Contains(body, "||") {
		t.Errorf("no fallback: a broker that cannot see the device must fail:\n%s", body)
	}

	// one gpu is a bare index too
	out, err = transform.Stub{}.Transform(js, mk(8, 1))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out.Payload), "--gpus=0 ") {
		t.Errorf("one gpu is '0', not '0-0':\n%s", out.Payload)
	}

	// and a cpu cluster leaves the operator's resource set alone
	out, err = transform.Stub{}.Transform(js, mk(4, 0))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(out.Payload), "flux R encode") {
		t.Errorf("no gpus, so nothing to regenerate:\n%s", out.Payload)
	}
}

func TestViewIsPulledAndPipIsBootstrapped(t *testing.T) {
	// Three things the working manifest established.
	//
	// The rebuilt view is republished under the same tag, so a node holding an
	// older layer would run a different interpreter than the one pre names.
	//
	// The view ships an interpreter but no pip, so ensurepip has to come first, or
	// the install fails with "No module named pip" and the job then dies with
	// "flux-secretary: not found", which reads as a launch problem and is not one.
	//
	// And the attempt count is stated, so the manifest records what was allowed.
	mk := func(gpus int) graph.ClusterGraph {
		f := []cluster.NodeFacts{{Arch: "amd64", Network: "ethernet", Cores: 8,
			GPUs: gpus, MemoryGB: 29}}
		if gpus > 0 {
			f[0].GPUVendor = "nvidia"
		}
		sub, _, _ := cluster.SubsystemsFromFacts(f)
		sub[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts("c", graph.FluxOperator, "h", f)
		return graph.ClusterGraph{ID: "c", Manager: graph.FluxOperator, Handle: "h", Subsystems: sub}
	}
	js := jobspec.New("app", "img", []string{"run"}, 4, 0, time.Hour, nil)

	out, err := transform.Stub{}.Transform(js, mk(0))
	if err != nil {
		t.Fatal(err)
	}
	body := string(out.Payload)
	for _, want := range []string{
		"image: ghcr.io/converged-computing/flux-view-ubuntu:jammy",
		"pullAlways: true",
		"/mnt/flux/view/bin/python3.14 -m ensurepip",
		"/mnt/flux/view/bin/python3.14 -m pip install --no-cache-dir flux-secretary[all]",
		"flux-secretary run --nodes 4 --attempts 10 --model",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q:\n%s", want, body)
		}
	}
	if strings.Index(body, "ensurepip") > strings.Index(body, "pip install") {
		t.Errorf("ensurepip must come first:\n%s", body)
	}
	if strings.Contains(body, "flux R encode") {
		t.Errorf("no gpus here, so leave the resource set alone:\n%s", body)
	}

	// with gpus the install still comes first, then R
	out, err = transform.Stub{}.Transform(js, mk(4))
	if err != nil {
		t.Fatal(err)
	}
	body = string(out.Payload)
	if !strings.Contains(body, "flux R encode --hosts=${hosts}") ||
		!strings.Contains(body, "--gpus=0-3") {
		t.Errorf("gpu cluster wants its resource set regenerated:\n%s", body)
	}
	if strings.Index(body, "flux R encode") < strings.Index(body, "pip install") {
		t.Errorf("the install must come first:\n%s", body)
	}
}

func TestArmClustersGetAnArmView(t *testing.T) {
	// The arm row pointed at :jammy, which is amd64 only. The operator mounted it
	// on an arm cluster, copied the view into place, and the container then died
	// with "exec /bin/bash: exec format error" — in both arms, in every replicate,
	// for both arm64 applications. That reads as an application failure and is not
	// one, so the pairing is asserted rather than left to the table.
	mk := func(arch string) graph.ClusterGraph {
		f := []cluster.NodeFacts{{Arch: arch, Network: "ethernet", Cores: 8,
			MemoryGB: 29}}
		sub, _, _ := cluster.SubsystemsFromFacts(f)
		sub[graph.ContainmentSubsystem] = cluster.ContainmentFromFacts(
			"c", graph.FluxOperator, "h", f)
		return graph.ClusterGraph{ID: "c", Manager: graph.FluxOperator, Handle: "h",
			Subsystems: sub}
	}
	js := jobspec.New("app", "img", []string{"run"}, 2, 0, time.Hour, nil)

	for _, tc := range []struct {
		arch, wantView, wantPython string
		wantArmFlag                bool
	}{
		{"amd64", "flux-view-ubuntu:jammy", "python3.14", false},
		{"arm64", "flux-view-ubuntu:jammy-arm", "python3.14", true},
	} {
		out, err := transform.Stub{}.Transform(js, mk(tc.arch))
		if err != nil {
			t.Fatal(err)
		}
		body := string(out.Payload)
		if !strings.Contains(body, "image: ghcr.io/converged-computing/"+tc.wantView) {
			t.Errorf("%s: want %s:\n%s", tc.arch, tc.wantView, body)
		}
		// the amd64 image name is a prefix of nothing else, but be explicit: an
		// arm cluster must never be handed the amd64 view
		if tc.arch == "arm64" &&
			strings.Contains(body, "flux-view-ubuntu:jammy\n") {
			t.Errorf("arm64 cluster was given the amd64 view:\n%s", body)
		}
		if !strings.Contains(body, "/view/bin/"+tc.wantPython+" -m ensurepip") {
			t.Errorf("%s: want %s installing:\n%s", tc.arch, tc.wantPython, body)
		}
		if got := strings.Contains(body, `arch: "arm"`); got != tc.wantArmFlag {
			t.Errorf("%s: arch arm flag %v, want %v", tc.arch, got, tc.wantArmFlag)
		}
	}
}
