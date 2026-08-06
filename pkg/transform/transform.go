// Package transform is the AGENT seam: given a jobspec and the cluster it was
// assigned to, produce the manager-native artifact (a k8s manifest or a flux
// command). Transform and dispatch are one logical step — the agent emits the
// right thing for the right manager, then the driver submits it — so this
// package produces cluster.Content that goes straight to a driver.
//
// The default Transformer here is DETERMINISTIC (template-based) so the whole
// pipeline runs and is testable without an LLM. Swap in an AgentTransformer
// that calls a model with BuildPrompt(); everything downstream is unchanged.
// This is exactly where the dispatch paper's failure modes (flag
// concatenation, wrong resource counts) will resurface across paradigms, so it
// is the natural place to add a validation/checker pass.
package transform

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/converged-computing/fluxq/pkg/cluster"
	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/jobspec"
)

// Transformer compiles agnostic intent into a native artifact for a target.
type Transformer interface {
	Transform(js jobspec.Jobspec, target graph.ClusterGraph) (cluster.Content, error)
}

// BuildPrompt composes the instruction an AgentTransformer would send to a
// model — the design's "this job needs to be sent to this cluster" prompt,
// carrying the agnostic intent and the target's manager so the agent knows
// which dialect to emit. Kept here so deterministic and agent transformers
// share one definition of the task.
func BuildPrompt(js jobspec.Jobspec, target graph.ClusterGraph) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Transform this job for cluster %q (manager: %s).\n", target.ID, target.Manager)
	fmt.Fprintf(&b, "Preserve the application's needs exactly; do not change the problem size.\n")
	fmt.Fprintf(&b, "Container image: %s\n", js.Image())
	fmt.Fprintf(&b, "Command: %s\n", strings.Join(js.Command(), " "))
	fmt.Fprintf(&b, "Resources: %d nodes x %d cores/node x %d tasks total",
		js.Nodes(), js.CoresPerNode(), js.TasksTotal())
	if js.GPUsPerNode() > 0 {
		fmt.Fprintf(&b, ", %d gpus/node", js.GPUsPerNode())
	}
	b.WriteString("\n")
	switch target.Manager {
	case graph.K8sJob:
		b.WriteString("Emit a Kubernetes batch/v1 Job manifest.\n")
	case graph.FluxOperator:
		b.WriteString("Emit a Flux Operator MiniCluster manifest.\n")
	case graph.SlurmOperator:
		b.WriteString("Emit a Slurm job for the slurm-operator.\n")
	case graph.FluxURI:
		b.WriteString("Emit an RFC 25 jobspec (JSON) — flux is jobspec-native.\n")
	}
	return b.String()
}

// Stub is the deterministic transformer. It is intentionally simple but
// produces the correct KIND per manager so drivers accept it.
type Stub struct{}

func (Stub) Transform(js jobspec.Jobspec, target graph.ClusterGraph) (cluster.Content, error) {
	switch target.Manager {
	case graph.FluxURI:
		// Flux is jobspec-native: the transform is (near) identity — hand the
		// rendered RFC jobspec straight to the driver, which submits it via
		// flux_job_submit. No shell command to mangle.
		spec, err := js.ToFluxSpec()
		if err != nil {
			return cluster.Content{}, err
		}
		return cluster.Content{Kind: "jobspec", Payload: spec}, nil
	case graph.K8sJob:
		return cluster.Content{Kind: "manifest", Payload: k8sJob(js, target)}, nil
	case graph.FluxOperator:
		body, err := miniCluster(js, target)
		if err != nil {
			return cluster.Content{}, err
		}
		return cluster.Content{Kind: "manifest", Payload: body}, nil
	case graph.SlurmOperator:
		return cluster.Content{Kind: "manifest", Payload: slurmJob(js, target)}, nil
	default:
		return cluster.Content{}, fmt.Errorf("no transform for manager %q", target.Manager)
	}
}

func k8sJob(js jobspec.Jobspec, _ graph.ClusterGraph) string {
	return fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %s
spec:
  completions: %d
  parallelism: %d
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: app
          image: %s
          command: [%s]
`, js.Name(), js.Nodes(), js.Nodes(), js.Image(), quoteList(js.Command()))
}

func miniCluster(js jobspec.Jobspec, target graph.ClusterGraph) (string, error) {
	// launcher and tasks: 0 stop the operator adding its own flux submit and -n;
	// the secretary sizes the launch from inside the allocation. launcher is a
	// container field, and at spec level it is silently pruned.
	devices, err := deviceResources(js, target)
	if err != nil {
		return "", err
	}
	view, err := secretaryView(js, target, isArm(target))
	if err != nil {
		return "", err
	}
	flux, err := fluxBlock(js, target)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`apiVersion: flux-framework.org/v1alpha2
kind: MiniCluster
metadata:
  name: %s
spec:
  size: %d
  tasks: 0
%s  containers:
    - image: %s
      launcher: true
      command: %q
%s%s%s`, objectName(js), js.Nodes(), flux, js.Image(),
		secretaryCommand(js, target), preBlock(append(secretaryInstall(target, view), fluxResources(target)...)...), devices,
		secretaryVolume(target)), nil
}

// deviceResources renders container resource limits for the GPU or fabric the
// job requires, and errors if the cluster cannot provide one.
func deviceResources(js jobspec.Jobspec, target graph.ClusterGraph) (string, error) {
	limits := []string{}

	if js.GPUsPerNode() > 0 {
		count := target.GPUsPerNode()
		if count < 1 {
			return "", fmt.Errorf(
				"job requires a GPU but cluster %q advertises none: dispatching "+
					"would run with no device", target.ID)
		}
		vendor := ""
		for _, v := range []string{"nvidia", "amd"} {
			if target.SubsystemHas("gpu", v) {
				vendor = v
				break
			}
		}
		if vendor == "" {
			return "", fmt.Errorf(
				"job requires a GPU but cluster %q does not say which vendor: "+
					"cannot write a resource limit", target.ID)
		}
		// Whole node exclusive, so claim every GPU the node has.
		limits = append(limits, fmt.Sprintf("%s.com/gpu: %d", vendor, count))
	}

	// Whether this cluster satisfies the job's requires is Fluxion's answer, and
	// it has already given it: re-deciding here means a second matcher that can
	// disagree with the first, and the transform would win by refusing a job the
	// scheduler accepted. So ask only what the CLUSTER has. The node is held
	// exclusively, so its fabric is claimed when it exists.
	if target.SubsystemHas("network", "efa") {
		limits = append(limits, "vpc.amazonaws.com/efa: 1")
	}

	if len(limits) == 0 {
		return "", nil
	}
	out := "      resources:\n        limits:\n"
	for _, l := range limits {
		out += "          " + l + "\n"
	}
	return out, nil
}

// secretaryInstall renders the pip install run before the job, or "" when the
// cluster asks for none.
// secretaryInstall renders the commands that put flux-secretary in the view, or
// nothing when the cluster asks for none.
//
// ensurepip comes first: the view has an interpreter but no pip, so the install
// fails with "No module named pip" and the job then dies with "flux-secretary: not
// found", which reads as a launch problem and is not one.
func secretaryInstall(target graph.ClusterGraph, view string) []string {
	pkg := target.Cfg("secretary_package")
	if pkg == "" {
		pkg = secretaryPackage
	}
	if pkg == "none" {
		return nil
	}
	py := secretaryPythonFor(target, view)
	return []string{
		py + " -m ensurepip",
		py + " -m pip install --no-cache-dir " + pkg,
	}
}

// secretaryPythonFor is the interpreter that installs flux-secretary: an absolute
// path under the mounted view, matched to the view that was chosen.
//
// Each view ships one python3.N and no python3, and the version is a property of
// that view, so it is looked up rather than discovered in the pod. Set
// secretary_python on the cluster for a view that is not in the table.
func secretaryPythonFor(target graph.ClusterGraph, view string) string {
	if py := target.Cfg("secretary_python"); py != "" {
		return py
	}
	mount := target.Cfg("view_mount")
	if mount == "" {
		mount = defaultViewMount
	}
	root := strings.TrimRight(mount, "/") + "/view/bin/"
	for _, v := range secretaryViews {
		if v.image == view {
			return root + v.python
		}
	}
	return root + secretaryPython
}

// secretaryCommand wraps the workload in flux-secretary, passing the command
// through unchanged after the separator.
func secretaryCommand(js jobspec.Jobspec, target graph.ClusterGraph) string {
	model := target.Cfg("secretary_model")
	if model == "" {
		model = secretaryModel
	}
	flags := ""
	attempts := secretaryAttempts
	if v := target.Cfg("secretary_attempts"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			attempts = n
		}
	}
	flags = fmt.Sprintf(" --attempts %d", attempts)
	if model != "none" {
		flags += " --model " + model
	}
	return fmt.Sprintf("%s run --nodes %d%s -- %s",
		secretaryEntrypoint, js.Nodes(), flags, strings.Join(js.Command(), " "))
}

// secretaryVolume renders the agent token volume, or "" when the cluster has no
// token configured.
func secretaryVolume(target graph.ClusterGraph) string {
	name := target.Cfg("secretary_secret")
	if name == "" {
		return ""
	}
	// Container level only: v1alpha2 has no spec.volumes and prunes it silently.
	return fmt.Sprintf(`      volumes:
        secretary-token:
          path: /etc/flux-secretary
          secretName: %s
`, name)
}

// fluxBlock renders spec.flux: the view image and, on arm targets, the arch.
func fluxBlock(js jobspec.Jobspec, target graph.ClusterGraph) (string, error) {
	var b strings.Builder
	arm := isArm(target)
	view, err := secretaryView(js, target, arm)
	if err != nil {
		return "", err
	}
	if !arm && view == "" {
		return "", nil
	}
	b.WriteString("  flux:\n")
	if arm {
		// Without this the operator installs x86 view binaries on an arm cluster.
		b.WriteString("    arch: \"arm\"\n")
	}
	if view != "" {
		b.WriteString("    container:\n")
		b.WriteString(fmt.Sprintf("      image: %s\n", view))
		// Republished under the same tag, so a node holding an older layer would
		// run a different interpreter than the one pre names.
		b.WriteString("      pullAlways: true\n")
	}
	return b.String(), nil
}

const secretaryPackage = "flux-secretary[all]"

// secretaryModel is the Bedrock model the agent uses. behalf's default is denied
// by some service control policies.
const secretaryModel = "us.anthropic.claude-opus-5"

// Launch attempts the agent may make. Stated rather than left to the secretary's
// default, so the manifest records what the run allowed.
const secretaryAttempts = 10

// The python that installs flux-secretary: the view's, or any python3 on PATH.
//
// Located under ${viewroot}, which the operator substitutes, rather than by
// asking PATH where flux is: PATH puts the view last, so an application image's
// own flux wins and the derivation lands in the wrong directory. The version is
// found rather than pinned, and the regex is anchored so python3.N-config and
// python3.N-gdb.py do not match.
const secretaryPython = "python3.11"

// What the container runs. pip puts this console script in the view's bin, which
// the operator has on PATH, and the name is unique to flux-secretary so no
// application image can shadow it. No interpreter, no substitution.
const secretaryEntrypoint = "flux-secretary"

// Where the operator mounts the view: spec.flux.container.mountPath, default
// /mnt/flux. The broker reads <mount>/config/etc/flux/system/R.
const defaultViewMount = "/mnt/flux"

// secretaryViews are the stock flux views, keyed by the glibc they were built
// against.
var secretaryViews = []struct {
	image  string
	glibc  string
	python string // the interpreter under <mount>/view/bin that installs the secretary
	arm    bool
}{
	{"ghcr.io/converged-computing/flux-view-ubuntu:tag-noble", "2.39", "python3.13", false},
	{"ghcr.io/converged-computing/flux-view-ubuntu:jammy", "2.35", "python3.14", false},
	{"ghcr.io/converged-computing/flux-view-ubuntu:tag-focal", "2.31", "python3.11", false},
	{"ghcr.io/converged-computing/flux-view-ubuntu:arm-noble", "2.39", "python3.13", true},
	// jammy-arm, NOT jammy. :jammy is amd64 only, and pointing the arm row at it
	// mounted an amd64 view on an arm cluster: the operator copied the view into
	// place and the container then died with "exec /bin/bash: exec format error".
	// Both arm64 applications failed that way in every replicate, in both arms,
	// which reads as an application problem and is not one.
	//
	// jammy-arm is the arm64 build of the same view as :jammy, so it carries the
	// same interpreter. The older arm-jammy image is a different build with
	// python3.11 and is not interchangeable.
	{"ghcr.io/converged-computing/flux-view-ubuntu:jammy-arm", "2.35", "python3.14", true},
	{"ghcr.io/converged-computing/flux-view-ubuntu:arm-focal", "2.31", "python3.11", true},
}

// The glibc assumed when an image's own is not recorded. Jammy: it loads in
// anything newer, and it is what the corpus was built against.
const defaultViewGlibc = "2.35"

// secretaryView is the newest flux view whose glibc the job's image can host.
// A view links against the container's libc, so a newer one will not load.
func secretaryView(js jobspec.Jobspec, target graph.ClusterGraph, arm bool) (string, error) {
	switch v := target.Cfg("secretary_view"); v {
	case "none":
		return "", nil
	case "":
	default:
		return v, nil
	}

	// The newest view whose glibc the image can host: a view links against the
	// container's libc, so an older one loads and a newer one aborts before flux
	// starts. An image with no recorded glibc is assumed to be jammy.
	have := containerGlibc(js)
	if have == "" {
		have = defaultViewGlibc
	}
	var best struct{ image, glibc string }
	for _, v := range secretaryViews {
		if v.arm != arm {
			continue
		}
		if compareVersions(v.glibc, have) > 0 {
			continue
		}
		if best.image == "" || compareVersions(v.glibc, best.glibc) > 0 {
			best.image, best.glibc = v.image, v.glibc
		}
	}
	if best.image == "" {
		return "", fmt.Errorf("no flux view fits image glibc %s (arm=%v): a view links "+
			"against the container's libc, so a newer one will not load", have, arm)
	}
	return best.image, nil
}

// containerGlibc is the image's glibc as recorded by artifact-secretary, or "".
func containerGlibc(js jobspec.Jobspec) string {
	if js.Attributes == nil || js.Attributes.User == nil {
		return ""
	}
	c, ok := js.Attributes.User["container"].(map[string]any)
	if !ok {
		return ""
	}
	for _, k := range []string{"libc_version", "glibc"} {
		if v, ok := c[k].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// compareVersions compares dotted numeric versions: -1, 0, or 1.
func compareVersions(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) || i < len(bs); i++ {
		var x, y int
		if i < len(as) {
			x, _ = strconv.Atoi(as[i])
		}
		if i < len(bs) {
			y, _ = strconv.Atoi(bs[i])
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

// isArm reports whether the target advertises an arm64 architecture.
func isArm(target graph.ClusterGraph) bool {
	g := target.Subsystems["architecture"]
	if g == nil {
		return false
	}
	for i := range g.Graph.Nodes {
		switch g.Graph.Nodes[i].Metadata.Type {
		case "arm64", "aarch64", "arm":
			return true
		}
	}
	return false
}

func slurmJob(js jobspec.Jobspec, target graph.ClusterGraph) string {
	return fmt.Sprintf(`apiVersion: slurm.schedmd.com/v1
kind: SlurmJob
metadata:
  name: %s
spec:
  nodes: %d
  ntasks: %d
  image: %s
  command: %q
`, js.Name(), js.Nodes(), js.TasksTotal(), js.Image(), strings.Join(js.Command(), " "))
}

func quoteList(xs []string) string {
	q := make([]string, len(xs))
	for i, x := range xs {
		q[i] = fmt.Sprintf("%q", x)
	}
	return strings.Join(q, ", ")
}

// shJoin renders command tokens as a shell-safe single line for `flux submit`.
func shJoin(args []string) string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = shQuote(a)
	}
	return strings.Join(out, " ")
}

// shQuote single-quotes a token unless it is a simple safe word.
func shQuote(s string) string {
	if s != "" && !strings.ContainsAny(s, " \t\n'\"\\$`&|;<>()*?[]{}#~!") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// objectName is the native object name: the job name plus the job id, so two
// runs of one jobspec do not collide.
func objectName(js jobspec.Jobspec) string {
	name := js.Name()
	if name == "" {
		name = "job"
	}
	if id := js.JobID(); id != "" {
		name = name + "-" + id
	}
	return dnsSafe(name)
}

// dnsSafe reduces a name to what Kubernetes accepts for an object name.
func dnsSafe(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out = append(out, r)
		case r == '-' || r == '.':
			out = append(out, r)
		default:
			out = append(out, '-')
		}
	}
	trimmed := strings.Trim(string(out), "-.")
	if len(trimmed) > 253 {
		trimmed = strings.Trim(trimmed[:253], "-.")
	}
	if trimmed == "" {
		return "job"
	}
	return trimmed
}

// preBlock renders commands.pre, or "" when there is nothing to run.
func preBlock(cmds ...string) string {
	var keep []string
	for _, c := range cmds {
		if c != "" {
			keep = append(keep, c)
		}
	}
	if len(keep) == 0 {
		return ""
	}
	// A literal block: several commands compose, and nothing inside needs
	// quoting even when it holds a colon or a shell substitution.
	var b strings.Builder
	b.WriteString("      commands:\n        pre: |\n")
	for _, c := range keep {
		b.WriteString("          " + c + "\n")
	}
	return b.String()
}

// fluxResources makes the pod's GPUs visible to the broker.
//
// The core count is counted in the container, not taken from the node. Kubernetes
// allocatable.cpu counts hardware threads, and a resource set counts what the
// container can actually schedule on: declaring the node's 8 for a g5.2xlarge
// drained rank 0 with "missing resources: core[4-7]", after which no allocation
// could be satisfied and the job waited forever. sched_getaffinity respects the
// cpuset, and one id must be a bare index because "0-0" produces no children.
//
// Only the device count comes from fluxq, because that is the one thing the
// container cannot discover: flux's own detection does not see the GPU.
func fluxResources(target graph.ClusterGraph) []string {
	gpus := target.GPUsPerNode()
	if gpus < 1 {
		return nil
	}
	mount := target.Cfg("view_mount")
	if mount == "" {
		mount = defaultViewMount
	}
	path := strings.TrimRight(mount, "/") + "/config/etc/flux/system/R"
	py := "python3"
	if v := target.Cfg("view_python"); v != "" {
		py = v
	}
	cores := fmt.Sprintf(
		`$(%s -c "import os;n=len(os.sched_getaffinity(0));`+
			`print('0-%%d'%%(n-1) if n>1 else '0')")`, py)
	// No fallback: if this cannot be written the job must fail with it rather than
	// start a broker that cannot see the device.
	return []string{
		fmt.Sprintf("flux R encode --hosts=${hosts} --cores=%s --gpus=%s > %s",
			cores, idRange(gpus), path),
		"echo resource set the broker will read:",
		"cat " + path,
	}
}

// idRange is how flux wants a count of ids: a bare index for one, a hyphenated
// range for more. "0-0" is accepted and then produces no children at all.
func idRange(n int) string {
	if n <= 1 {
		return "0"
	}
	return fmt.Sprintf("0-%d", n-1)
}
