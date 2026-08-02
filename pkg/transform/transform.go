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
	// launcher: true so the operator does NOT wrap the command in its own flux
	// submit. flux-secretary owns submission: it runs inside the allocation,
	// reads what Flux actually has, and sizes the launch accordingly. Nothing out
	// here can know that. tasks: 0 keeps the operator from adding an -n of its
	// own, which produced "alloc denied due to type=unsatisfiable".
	devices, err := deviceResources(js, target)
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
  launcher: true
%s%s  containers:
    - image: %s
      command: %q
      commands:
        pre: %s
%s%s`, js.Name(), js.Nodes(), fluxBlock(target), secretaryVolume(target, "  "),
		js.Image(), secretaryCommand(js), secretaryInstall,
		devices, secretaryVolume(target, "      ")), nil
}

// deviceResources writes the container resource limits for hardware the job
// requires.
//
// A Kubernetes pod only ever receives a device it explicitly requests: a GPU pod
// with no nvidia.com/gpu limit starts with no visible device, and an EFA pod with
// no vpc.amazonaws.com/efa limit gets no interface and its MPI quietly falls back
// to TCP. Both failures are silent, which is worse than a crash, because the job
// still produces numbers.
//
// So this returns an ERROR rather than omitting a device the job asked for. A
// scheduler that places work on hardware the job never receives is not measuring
// anything.
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

	// EFA is requested only when the job asked for it, and it must be present:
	// a job matched on network=efa that runs over TCP measures the wrong thing.
	if requiresValue(js, "network", "efa") {
		if !target.SubsystemHas("network", "efa") {
			return "", fmt.Errorf("job requires efa but cluster %q does not advertise it",
				target.ID)
		}
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

// requiresValue reports whether a requires section names a value, including
// inside an anyof.
func requiresValue(js jobspec.Jobspec, sub, value string) bool {
	var walk func(rs []jobspec.Resource) bool
	walk = func(rs []jobspec.Resource) bool {
		for _, r := range rs {
			if r.Type == value || walk(r.With) {
				return true
			}
		}
		return false
	}
	return walk(js.Requires[sub])
}

// The secretary is installed with the interpreter Flux ships its bindings for.
// The bindings are compiled, so the container's own python3 may import the
// package but not flux.
const secretaryInstall = "flux python -m pip install --user --quiet flux-secretary"

// secretaryCommand wraps the workload. The command is passed through untouched
// after the separator: the secretary chooses how to launch it, never what to run.
func secretaryCommand(js jobspec.Jobspec) string {
	return fmt.Sprintf("flux python -m fluxsecretary.cli run --nodes %d -- %s",
		js.Nodes(), strings.Join(js.Command(), " "))
}

// secretaryVolume renders the agent token volume, when the cluster was
// registered with one. It is emitted TWICE: at the spec level to declare the
// volume, and under the container to mount it. Without a token the secretary
// falls back to its deterministic ladder, so a fleet with no token still runs.
// The CRD takes environment as plain key/value with no secretKeyRef, so a
// mounted secret is how a token gets in.
func secretaryVolume(target graph.ClusterGraph, indent string) string {
	name := target.Cfg("secretary_secret")
	if name == "" {
		return ""
	}
	return fmt.Sprintf(`%svolumes:
%s  secretary-token:
%s    path: /etc/flux-secretary
%s    secretName: %s
`, indent, indent, indent, indent, name)
}

// fluxBlock adds MiniCluster-level flux settings derived from the target. An
// arm64 cluster needs the arm flux view, or the operator installs x86 binaries.
func fluxBlock(target graph.ClusterGraph) string {
	if isArm(target) {
		return "  flux:\n    arch: \"arm\"\n"
	}
	return ""
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
