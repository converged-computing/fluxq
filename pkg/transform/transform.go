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
		return cluster.Content{Kind: "manifest", Payload: miniCluster(js, target)}, nil
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

func miniCluster(js jobspec.Jobspec, _ graph.ClusterGraph) string {
	return fmt.Sprintf(`apiVersion: flux-framework.org/v1alpha2
kind: MiniCluster
metadata:
  name: %s
spec:
  size: %d
  tasks: %d
  containers:
    - image: %s
      command: %q
`, js.Name(), js.Nodes(), js.TasksTotal(), js.Image(), strings.Join(js.Command(), " "))
}

func slurmJob(js jobspec.Jobspec, _ graph.ClusterGraph) string {
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
