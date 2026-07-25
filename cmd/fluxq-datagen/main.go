// Command fluxq-datagen writes the dummy JGF fleet (real flux-sched schema) for
// testing without real clusters. Each cluster is data/fleet/<id>/containment.json
// with capabilities as vertex properties. Regenerate: go run ./cmd/fluxq-datagen
package main

import (
	"log"
	"os"

	"github.com/converged-computing/fluxq/pkg/graph"
)

type clusterSpec struct {
	id     string
	mgr    graph.ManagerType
	handle string
	nodes  []graph.NodeSpec
	caps   []string // capability property keys (software + network)
}

func specs() []clusterSpec {
	return []clusterSpec{
		{"efa-flux", graph.FluxOperator, "eks://efa-flux",
			[]graph.NodeSpec{{Count: 5, Cores: 64, MemGB: 128}}, []string{"lammps", "amg", "efa"}},
		{"slurm-cpu", graph.SlurmOperator, "gke://slurm-cpu",
			[]graph.NodeSpec{{Count: 8, Cores: 48, MemGB: 96}}, []string{"lammps", "qmcpack", "ethernet"}},
		{"gpu-k8s", graph.K8sJob, "gke://gpu-k8s",
			[]graph.NodeSpec{{Count: 4, Cores: 32, GPUs: 4, MemGB: 256}}, []string{"qmcpack", "ethernet"}},
		{"onprem-flux", graph.FluxURI, "ssh://login/run/flux/local-0",
			[]graph.NodeSpec{{Count: 3, Cores: 64, MemGB: 128}}, []string{"lammps", "amg", "infiniband"}},
		{"mixed-eks", graph.FluxOperator, "eks://mixed-eks",
			[]graph.NodeSpec{{Count: 6, Cores: 96, MemGB: 192}, {Count: 2, Cores: 48, GPUs: 8, MemGB: 512}},
			[]string{"lammps", "qmcpack", "amg", "efa"}},
	}
}

func main() {
	root := "data/fleet"
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		log.Fatal(err)
	}
	for _, s := range specs() {
		g := graph.BuildContainment(s.id, s.mgr, s.handle, s.nodes, s.caps)
		if err := graph.ExportCluster(root, s.id, g); err != nil {
			log.Fatal(err)
		}
		log.Printf("wrote %s/%s/containment.json (caps: %v)", root, s.id, s.caps)
	}
	log.Printf("done: %d clusters under %s", len(specs()), root)
}
