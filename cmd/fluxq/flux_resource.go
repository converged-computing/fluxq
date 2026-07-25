package main

// `fluxq cluster subsystem from-flux` derives a containment subsystem from the
// flux instance the command is run inside (the RV1 resource set from
// `flux resource R`) and attaches it to a registered cluster. This is how you
// point fluxq at a real local flux broker instead of a hand-authored JGF:
//
//	flux start                         # (in the devcontainer)
//	fluxq cluster register --name local --manager flux-uri
//	export FLUXQ_SECRET=<printed>
//	fluxq cluster subsystem from-flux --cluster local
//
// RV1 is the authoritative resource description (what Fluxion itself consumes),
// so we read it rather than scraping `flux resource list` text.

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/converged-computing/fluxq/pkg/graph"
)

// rv1 is the subset of the RFC 20 (R version 1) resource set we need.
type rv1 struct {
	Execution struct {
		RLite []struct {
			Rank     string `json:"rank"`
			Children struct {
				Core string `json:"core"`
				GPU  string `json:"gpu"`
			} `json:"children"`
		} `json:"R_lite"`
	} `json:"execution"`
}

// idsetCount counts members of a flux idset ("0", "0-7", "0-3,8-11").
func idsetCount(s string) int {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	n := 0
	for _, part := range strings.Split(s, ",") {
		if part == "" {
			continue
		}
		if i := strings.IndexByte(part, '-'); i >= 0 {
			lo, e1 := strconv.Atoi(strings.TrimSpace(part[:i]))
			hi, e2 := strconv.Atoi(strings.TrimSpace(part[i+1:]))
			if e1 == nil && e2 == nil && hi >= lo {
				n += hi - lo + 1
			}
		} else if _, err := strconv.Atoi(part); err == nil {
			n++
		}
	}
	return n
}

// fluxContainmentGroups runs `flux resource R` and folds RV1 into NodeSpec
// groups (nodes with the same cores/gpus shape are merged into one group).
func fluxContainmentGroups() ([]graph.NodeSpec, error) {
	out, err := exec.Command("flux", "resource", "R").Output()
	if err != nil {
		return nil, fmt.Errorf("run `flux resource R` (are you inside a flux instance? try `flux start`): %w", err)
	}
	var r rv1
	if err := json.Unmarshal(out, &r); err != nil {
		return nil, fmt.Errorf("parse RV1 from `flux resource R`: %w", err)
	}
	type sig struct{ cores, gpus int }
	var order []sig
	count := map[sig]int{}
	for _, e := range r.Execution.RLite {
		nodes := idsetCount(e.Rank)
		if nodes == 0 {
			nodes = 1 // a bare rank with no range is a single node
		}
		s := sig{cores: idsetCount(e.Children.Core), gpus: idsetCount(e.Children.GPU)}
		if _, ok := count[s]; !ok {
			order = append(order, s)
		}
		count[s] += nodes
	}
	if len(order) == 0 {
		return nil, fmt.Errorf("no resources found in RV1 (empty R_lite)")
	}
	groups := make([]graph.NodeSpec, 0, len(order))
	for _, s := range order {
		groups = append(groups, graph.NodeSpec{Count: count[s], Cores: s.cores, GPUs: s.gpus})
	}
	return groups, nil
}

func subsystemFromFlux(args []string) error {
	fs := flag.NewFlagSet("cluster subsystem from-flux", flag.ExitOnError)
	server := serverFlag(fs)
	secret := secretFlag(fs)
	cluster := fs.String("cluster", "", "registered cluster to attach containment to")
	capsCSV := fs.String("caps", "", "optional capability properties on the root, comma-separated")
	print := fs.Bool("print", false, "print the derived containment JGF and exit (do not post)")
	_ = fs.Parse(args)

	groups, err := fluxContainmentGroups()
	if err != nil {
		return err
	}
	var caps []string
	if *capsCSV != "" {
		caps = strings.Split(*capsCSV, ",")
	}
	name := *cluster
	if name == "" {
		name = "local"
	}
	handle := os.Getenv("FLUX_URI")
	if handle == "" {
		handle = "flux-uri://" + name
	}
	jgf := graph.BuildContainment(name, graph.FluxURI, handle, groups, caps)
	raw, err := jgf.JSON()
	if err != nil {
		return err
	}

	total := 0
	shape := make([]string, 0, len(groups))
	for _, g := range groups {
		total += g.Count
		shape = append(shape, fmt.Sprintf("%d×%dcore", g.Count, g.Cores))
	}

	if *print {
		fmt.Println(raw)
		fmt.Fprintf(os.Stderr, "# derived from local flux: %d node(s) [%s]\n", total, strings.Join(shape, ", "))
		return nil
	}
	if *cluster == "" {
		return fmt.Errorf("usage: fluxq cluster subsystem from-flux --cluster C [--caps a,b] [--print] --secret S")
	}
	body := map[string]any{"descriptive": false, "graph": json.RawMessage(raw)}
	out, _, err := httpDoAuth("POST", *server+"/v1/clusters/"+*cluster+"/subsystems/containment", *secret, body)
	if err != nil {
		return err
	}
	fmt.Printf("registered containment from local flux: %d node(s) [%s] — %s\n",
		total, strings.Join(shape, ", "), strings.TrimSpace(string(out)))
	return nil
}
