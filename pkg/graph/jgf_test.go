package graph

import (
	"strings"
	"testing"
)

func TestSingletonSubsystemPathsEndInVertexName(t *testing.T) {
	// Fluxion builds its graph from `paths`; the subsystem graphs it is known to
	// accept all have every path ending in that vertex's own name. A mismatch
	// loads without error but never matches, which is silent and expensive to
	// debug.
	g := SingletonSubsystem("architecture", "amd64")
	if len(g.Graph.Nodes) != 2 || len(g.Graph.Edges) != 1 {
		t.Fatalf("want cluster root + value vertex + 1 edge, got %+v", g.Graph)
	}
	for _, v := range g.Graph.Nodes {
		p := v.Metadata.Paths[ContainmentSubsystem]
		if p == "" {
			t.Fatalf("%s: no containment path", v.Metadata.Type)
		}
		if last := p[strings.LastIndex(p, "/")+1:]; last != v.Metadata.Name {
			t.Errorf("%s: path %q must end in the vertex name %q",
				v.Metadata.Type, p, v.Metadata.Name)
		}
	}
	if g.Graph.Nodes[0].Metadata.UniqID == g.Graph.Nodes[1].Metadata.UniqID {
		t.Error("vertices must have distinct uniq_id")
	}
}
