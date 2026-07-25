package cluster

import "testing"

func TestDiscoverBuildsAbsoluteSubsystems(t *testing.T) {
	nodes := []NodeFacts{
		{Arch: "arm64", GPUVendor: "nvidia", Network: "efa", MemoryGB: 340},
		{Arch: "arm64", GPUVendor: "nvidia", Network: "efa", MemoryGB: 340},
	}
	subs, mem, err := SubsystemsFromFacts(nodes)
	if err != nil {
		t.Fatal(err)
	}
	for _, cat := range []struct{ sub, val string }{
		{"architecture", "arm64"}, {"gpu", "nvidia"}, {"network", "efa"},
	} {
		g := subs[cat.sub]
		if g == nil || len(g.Graph.Nodes) != 1 || g.Graph.Nodes[0].Metadata.Type != cat.val {
			t.Fatalf("%s: want a vertex of type %q, got %+v", cat.sub, cat.val, g)
		}
	}
	if len(mem) != 2 || mem[0] != 340 {
		t.Fatalf("memory captured raw for vocabulary bucketing: %v", mem)
	}
}

func TestDiscoverOmitsAbsentAndMixed(t *testing.T) {
	// no gpu anywhere -> omitted; mixed arch -> omitted (don't guess); network uniform
	nodes := []NodeFacts{
		{Arch: "amd64", Network: "ethernet"},
		{Arch: "arm64", Network: "ethernet"},
	}
	subs, _, _ := SubsystemsFromFacts(nodes)
	if _, ok := subs["gpu"]; ok {
		t.Fatal("gpu must be omitted when no node has one")
	}
	if _, ok := subs["architecture"]; ok {
		t.Fatal("architecture must be omitted when nodes disagree (mixed pool)")
	}
	if subs["network"] == nil {
		t.Fatal("uniform network should still be discovered")
	}
}
