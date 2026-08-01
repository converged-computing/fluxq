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
		if g == nil {
			t.Fatalf("%s: missing subsystem", cat.sub)
		}
		// Fluxion needs a cluster root + a contains edge to the value vertex.
		var hasRoot, hasValue bool
		for _, v := range g.Graph.Nodes {
			if v.Metadata.Type == "cluster" {
				hasRoot = true
			}
			if v.Metadata.Type == cat.val {
				hasValue = true
				if v.Metadata.Paths["containment"] == "" {
					t.Fatalf("%s: value vertex must have a containment path", cat.sub)
				}
			}
		}
		if !hasRoot || !hasValue || len(g.Graph.Edges) != 1 {
			t.Fatalf("%s: want cluster root + %q vertex + 1 edge, got %+v", cat.sub, cat.val, g)
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

func TestContainmentFromFactsGroupsNodes(t *testing.T) {
	nodes := []NodeFacts{
		{Arch: "amd64", Network: "ethernet", MemoryGB: 16, Cores: 4},
		{Arch: "amd64", Network: "ethernet", MemoryGB: 16, Cores: 4},
		{Arch: "amd64", Network: "ethernet", MemoryGB: 16, Cores: 4},
	}
	g := ContainmentFromFacts("sched-gke-cpu", "k8s-job", "k8s-job://sched-gke-cpu", nodes)
	if g == nil || len(g.Graph.Nodes) == 0 {
		t.Fatal("containment must be built from discovered nodes")
	}
	var cluster, nodeCount, coreCount int
	for _, v := range g.Graph.Nodes {
		switch v.Metadata.Type {
		case "cluster":
			cluster++
		case "node":
			nodeCount++
		case "core":
			coreCount++
		}
	}
	if cluster != 1 || nodeCount != 3 {
		t.Fatalf("want 1 cluster + 3 nodes, got %d/%d", cluster, nodeCount)
	}
	if coreCount != 12 { // 3 nodes x 4 cores
		t.Fatalf("want 12 cores, got %d", coreCount)
	}
}

func TestDiscoveryRegistersMatchableMemorySubsystem(t *testing.T) {
	// A cluster must advertise a matchable `memory` bucket:
	// requires.memory names `memory`, and an unregistered subsystem is
	// unsatisfiable rather than ignored.
	for _, tc := range []struct {
		gb   int
		want string
	}{
		{12, "0-16GB"}, {28, "16-64GB"}, {120, "64-192GB"}, {248, "192GB+"},
	} {
		subs, _, err := SubsystemsFromFacts([]NodeFacts{
			{Arch: "amd64", Network: "ethernet", MemoryGB: tc.gb, Cores: 4}})
		if err != nil {
			t.Fatal(err)
		}
		g := subs["memory"]
		if g == nil {
			t.Fatalf("%dGB: no memory subsystem registered", tc.gb)
		}
		var got string
		for _, v := range g.Graph.Nodes {
			if v.Metadata.Type != "cluster" {
				got = v.Metadata.Type
			}
		}
		if got != tc.want {
			t.Fatalf("%dGB -> %q, want %q", tc.gb, got, tc.want)
		}
	}
	t.Log("memory buckets are matchable and absolute")
}
