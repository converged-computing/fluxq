package graph

import (
	"encoding/json"
	"os"
)

// JGF is the JSON Graph Format Fluxion uses for resource graphs (the same
// schema as flux-sched's tiny.json / fluxion-quantum's exports). Modeling it
// directly means our exports load unchanged into real Fluxion, and the sim
// matcher traverses the exact structure the reapi bindings would.
type JGF struct {
	Graph JGFGraph `json:"graph"`
}

type JGFGraph struct {
	Nodes []Vertex `json:"nodes"`
	Edges []Edge   `json:"edges"`
}

type Vertex struct {
	ID       string     `json:"id"`
	Metadata VertexMeta `json:"metadata"`
}

// VertexMeta is a pragmatic subset of Fluxion vertex metadata. Size lets a
// single vertex stand for N units of its type (e.g. a core vertex size=64),
// so a homogeneous cluster stays compact. Properties carries cluster-level
// attributes (manager, handle) on the root, matching where Fluxion keeps them.
type VertexMeta struct {
	Type       string            `json:"type"`
	Basename   string            `json:"basename,omitempty"`
	Name       string            `json:"name,omitempty"`
	ID         int               `json:"id"`
	UniqID     int               `json:"uniq_id"`
	Rank       int               `json:"rank"`
	Size       int               `json:"size"`
	Unit       string            `json:"unit"`
	Exclusive  bool              `json:"exclusive"`
	Properties map[string]string `json:"properties,omitempty"`
	Paths      map[string]string `json:"paths,omitempty"`
}

// Edge is directed parent->child. Subsystem names the space the edge belongs
// to ("containment" is dominant); auxiliary subsystems reuse the same schema.
type Edge struct {
	Source   string   `json:"source"`
	Target   string   `json:"target"`
	Metadata EdgeMeta `json:"metadata,omitempty"`
}

type EdgeMeta struct {
	Subsystem string `json:"subsystem,omitempty"`
	Name      string `json:"name,omitempty"`
}

// index returns id->vertex and parent->children adjacency.
func (g *JGF) index() (map[string]*Vertex, map[string][]string) {
	byID := make(map[string]*Vertex, len(g.Graph.Nodes))
	for i := range g.Graph.Nodes {
		byID[g.Graph.Nodes[i].ID] = &g.Graph.Nodes[i]
	}
	children := map[string][]string{}
	for _, e := range g.Graph.Edges {
		children[e.Source] = append(children[e.Source], e.Target)
	}
	return byID, children
}

// verticesOfType returns every vertex whose metadata.Type matches.
func (g *JGF) verticesOfType(t string) []*Vertex {
	var out []*Vertex
	for i := range g.Graph.Nodes {
		if g.Graph.Nodes[i].Metadata.Type == t {
			out = append(out, &g.Graph.Nodes[i])
		}
	}
	return out
}

// find returns the first vertex satisfying pred (a satisfy traversal).
func (g *JGF) find(pred func(Vertex) bool) *Vertex {
	for i := range g.Graph.Nodes {
		if pred(g.Graph.Nodes[i]) {
			return &g.Graph.Nodes[i]
		}
	}
	return nil
}

func loadJGF(path string) (*JGF, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var g JGF
	if err := json.Unmarshal(b, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

func writeJGF(path string, g *JGF) error {
	b, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// Exported traversal helpers for use by the matcher package.

// IndexExported returns id->vertex and parent->children adjacency.
func (g *JGF) IndexExported() (map[string]*Vertex, map[string][]string) { return g.index() }

// VerticesOfTypeExported returns all vertices of a given metadata type.
func (g *JGF) VerticesOfTypeExported(t string) []*Vertex { return g.verticesOfType(t) }

// FindExported returns the first vertex satisfying pred (a satisfy traversal).
func (g *JGF) FindExported(pred func(Vertex) bool) *Vertex { return g.find(pred) }

// JSON renders the JGF graph as a JSON string (what reapi InitContext expects).
func (g *JGF) JSON() (string, error) {
	b, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// SingletonSubsystem builds a one-vertex descriptive subsystem graph whose
// vertex TYPE is the discovered value (e.g. subsystem "architecture" with type
// "arm64"). A requires section matches it by type: requires["architecture"] =
// [{type:"arm64"}]. This is what --discover emits per vocabulary category.
func SingletonSubsystem(subsystem, valueType string) *JGF {
	return &JGF{Graph: JGFGraph{
		Nodes: []Vertex{{
			ID: "1",
			Metadata: VertexMeta{
				Type:     valueType,
				Basename: valueType,
				Name:     valueType,
				UniqID:   1,
				Size:     1,
				Paths:    map[string]string{subsystem: "/" + valueType},
			},
		}},
	}}
}
