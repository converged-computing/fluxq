// Package jobspec defines the job description that flows through the pipeline.
// It IS a real Flux jobspec (RFC 25, version 1): resources + tasks + attributes,
// the exact document `flux run`/Fluxion consume. The only addition is an
// optional `requires` block — a map of subsystem name -> Flux resource subtree —
// carrying the extra sections we split out to query auxiliary subsystem graphs
// (software, network, …). Vanilla Flux ignores `requires`; fluxq uses it.
//
// The top-level `resources` is the CONTAINMENT section (the countable request).
// Each `requires[sub]` is another section, in the same resource vocabulary,
// that we slot-wrap and MatchSatisfy against that subsystem's own graph. So one
// jobspec document carries one containment section + N subsystem sections, and
// selection is "containment matches AND every requested subsystem satisfies".
package jobspec

import "time"

// Resource is a Flux jobspec resource vertex (RFC 14/25): a type, a count, an
// optional slot label, and nested children. Subsystem matching is by TYPE — a
// specific software package is a vertex of that type (e.g. type "lammps"), so a
// `requires` entry is just Flux resources, not a bespoke predicate language.
type Resource struct {
	Type      string     `json:"type" yaml:"type"`
	Count     int        `json:"count" yaml:"count"`
	Label     string     `json:"label,omitempty" yaml:"label,omitempty"`
	Exclusive bool       `json:"exclusive,omitempty" yaml:"exclusive,omitempty"`
	Unit      string     `json:"unit,omitempty" yaml:"unit,omitempty"`
	With      []Resource `json:"with,omitempty" yaml:"with,omitempty"`
}

// Task is a Flux jobspec task: a command bound to a slot label.
type Task struct {
	Command []string       `json:"command" yaml:"command"`
	Slot    string         `json:"slot,omitempty" yaml:"slot,omitempty"`
	Count   map[string]int `json:"count,omitempty" yaml:"count,omitempty"`
}

// Attributes mirrors Flux jobspec attributes. system.duration and
// system.job.name are standard; we stash the container image under user.image
// (user attributes are explicitly free-form in RFC 25).
type Attributes struct {
	System map[string]any `json:"system,omitempty" yaml:"system,omitempty"`
	User   map[string]any `json:"user,omitempty" yaml:"user,omitempty"`
}

// Jobspec is a Flux v1 jobspec plus the `requires` subsystem sections.
type Jobspec struct {
	Version    int                   `json:"version" yaml:"version"`
	Resources  []Resource            `json:"resources" yaml:"resources"`
	Tasks      []Task                `json:"tasks,omitempty" yaml:"tasks,omitempty"`
	Attributes *Attributes           `json:"attributes,omitempty" yaml:"attributes,omitempty"`
	Requires   map[string][]Resource `json:"requires,omitempty" yaml:"requires,omitempty"`
}

// ---- accessors: read the Flux document, don't duplicate its fields ----

func (j Jobspec) Name() string {
	if j.Attributes != nil {
		if job, ok := j.Attributes.System["job"].(map[string]any); ok {
			if n, ok := job["name"].(string); ok {
				return n
			}
		}
	}
	return ""
}

func (j Jobspec) Image() string {
	if j.Attributes != nil && j.Attributes.User != nil {
		if s, ok := j.Attributes.User["image"].(string); ok {
			return s
		}
	}
	return ""
}

func (j Jobspec) Command() []string {
	if len(j.Tasks) > 0 {
		return j.Tasks[0].Command
	}
	return nil
}

func (j Jobspec) Duration() time.Duration {
	if j.Attributes != nil {
		if v, ok := numAttr(j.Attributes.System["duration"]); ok {
			return time.Duration(v) * time.Second
		}
	}
	return 0
}

func numAttr(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	}
	return 0, false
}

// counts walks the containment resource tree. Resources under a `node` are
// counted per-node; if no node type appears the whole request is treated as a
// single node.
func (j Jobspec) counts() (nodes, coresPerNode, gpusPerNode, memPerNode int) {
	var perNode func(rs []Resource, mult int, acc *[3]int)
	perNode = func(rs []Resource, mult int, acc *[3]int) {
		for _, r := range rs {
			c := r.Count
			if c <= 0 {
				c = 1
			}
			m := mult * c
			switch r.Type {
			case "core":
				acc[0] += m
			case "gpu":
				acc[1] += m
			case "memory":
				acc[2] += m
			}
			perNode(r.With, m, acc)
		}
	}
	found := false
	var walk func(rs []Resource, mult int)
	walk = func(rs []Resource, mult int) {
		for _, r := range rs {
			c := r.Count
			if c <= 0 {
				c = 1
			}
			m := mult * c
			if r.Type == "node" {
				found = true
				nodes += m
				var acc [3]int
				perNode(r.With, 1, &acc)
				if acc[0] > coresPerNode {
					coresPerNode = acc[0]
				}
				if acc[1] > gpusPerNode {
					gpusPerNode = acc[1]
				}
				if acc[2] > memPerNode {
					memPerNode = acc[2]
				}
				continue
			}
			walk(r.With, m)
		}
	}
	walk(j.Resources, 1)
	if !found {
		nodes = 1
		var acc [3]int
		perNode(j.Resources, 1, &acc)
		coresPerNode, gpusPerNode, memPerNode = acc[0], acc[1], acc[2]
	}
	if nodes < 1 {
		nodes = 1
	}
	return
}

func (j Jobspec) Nodes() int { n, _, _, _ := j.counts(); return n }

// Exclusive reports whether the job wants WHOLE nodes. True when a node resource
// is marked exclusive, and also when no cores are requested at all: a node-level
// request with no per-core detail means "give me these nodes", not "give me one
// core on each of them".
func (j Jobspec) Exclusive() bool {
	var walk func(rs []Resource) bool
	walk = func(rs []Resource) bool {
		for _, r := range rs {
			if r.Type == "node" && r.Exclusive {
				return true
			}
			if walk(r.With) {
				return true
			}
		}
		return false
	}
	if walk(j.Resources) {
		return true
	}
	return j.CoresPerNode() < 1
}
func (j Jobspec) CoresPerNode() int { _, c, _, _ := j.counts(); return c }
func (j Jobspec) GPUsPerNode() int  { _, _, g, _ := j.counts(); return g }
func (j Jobspec) MemGBPerNode() int { _, _, _, m := j.counts(); return m }

// TasksTotal is a best-effort total rank count for stub transforms: honor an
// explicit task total, else per_slot × nodes, else nodes.
func (j Jobspec) TasksTotal() int {
	n := j.Nodes()
	if len(j.Tasks) == 0 {
		return n
	}
	if t, ok := j.Tasks[0].Count["total"]; ok && t > 0 {
		return t
	}
	if ps, ok := j.Tasks[0].Count["per_slot"]; ok && ps > 0 {
		return n * ps
	}
	return n
}

// RequiredTypes flattens the top-level resource types requested across all
// subsystems (used by the advisory reconciler, not by matching).
func (j Jobspec) RequiredTypes() []string {
	var out []string
	for _, rs := range j.Requires {
		for _, r := range rs {
			if r.Type != "" {
				out = append(out, r.Type)
			}
		}
	}
	return out
}

// ---- builders (datagen/reconcile/tests) ----

// Containment builds a canonical Flux containment resource tree:
// node(count=nodes) -> slot(label=default) -> core(count=cores)[ + gpu][ + memory].
func Containment(nodes, coresPerNode, gpusPerNode, memGBPerNode int) []Resource {
	slotWith := []Resource{{Type: "core", Count: coresPerNode}}
	if gpusPerNode > 0 {
		slotWith = append(slotWith, Resource{Type: "gpu", Count: gpusPerNode})
	}
	if memGBPerNode > 0 {
		slotWith = append(slotWith, Resource{Type: "memory", Count: memGBPerNode, Unit: "GB"})
	}
	return []Resource{{
		Type: "node", Count: nodes,
		With: []Resource{{Type: "slot", Count: 1, Label: "default", With: slotWith}},
	}}
}

// New assembles a Flux jobspec from the common fields, keeping the document
// Flux-shaped. Used by tests and the example generator.
func New(name, image string, command []string, nodes, coresPerNode int, duration time.Duration, requires map[string][]Resource) Jobspec {
	sys := map[string]any{}
	if duration > 0 {
		sys["duration"] = int(duration.Seconds())
	}
	if name != "" {
		sys["job"] = map[string]any{"name": name}
	}
	user := map[string]any{}
	if image != "" {
		user["image"] = image
	}
	return Jobspec{
		Version:    1,
		Resources:  Containment(nodes, coresPerNode, 0, 0),
		Tasks:      []Task{{Command: command, Slot: "default", Count: map[string]int{"per_slot": 1}}},
		Attributes: &Attributes{System: sys, User: user},
		Requires:   requires,
	}
}
