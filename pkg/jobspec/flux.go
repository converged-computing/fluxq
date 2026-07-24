package jobspec

import (
	"encoding/json"
	"fmt"
)

// ToFluxSpec renders the CONTAINMENT request as a Flux v1 jobspec in the shape
// real reapi requires to ALLOCATE against a fleetq containment graph
// (cluster→rack→node→socket→core): node→slot→socket→core. MatchSatisfy is
// lenient about intermediate levels, but MatchAllocate is strict and needs the
// socket level present, so we derive this canonical shape from the jobspec's
// node/core counts rather than forwarding the user's resource tree verbatim.
// The user still authors a real Flux jobspec; command/attributes pass through,
// and `requires` is dropped (subsystems match against their own graphs).
func (j Jobspec) ToFluxSpec() (string, error) {
	cores := j.CoresPerNode()
	if cores < 1 {
		cores = 1
	}
	inner := []Resource{{Type: "socket", Count: 1, With: []Resource{{Type: "core", Count: cores}}}}
	if g := j.GPUsPerNode(); g > 0 { // gpu/memory allocation shape is unverified against a live broker
		inner = append(inner, Resource{Type: "gpu", Count: g})
	}
	if mem := j.MemGBPerNode(); mem > 0 {
		inner = append(inner, Resource{Type: "memory", Count: mem, Unit: "GB"})
	}
	slot := Resource{Type: "slot", Count: 1, Label: "default", With: inner}
	node := Resource{Type: "node", Count: j.Nodes(), With: []Resource{slot}}
	cmd := j.Command()
	if len(cmd) == 0 {
		cmd = []string{"true"}
	}
	doc := fluxDoc{
		Version:    1,
		Resources:  []Resource{node},
		Tasks:      []Task{{Command: cmd, Slot: "default", Count: map[string]int{"per_slot": 1}}},
		Attributes: attrsMap(j.Attributes),
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("render flux jobspec: %w", err)
	}
	return string(b), nil
}

// SubsystemFluxSpec renders one subsystem section (a []Resource under a
// `requires` key) as a standalone Flux jobspec to MatchSatisfy against that
// subsystem's graph. The section is slot-wrapped so it is a valid jobspec; its
// resource types are matched by type in the subsystem graph (e.g. a "software"
// section requesting type "lammps" WITH "kokkos").
// AnyOfType marks a requires entry whose `with` children are ALTERNATIVES (OR):
// the entry is satisfied if ANY one child is. Everything else stays AND. Because
// each subsystem is satisfied by its own independent graph query, OR needs no
// Fluxion grammar support — we expand a section into its concrete alternatives
// and satisfy if any one of them satisfies.
const AnyOfType = "anyof"

// ExpandSection turns a requires section (possibly containing `anyof` groups)
// into the list of concrete AND-sections it denotes. The section is satisfied
// iff ANY returned concrete section is. With no `anyof`, the section is returned
// unchanged as the single alternative (so existing behavior is unaffected).
func ExpandSection(section []Resource) [][]Resource {
	alts := [][]Resource{{}}
	for _, r := range section {
		var next [][]Resource
		if r.Type == AnyOfType {
			for _, base := range alts {
				for _, choice := range r.With {
					row := append(append([]Resource{}, base...), choice)
					next = append(next, row)
				}
			}
		} else {
			for _, base := range alts {
				next = append(next, append(append([]Resource{}, base...), r))
			}
		}
		alts = next
	}
	return alts
}

func SubsystemFluxSpec(section []Resource) (string, error) {
	if len(section) == 0 {
		return "", fmt.Errorf("empty subsystem section")
	}
	slot := Resource{Type: "slot", Count: 1, Label: "satisfy", With: section}
	doc := fluxDoc{
		Version:    1,
		Resources:  []Resource{slot},
		Tasks:      []Task{{Command: []string{"true"}, Slot: "satisfy", Count: map[string]int{"per_slot": 1}}},
		Attributes: map[string]any{},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("render subsystem jobspec: %w", err)
	}
	return string(b), nil
}

func orOne(v int) int {
	if v <= 0 {
		return 1
	}
	return v
}

func attrsMap(a *Attributes) map[string]any {
	out := map[string]any{}
	if a == nil {
		return out
	}
	if len(a.System) > 0 {
		out["system"] = a.System
	}
	if len(a.User) > 0 {
		out["user"] = a.User
	}
	return out
}

// fluxDoc is the wire shape marshaled to Fluxion (attributes as a plain map so
// an empty attributes object still serializes cleanly).
type fluxDoc struct {
	Version    int            `json:"version"`
	Resources  []Resource     `json:"resources"`
	Tasks      []Task         `json:"tasks"`
	Attributes map[string]any `json:"attributes"`
}
