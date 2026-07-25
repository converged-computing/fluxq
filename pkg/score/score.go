// Package score ranks the feasible clusters a matcher returns for a job. It is
// pluggable, like policy: feasibility is the graph's job (matcher.Evaluate),
// GOODNESS is here. The manager scores every feasible candidate, breaks ties
// RANDOMLY (so the same cluster is not always returned), and commits to the
// best. A scorer also declares whether it needs the whole feasible set or can
// commit on the first feasible candidate (a match-first optimization that lets
// the manager skip scoring entirely).
package score

import (
	"math/rand"
	"sort"

	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/matcher"
)

type Scorer interface {
	Name() string
	// NeedsFullSet=false lets the manager commit on the first feasible cluster
	// (no cross-cluster comparison), collapsing the two passes into one.
	NeedsFullSet() bool
	// Score a single feasible candidate; higher is better.
	Score(js jobspec.Jobspec, c matcher.Candidate) float64
}

// Default balances two things: prefer clusters that satisfy MORE of the job's
// requested subsystems, then best-fit on containment (least wasted capacity, so
// big jobs do not fragment small clusters). Slot-locality can be folded in as a
// further term later.
type Default struct{}

func (Default) Name() string       { return "default" }
func (Default) NeedsFullSet() bool { return true }

func (Default) Score(js jobspec.Jobspec, c matcher.Candidate) float64 {
	if !c.Feasible {
		return -1
	}
	matched := float64(len(c.Matched))
	// best-fit: fewer leftover free nodes beyond the ask scores higher.
	surplus := c.FreeNodes - js.Nodes()
	if surplus < 0 {
		surplus = 0
	}
	fit := 1.0 / float64(1+surplus)
	// free-now is a mild bonus so runnable-now beats feasible-but-full ties.
	free := 0.0
	if c.FreeNow {
		free = 0.5
	}
	return matched*10 + fit + free
}

// FirstFeasible commits to the first feasible cluster (queue/registration
// order), skipping cross-cluster comparison — the match-first policy.
type FirstFeasible struct{}

func (FirstFeasible) Name() string                                     { return "first-feasible" }
func (FirstFeasible) NeedsFullSet() bool                               { return false }
func (FirstFeasible) Score(jobspec.Jobspec, matcher.Candidate) float64 { return 0 }

// Rank scores and orders the feasible candidates, breaking ties randomly. It
// shuffles first, then stable-sorts by score descending, so equal scores come
// back in random order (no always-same-cluster bias).
func Rank(s Scorer, js jobspec.Jobspec, cands []matcher.Candidate) []matcher.Candidate {
	feasible := make([]matcher.Candidate, 0, len(cands))
	for _, c := range cands {
		if c.Feasible {
			feasible = append(feasible, c)
		}
	}
	rand.Shuffle(len(feasible), func(i, j int) { feasible[i], feasible[j] = feasible[j], feasible[i] })
	for i := range feasible {
		feasible[i].Score = s.Score(js, feasible[i])
	}
	sort.SliceStable(feasible, func(i, j int) bool { return feasible[i].Score > feasible[j].Score })
	return feasible
}
