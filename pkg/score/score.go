// Package score ranks the feasible clusters a matcher returns for a job. It is
// pluggable, like policy: feasibility is the graph's job (matcher.Evaluate),
// GOODNESS is here. The manager scores every feasible candidate, breaks ties
// RANDOMLY (so the same cluster is not always returned), and commits to the
// best. A scorer also declares whether it needs the whole feasible set or can
// commit on the first feasible candidate (a match-first optimization that lets
// the manager skip scoring entirely).
package score

import (
	"fmt"
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
	ranked, _ := RankWithTrace(s, js, cands)
	return ranked
}

// RankWithTrace ranks the feasible candidates and returns a record of how.
//
// Rank alone answers "where did it go" and discards everything that would answer
// "why". The clusters that were rejected, and which subsystem each was missing,
// are the interesting half: a placement that differs between two runs is only
// evidence if you can say what distinguished the alternatives. The tie-break is
// a shuffle, so its seed is recorded too, or an arbitrary choice between equal
// scores is indistinguishable from a decision.
func RankWithTrace(s Scorer, js jobspec.Jobspec, cands []matcher.Candidate) (
	[]matcher.Candidate, Trace) {

	tr := Trace{
		Considered: len(cands),
		Scorer:     fmt.Sprintf("%T", s),
		Rejected:   []Rejection{},
	}

	feasible := make([]matcher.Candidate, 0, len(cands))
	for _, c := range cands {
		if c.Feasible {
			feasible = append(feasible, c)
			continue
		}
		tr.Rejected = append(tr.Rejected, Rejection{
			Cluster:   c.Cluster,
			Missing:   c.Missing,
			Matched:   c.Matched,
			FreeNodes: c.FreeNodes,
		})
	}
	tr.Feasible = len(feasible)

	// The seed is recorded so a tie-break can be replayed. Without it, two runs
	// that scored identically and landed differently look like a decision.
	tr.ShuffleSeed = rand.Int63()
	shuffler := rand.New(rand.NewSource(tr.ShuffleSeed))
	shuffler.Shuffle(len(feasible), func(i, j int) {
		feasible[i], feasible[j] = feasible[j], feasible[i]
	})

	for i := range feasible {
		feasible[i].Score = s.Score(js, feasible[i])
	}
	sort.SliceStable(feasible, func(i, j int) bool {
		return feasible[i].Score > feasible[j].Score
	})

	for _, c := range feasible {
		e := Ranking{Cluster: c.Cluster, Score: c.Score, Matched: c.Matched,
			FreeNow: c.FreeNow, FreeNodes: c.FreeNodes}
		if d, ok := s.(Explainer); ok {
			e.Terms = d.Explain(js, c)
		}
		tr.Ranked = append(tr.Ranked, e)
	}
	if len(feasible) > 0 {
		tr.Selected = feasible[0].Cluster
		// A tie at the top means the shuffle chose, not the score.
		for _, c := range feasible[1:] {
			if c.Score == feasible[0].Score {
				tr.TiedAtTop = append(tr.TiedAtTop, c.Cluster)
			}
		}
		if len(tr.TiedAtTop) > 0 {
			tr.TiedAtTop = append([]string{tr.Selected}, tr.TiedAtTop...)
		}
	}
	return feasible, tr
}

// Trace records how a placement decision was reached, so it can be read back and
// argued with rather than taken on trust.
type Trace struct {
	Considered  int         `json:"considered"`
	Feasible    int         `json:"feasible"`
	Scorer      string      `json:"scorer"`
	Selected    string      `json:"selected,omitempty"`
	Ranked      []Ranking   `json:"ranked"`
	Rejected    []Rejection `json:"rejected"`
	ShuffleSeed int64       `json:"shuffle_seed"`
	// Clusters that scored the same as the winner. The order between them came
	// from the shuffle, so the choice is arbitrary and should be reported that way.
	TiedAtTop []string `json:"tied_at_top,omitempty"`

	// The scorer declined to rank (NeedsFullSet false), so the order is whatever
	// the matcher produced and Ranked is empty. Said explicitly, because an empty
	// ranking otherwise looks like a bug.
	Unranked bool `json:"unranked,omitempty"`
}

// Ranking is one feasible cluster and what it scored.
type Ranking struct {
	Cluster   string             `json:"cluster"`
	Score     float64            `json:"score"`
	Terms     map[string]float64 `json:"terms,omitempty"` // the score, broken down
	Matched   []string           `json:"matched,omitempty"`
	FreeNow   bool               `json:"free_now"`
	FreeNodes int                `json:"free_nodes"`
}

// Rejection is one cluster that could not run the job, and what it lacked.
type Rejection struct {
	Cluster   string   `json:"cluster"`
	Missing   []string `json:"missing"` // subsystems that did not satisfy, or "containment"
	Matched   []string `json:"matched,omitempty"`
	FreeNodes int      `json:"free_nodes"`
}

// Explainer is a Scorer that can break its score into named terms. Optional: a
// scorer without it still ranks, the trace simply carries the total alone.
type Explainer interface {
	Explain(js jobspec.Jobspec, c matcher.Candidate) map[string]float64
}

// Explain breaks the default score into the terms that produced it, so a reader
// can see whether a cluster won on subsystem matches, on fit, or on being free.
func (d Default) Explain(js jobspec.Jobspec, c matcher.Candidate) map[string]float64 {
	if !c.Feasible {
		return map[string]float64{"infeasible": -1}
	}
	surplus := c.FreeNodes - js.Nodes()
	if surplus < 0 {
		surplus = 0
	}
	free := 0.0
	if c.FreeNow {
		free = 0.5
	}
	return map[string]float64{
		"matched":  float64(len(c.Matched)) * 10,
		"fit":      1.0 / float64(1+surplus),
		"free_now": free,
		"surplus":  float64(surplus),
	}
}
