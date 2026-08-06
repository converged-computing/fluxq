package score_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/converged-computing/fluxq/pkg/jobspec"
	"github.com/converged-computing/fluxq/pkg/matcher"
	"github.com/converged-computing/fluxq/pkg/score"
)

func cands() []matcher.Candidate {
	return []matcher.Candidate{
		{Cluster: "gke-small", Feasible: true, Matched: []string{"architecture"},
			FreeNow: true, FreeNodes: 3},
		{Cluster: "gke-arm", Feasible: false, Missing: []string{"architecture"},
			FreeNodes: 3},
		{Cluster: "gke-big", Feasible: true, Matched: []string{"architecture"},
			FreeNow: true, FreeNodes: 3},
		{Cluster: "eks-full", Feasible: false, Missing: []string{"containment"},
			Matched: []string{"architecture"}, FreeNodes: 0},
	}
}

func job() jobspec.Jobspec {
	return jobspec.New("app", "img", []string{"run"}, 2, 0, time.Hour, nil)
}

func TestTraceKeepsTheClustersThatLost(t *testing.T) {
	// Ranked candidates say where a job went. Answering WHY needs the clusters
	// that were rejected and what each was missing, which Rank discards: a
	// placement that differs between two arms is only evidence if the
	// alternatives can be named.
	ranked, tr := score.RankWithTrace(score.Default{}, job(), cands())

	if len(ranked) != 2 {
		t.Fatalf("two feasible clusters, got %d", len(ranked))
	}
	if tr.Considered != 4 || tr.Feasible != 2 {
		t.Errorf("considered=%d feasible=%d, want 4 and 2", tr.Considered, tr.Feasible)
	}
	if len(tr.Rejected) != 2 {
		t.Fatalf("two rejections expected, got %d", len(tr.Rejected))
	}
	// and the reason has to survive, not just the fact
	byCluster := map[string]score.Rejection{}
	for _, r := range tr.Rejected {
		byCluster[r.Cluster] = r
	}
	if got := byCluster["gke-arm"].Missing; len(got) != 1 || got[0] != "architecture" {
		t.Errorf("gke-arm should be missing architecture, got %v", got)
	}
	if got := byCluster["eks-full"].Missing; len(got) != 1 || got[0] != "containment" {
		t.Errorf("eks-full should be missing containment, got %v", got)
	}
	// a cluster can match a subsystem and still lose on containment
	if len(byCluster["eks-full"].Matched) != 1 {
		t.Errorf("eks-full matched architecture and should still say so: %+v",
			byCluster["eks-full"])
	}
}

func TestTraceNamesTiesRatherThanHidingThem(t *testing.T) {
	// Two clusters scoring the same are ordered by a shuffle. Reporting only the
	// winner makes an arbitrary choice look like a decision, which is how a fleet
	// with equal candidates produces a lopsided placement count that nobody can
	// account for.
	_, tr := score.RankWithTrace(score.Default{}, job(), cands())

	if len(tr.TiedAtTop) != 2 {
		t.Fatalf("gke-small and gke-big score the same; want both named, got %v",
			tr.TiedAtTop)
	}
	if tr.TiedAtTop[0] != tr.Selected {
		t.Errorf("the selected cluster should lead the tie list: %v vs %q",
			tr.TiedAtTop, tr.Selected)
	}
	if tr.ShuffleSeed == 0 {
		t.Error("the seed must be recorded, or the tie-break cannot be replayed")
	}

	// with distinct scores there is no tie to report
	c := cands()
	c[2].FreeNodes = 40 // a much worse fit
	_, tr2 := score.RankWithTrace(score.Default{}, job(), c)
	if len(tr2.TiedAtTop) != 0 {
		t.Errorf("scores differ, so nothing is tied: %v", tr2.TiedAtTop)
	}
	if tr2.Selected != "gke-small" {
		t.Errorf("best fit should win outright, got %q", tr2.Selected)
	}
}

func TestTraceBreaksTheScoreIntoTerms(t *testing.T) {
	// One float cannot be argued with. The terms say whether a cluster won on
	// subsystem matches, on fit, or on happening to be free.
	_, tr := score.RankWithTrace(score.Default{}, job(), cands())
	top := tr.Ranked[0]
	for _, want := range []string{"matched", "fit", "free_now", "surplus"} {
		if _, ok := top.Terms[want]; !ok {
			t.Errorf("missing term %q in %v", want, top.Terms)
		}
	}
	if top.Terms["matched"] != 10 {
		t.Errorf("one matched subsystem is worth 10, got %v", top.Terms["matched"])
	}
	sum := top.Terms["matched"] + top.Terms["fit"] + top.Terms["free_now"]
	if sum != top.Score {
		t.Errorf("terms should add to the score: %v vs %v", sum, top.Score)
	}
}

func TestTraceRoundTripsAsJSON(t *testing.T) {
	// The receipt is consumed by the experiment harness, so it has to survive
	// serialisation with its field names intact.
	_, tr := score.RankWithTrace(score.Default{}, job(), cands())
	b, err := json.Marshal(tr)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"considered"`, `"feasible"`, `"selected"`,
		`"ranked"`, `"rejected"`, `"missing"`, `"terms"`, `"shuffle_seed"`,
		`"tied_at_top"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %s in %s", want, b)
		}
	}
	var back score.Trace
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Selected != tr.Selected || len(back.Rejected) != len(tr.Rejected) {
		t.Errorf("round trip lost something: %+v", back)
	}
}

func TestRankStillBehavesTheSame(t *testing.T) {
	// The old entry point must keep returning only the feasible clusters, ranked.
	got := score.Rank(score.Default{}, job(), cands())
	if len(got) != 2 {
		t.Fatalf("Rank should return the two feasible clusters, got %d", len(got))
	}
	for _, c := range got {
		if !c.Feasible {
			t.Errorf("Rank returned an infeasible cluster: %+v", c)
		}
	}
}
