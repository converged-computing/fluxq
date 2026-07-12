// Package queue holds submitted work and its lifecycle. The interface is
// deliberately shaped like fluxnetes' river-backed design: a provisional set
// (Submitted, awaiting a match) that the policy reads from, and state
// transitions as jobs move to matched/dispatched/running/terminal. The
// in-memory implementation here runs anywhere; a river/Postgres implementation
// (see river_adapter.go, //go:build river) is a drop-in for durability,
// retries, and crash recovery.
package queue

import (
	"sort"
	"sync"
	"time"

	"github.com/converged-computing/fleetq/pkg/jobspec"
)

type State string

const (
	Submitted   State = "SUBMITTED"   // in the provisional queue, not yet matched
	Matched     State = "MATCHED"     // Fluxion allocated a cluster
	Dispatching State = "DISPATCHING" // transform + submit in flight
	Running     State = "RUNNING"     // remote manager reports running
	Completed   State = "COMPLETED"
	Failed      State = "FAILED"
)

func (s State) Terminal() bool { return s == Completed || s == Failed }

// Job is the tracked unit. It threads together the agnostic intent, the match
// result, and the native remote handle so one record answers "where is it and
// how do I ask about it" — this is the receipt spine the monitor reconciles.
type Job struct {
	ID          string
	Spec        jobspec.Jobspec
	State       State
	SubmittedAt time.Time

	// filled after a match
	AllocID   string
	ClusterID string

	// filled after dispatch
	RemoteHandle string // native id at the target (k8s object, flux jobid, ...)
	Note         string // last human-readable status/error
	Artifact     string `json:",omitempty"` // last generated native artifact (for repair + audit)

	// set when an infeasible job was given a reconciliation suggestion
	Suggestion    string           `json:",omitempty"` // rationale for a proposed reconfiguration
	SuggestedSpec *jobspec.Jobspec `json:",omitempty"` // a jobspec that WOULD fit

	// Reschedules counts wrong-target hand-backs to scheduling (cancel+free+
	// re-select). Bounded so a job can't bounce between clusters forever.
	Reschedules int `json:",omitempty"`

	UpdatedAt time.Time
}

// Queue is the storage seam.
type Queue interface {
	Enqueue(j Job) error
	Get(id string) (Job, bool)
	// Provisional returns Submitted jobs oldest-first (FCFS base order).
	Provisional() []Job
	// Active returns non-terminal, already-matched jobs (for the monitor loop).
	Active() []Job
	// All is the "flux jobs" view.
	All() []Job
	Update(j Job) error
}

// Memory is the zero-dependency implementation.
type Memory struct {
	mu   sync.Mutex
	jobs map[string]Job
}

func NewMemory() *Memory { return &Memory{jobs: map[string]Job{}} }

func (m *Memory) Enqueue(j Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j.UpdatedAt = time.Now()
	m.jobs[j.ID] = j
	return nil
}

func (m *Memory) Get(id string) (Job, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	return j, ok
}

func (m *Memory) Update(j Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j.UpdatedAt = time.Now()
	m.jobs[j.ID] = j
	return nil
}

func (m *Memory) filter(pred func(Job) bool) []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Job
	for _, j := range m.jobs {
		if pred(j) {
			out = append(out, j)
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].SubmittedAt.Before(out[k].SubmittedAt) })
	return out
}

func (m *Memory) Provisional() []Job {
	return m.filter(func(j Job) bool { return j.State == Submitted })
}
func (m *Memory) Active() []Job {
	return m.filter(func(j Job) bool {
		return j.State == Matched || j.State == Dispatching || j.State == Running
	})
}
func (m *Memory) All() []Job { return m.filter(func(Job) bool { return true }) }
