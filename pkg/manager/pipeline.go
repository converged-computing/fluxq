package manager

// The staged pipeline. Scheduling (whole-queue view, in scheduleOnce) ends at
// Allocate and hands the job to the dispatch stage. From there each stage is an
// independent consumer with its own concurrency and retry (river queues in
// river.go); these handlers are the transport-agnostic core they call, so the
// logic is testable under -race with no database.
//
// The discipline that keeps it correct: SLOW work (transform, submit, agent,
// status, cancel) runs with NO lock held; only queue/matcher state mutation
// takes m.mu. Never hold m.mu across a driver or agent call.
//
// Free happens exactly once per job via freeLocked (it clears AllocID). The
// allocation is owned by whatever stage holds the job; every terminal exit and
// the one wrong-target hand-back frees it. Repair keeps the allocation (same
// cluster); only wrong-target cancels + frees + returns the job to scheduling,
// bounded by Job.Reschedules.

import (
	"time"

	"github.com/converged-computing/fleetq/pkg/cluster"
	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/jobspec"
	"github.com/converged-computing/fleetq/pkg/queue"
)

type dispatchOutcome int

const (
	outcomeSuccess     dispatchOutcome = iota // running -> monitor
	outcomeRepairable                         // fixable artifact -> repair (keep allocation)
	outcomeWrongTarget                        // cluster can't host this -> cancel+free+reschedule
	outcomeTransient                          // blip (token, 5xx) -> retry same stage (river backoff)
	outcomeHardFail                           // terminal -> free + Failed (user resubmits)
)

type attemptResult struct {
	outcome  dispatchOutcome
	handle   string
	note     string
	artifact string // the native artifact that was generated/attempted (carried for repair + audit)
}

const (
	maxReschedules  = 3
	monitorInterval = 2 * time.Second
)

// repairer is the optional capability a Transformer can implement to fix an
// artifact a validator rejected (AgentTransformer does). Structural, so the
// transform package need not import this one.
type repairer interface {
	Repair(js jobspec.Jobspec, target graph.ClusterGraph, failed, validationErr string) (cluster.Content, error)
}

// --- seams (real implementations; overridable in tests) ---

func (m *Manager) runAttempt(j queue.Job) attemptResult {
	if m.attempt != nil {
		return m.attempt(j)
	}
	c, ok := m.Fleet.Get(j.ClusterID)
	if !ok {
		return attemptResult{outcome: outcomeHardFail, note: "assigned cluster not found"}
	}
	content, err := m.Trans.Transform(j.Spec, c)
	if err != nil {
		return attemptResult{outcome: outcomeHardFail, note: "transform: " + err.Error()}
	}
	return m.validateThenSubmit(c, content)
}

func (m *Manager) runRepair(j queue.Job, lastErr string) attemptResult {
	if m.repairFn != nil {
		return m.repairFn(j, lastErr)
	}
	r, ok := m.Trans.(repairer)
	if !ok {
		return attemptResult{outcome: outcomeHardFail, note: "no repair agent configured"}
	}
	c, ok := m.Fleet.Get(j.ClusterID)
	if !ok {
		return attemptResult{outcome: outcomeHardFail, note: "assigned cluster not found"}
	}
	content, err := r.Repair(j.Spec, c, j.Artifact, lastErr)
	if err != nil {
		return attemptResult{outcome: outcomeHardFail, note: "repair: " + err.Error()}
	}
	return m.validateThenSubmit(c, content)
}

// validateThenSubmit is the credential gate shared by dispatch and repair: the
// artifact must pass the validator before it reaches the credentialed Submit.
// LLM proposes, validator disposes.
func (m *Manager) validateThenSubmit(c graph.ClusterGraph, content cluster.Content) attemptResult {
	switch v, detail := m.validateContent(c, content); v {
	case verdictWrongTarget:
		return attemptResult{outcome: outcomeWrongTarget, note: detail, artifact: content.Payload}
	case verdictRepairable:
		return attemptResult{outcome: outcomeRepairable, note: detail, artifact: content.Payload}
	}
	drv, err := m.driverFor(c)
	if err != nil {
		return attemptResult{outcome: outcomeHardFail, note: err.Error(), artifact: content.Payload}
	}
	handle, err := drv.Submit(c, contentToDriver(content))
	if err != nil {
		return attemptResult{outcome: outcomeHardFail, note: "submit: " + err.Error(), artifact: content.Payload}
	}
	return attemptResult{outcome: outcomeSuccess, handle: handle, note: "dispatched", artifact: content.Payload}
}

func (m *Manager) status(c graph.ClusterGraph, handle string) (queue.State, string, error) {
	if m.statusFn != nil {
		return m.statusFn(c, handle)
	}
	drv, err := m.driverFor(c)
	if err != nil {
		return "", "", err
	}
	return drv.Status(c, handle)
}

func (m *Manager) cancelRemote(c graph.ClusterGraph, handle string) error {
	if m.cancelFn != nil {
		return m.cancelFn(c, handle)
	}
	drv, err := m.driverFor(c)
	if err != nil {
		return err
	}
	return drv.Cancel(c, handle)
}

// freeLocked releases the allocation exactly once. Caller holds m.mu.
func (m *Manager) freeLocked(j *queue.Job) {
	if j.AllocID == "" {
		return
	}
	_ = m.Matcher.Free(j.AllocID)
	j.AllocID = ""
}

// failByID frees + marks Failed under the lock. The user sees a terminal,
// unsatisfied job with the reason; resubmitting is on them.
func (m *Manager) failByID(id, note string) {
	m.mu.Lock()
	if j, ok := m.Queue.Get(id); ok {
		m.freeLocked(&j)
		j.State = queue.Failed
		j.Note = note
		_ = m.Queue.Update(j)
	}
	m.mu.Unlock()
	m.logf("fail %s: %s", id, note)
}

// --- stage handlers (return the next queue name; "" = none) ---

// handleDispatch: transform + submit (unlocked), then route.
func (m *Manager) handleDispatch(id string) (next string, retry bool) {
	m.mu.Lock()
	j, ok := m.Queue.Get(id)
	if !ok || j.State.Terminal() {
		m.mu.Unlock()
		return "", false
	}
	j.State = queue.Dispatching
	_ = m.Queue.Update(j)
	work := j
	m.mu.Unlock()

	res := m.runAttempt(work) // UNLOCKED — the future agent loop lives here

	switch res.outcome {
	case outcomeSuccess:
		m.mu.Lock()
		if j, ok := m.Queue.Get(id); ok {
			j.State = queue.Running
			j.RemoteHandle = res.handle
			j.Note = res.note
			j.Artifact = res.artifact
			_ = m.Queue.Update(j)
		}
		m.mu.Unlock()
		m.logf("dispatch %s -> running as %s", id, res.handle)
		return "monitor", false

	case outcomeRepairable:
		m.mu.Lock()
		if j, ok := m.Queue.Get(id); ok {
			j.Note = res.note         // the validation error to fix
			j.Artifact = res.artifact // the rejected artifact repair will edit
			_ = m.Queue.Update(j)     // keep the allocation
		}
		m.mu.Unlock()
		return "repair", false

	case outcomeTransient:
		return "", true // keep allocation; caller signals river to retry with backoff

	case outcomeWrongTarget:
		m.reschedule(id, res.note)
		return "", false

	default: // outcomeHardFail
		m.failByID(id, res.note)
		return "", false
	}
}

// handleRepair: run the repair step (unlocked), then route. attempt/max come
// from the transport (river job attempt counters).
func (m *Manager) handleRepair(id, lastErr string, attempt, maxAttempt int) (next string, retry bool) {
	m.mu.Lock()
	j, ok := m.Queue.Get(id)
	if !ok || j.State.Terminal() {
		m.mu.Unlock()
		return "", false
	}
	work := j
	m.mu.Unlock()

	res := m.runRepair(work, lastErr) // UNLOCKED

	switch res.outcome {
	case outcomeSuccess: // repaired artifact validated AND submitted -> monitor
		m.mu.Lock()
		if j, ok := m.Queue.Get(id); ok {
			j.State = queue.Running
			j.RemoteHandle = res.handle
			j.Note = res.note
			j.Artifact = res.artifact
			_ = m.Queue.Update(j)
		}
		m.mu.Unlock()
		m.logf("repair %s -> running as %s", id, res.handle)
		return "monitor", false
	case outcomeWrongTarget:
		m.reschedule(id, res.note)
		return "", false
	case outcomeRepairable:
		if attempt < maxAttempt {
			m.mu.Lock()
			if j, ok := m.Queue.Get(id); ok {
				j.Note = res.note
				j.Artifact = res.artifact // carry the still-broken artifact into the next repair
				_ = m.Queue.Update(j)
			}
			m.mu.Unlock()
			return "", true // more attempts left: river backoff, retry repair
		}
		m.failByID(id, "repair exhausted: "+res.note)
		return "", false
	default: // outcomeHardFail
		m.failByID(id, "repair: "+res.note)
		return "", false
	}
}

// handleMonitor: poll status (unlocked); free on terminal. Returns whether the
// job is done and how long to wait before the next poll.
func (m *Manager) handleMonitor(id string) (done bool, snooze time.Duration) {
	m.mu.Lock()
	j, ok := m.Queue.Get(id)
	if !ok || j.State.Terminal() {
		m.mu.Unlock()
		return true, 0
	}
	if j.RemoteHandle == "" {
		m.mu.Unlock()
		return false, monitorInterval
	}
	c, _ := m.Fleet.Get(j.ClusterID)
	handle := j.RemoteHandle
	m.mu.Unlock()

	st, note, err := m.status(c, handle) // UNLOCKED
	if err != nil {
		return false, monitorInterval // transient; keep polling
	}

	m.mu.Lock()
	if j, ok := m.Queue.Get(id); ok {
		if st != j.State || note != j.Note {
			j.State, j.Note = st, note
		}
		if st.Terminal() {
			m.freeLocked(&j)
			m.logf("free %s (terminal %s)", id, st)
		}
		_ = m.Queue.Update(j)
	}
	m.mu.Unlock()
	return st.Terminal(), monitorInterval
}

// reschedule is the ONLY path back to the schedule queue: cancel any remote job
// (unlocked), free the allocation, and return the job to provisional for a
// fresh selection — possibly a better cluster given the fleet moved on. Bounded
// by Job.Reschedules so it cannot bounce forever.
func (m *Manager) reschedule(id, why string) {
	m.mu.Lock()
	j, ok := m.Queue.Get(id)
	if !ok {
		m.mu.Unlock()
		return
	}
	c, _ := m.Fleet.Get(j.ClusterID)
	handle := j.RemoteHandle
	m.mu.Unlock()

	if handle != "" {
		_ = m.cancelRemote(c, handle) // UNLOCKED — never cancel under the lock
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok = m.Queue.Get(id)
	if !ok {
		return
	}
	m.freeLocked(&j)
	j.RemoteHandle = ""
	if j.Reschedules+1 > maxReschedules {
		j.State = queue.Failed
		j.Note = "wrong-target; reschedule limit reached: " + why
		_ = m.Queue.Update(j)
		m.logf("fail %s: reschedule limit reached", id)
		return
	}
	j.Reschedules++
	j.ClusterID = ""
	j.State = queue.Submitted
	j.Note = "rescheduling (" + why + ")"
	_ = m.Queue.Update(j)
	m.logf("reschedule %s (attempt %d): %s", id, j.Reschedules, why)
}
