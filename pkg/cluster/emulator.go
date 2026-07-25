package cluster

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/converged-computing/fluxq/pkg/graph"
	"github.com/converged-computing/fluxq/pkg/queue"
)

// The emulator lets the whole pipeline be exercised with no real clusters. A
// shared runtime handles timing, elapsed-time status, cancellation, and failure
// injection; a per-manager EmuBackend supplies the dialect-specific behavior —
// what artifact it accepts and how it validates it, the shape of its handles,
// and its own status vocabulary. That makes each emulated backend a contract
// test for transform: it rejects output the real backend would reject.

// Phase is a scheduled transition: at elapsed time At, enter State, append Log.
type Phase struct {
	At    time.Duration
	State queue.State
	Log   string
}

// Plan is an ordered set of phases (first should be At=0).
type Plan struct{ Phases []Phase }

// EmulatorConfig controls default timing and optional failure injection.
type EmulatorConfig struct {
	Pending time.Duration
	Run     time.Duration
	// Inject, if set and it returns ok, overrides the default lifecycle for a
	// submission (force timeouts, crashes, etc.).
	Inject func(Content) (Plan, bool)
}

func (c EmulatorConfig) withDefaults() EmulatorConfig {
	if c.Pending == 0 {
		c.Pending = 300 * time.Millisecond
	}
	if c.Run == 0 {
		c.Run = 1200 * time.Millisecond
	}
	return c
}

// EmuBackend captures the per-manager-type behavior an emulator imitates.
type EmuBackend interface {
	Manager() graph.ManagerType
	WantKind() string // "manifest" | "command"
	// Validate parses the artifact as this backend would; returns a defect note
	// and ok=false if the transform output is malformed for this dialect.
	Validate(c Content) (note string, ok bool)
	// NewHandle mints a backend-shaped native id.
	NewHandle(seq int) string
	// Lifecycle is the default success plan with this backend's state labels.
	Lifecycle(cfg EmulatorConfig) Plan
}

type emJob struct {
	submitted time.Time
	plan      Plan
	canceled  bool
}

// Emulator is one fake cluster's runtime, parameterized by a backend.
type Emulator struct {
	mu      sync.Mutex
	cfg     EmulatorConfig
	backend EmuBackend
	jobs    map[string]*emJob
	n       int
}

func newEmulator(b EmuBackend, cfg EmulatorConfig) *Emulator {
	return &Emulator{cfg: cfg.withDefaults(), backend: b, jobs: map[string]*emJob{}}
}

func (e *Emulator) submit(c Content) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.n++
	handle := e.backend.NewHandle(e.n)

	var plan Plan
	if strings.TrimSpace(c.Payload) == "" {
		plan = Plan{Phases: []Phase{{At: 0, State: queue.Failed, Log: "[emu] rejected: empty payload from transform"}}}
	} else if note, ok := e.backend.Validate(c); !ok {
		plan = Plan{Phases: []Phase{{At: 0, State: queue.Failed, Log: "[emu] rejected: " + note}}}
	} else if e.cfg.Inject != nil {
		if p, ok := e.cfg.Inject(c); ok {
			plan = p
		}
	}
	if len(plan.Phases) == 0 {
		plan = e.backend.Lifecycle(e.cfg)
	}
	e.jobs[handle] = &emJob{submitted: time.Now(), plan: plan}
	return handle, nil
}

func (e *Emulator) current(handle string) (queue.State, string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	j, ok := e.jobs[handle]
	if !ok {
		return queue.Failed, "unknown handle", false
	}
	if j.canceled {
		return queue.Failed, "canceled", true
	}
	elapsed := time.Since(j.submitted)
	state, note := queue.Running, "PENDING"
	for _, p := range j.plan.Phases {
		if elapsed >= p.At {
			state, note = p.State, p.Log
		}
	}
	return state, note, true
}

func (e *Emulator) logs(handle string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	j, ok := e.jobs[handle]
	if !ok {
		return ""
	}
	elapsed := time.Since(j.submitted)
	var b strings.Builder
	for _, p := range j.plan.Phases {
		if elapsed >= p.At {
			fmt.Fprintf(&b, "%v %s\n", p.At, p.Log)
		}
	}
	return b.String()
}

func (e *Emulator) cancel(handle string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if j, ok := e.jobs[handle]; ok {
		j.canceled = true
	}
}

// ---- shared lifecycle builder ----

func lifecycle(cfg EmulatorConfig, pending, running, done string) Plan {
	return Plan{Phases: []Phase{
		{At: 0, State: queue.Running, Log: "[emu] " + pending},
		{At: cfg.Pending, State: queue.Running, Log: "[emu] " + running},
		{At: cfg.Pending + cfg.Run, State: queue.Completed, Log: "[emu] " + done},
	}}
}

// ---- per-backend modes ----

type fluxOperatorBackend struct{}

func (fluxOperatorBackend) Manager() graph.ManagerType { return graph.FluxOperator }
func (fluxOperatorBackend) WantKind() string           { return "manifest" }
func (fluxOperatorBackend) NewHandle(seq int) string {
	return fmt.Sprintf("minicluster/flux-sample-%d", seq)
}
func (fluxOperatorBackend) Lifecycle(c EmulatorConfig) Plan {
	return lifecycle(c, "MiniCluster created; pods pending", "MiniCluster RUNNING", "MiniCluster COMPLETED; exit 0")
}
func (fluxOperatorBackend) Validate(c Content) (string, bool) {
	if c.Kind != "manifest" {
		return "flux-operator expects a manifest", false
	}
	if !strings.Contains(c.Payload, "kind: MiniCluster") {
		return "manifest is not a MiniCluster", false
	}
	if !strings.Contains(c.Payload, "size:") {
		return "MiniCluster missing spec.size", false
	}
	if !strings.Contains(c.Payload, "image:") {
		return "MiniCluster missing a container image", false
	}
	return "", true
}

type slurmOperatorBackend struct{}

func (slurmOperatorBackend) Manager() graph.ManagerType { return graph.SlurmOperator }
func (slurmOperatorBackend) WantKind() string           { return "manifest" }
func (slurmOperatorBackend) NewHandle(seq int) string   { return fmt.Sprintf("slurmjob/%d", seq) }
func (slurmOperatorBackend) Lifecycle(c EmulatorConfig) Plan {
	return lifecycle(c, "PENDING", "RUNNING", "COMPLETED")
}
func (slurmOperatorBackend) Validate(c Content) (string, bool) {
	if c.Kind != "manifest" {
		return "slurm-operator expects a manifest", false
	}
	if !strings.Contains(c.Payload, "kind: SlurmJob") {
		return "manifest is not a SlurmJob", false
	}
	if !strings.Contains(c.Payload, "nodes:") || !strings.Contains(c.Payload, "ntasks:") {
		return "SlurmJob missing nodes/ntasks", false
	}
	return "", true
}

type k8sJobBackend struct{}

func (k8sJobBackend) Manager() graph.ManagerType { return graph.K8sJob }
func (k8sJobBackend) WantKind() string           { return "manifest" }
func (k8sJobBackend) NewHandle(seq int) string   { return fmt.Sprintf("job/app-%05d", seq) }
func (k8sJobBackend) Lifecycle(c EmulatorConfig) Plan {
	return lifecycle(c, "ContainerCreating", "Running", "Completed")
}
func (k8sJobBackend) Validate(c Content) (string, bool) {
	if c.Kind != "manifest" {
		return "k8s-job expects a manifest", false
	}
	if !strings.Contains(c.Payload, "kind: Job") {
		return "manifest is not a batch/v1 Job", false
	}
	if !strings.Contains(c.Payload, "image:") {
		return "Job container missing an image", false
	}
	if !strings.Contains(c.Payload, "command:") {
		return "Job container missing a command", false
	}
	return "", true
}

type fluxURIBackend struct{}

func (fluxURIBackend) Manager() graph.ManagerType { return graph.FluxURI }
func (fluxURIBackend) WantKind() string           { return "jobspec" }
func (fluxURIBackend) NewHandle(seq int) string   { return fmt.Sprintf("fluxjob-f%X", seq*7919) }
func (fluxURIBackend) Lifecycle(c EmulatorConfig) Plan {
	return lifecycle(c, "SCHED", "RUN", "INACTIVE (exit 0)")
}
func (fluxURIBackend) Validate(c Content) (string, bool) {
	if c.Kind != "jobspec" {
		return "flux-uri expects a jobspec", false
	}
	var spec struct {
		Resources json.RawMessage `json:"resources"`
		Tasks     json.RawMessage `json:"tasks"`
	}
	if err := json.Unmarshal([]byte(c.Payload), &spec); err != nil {
		return "jobspec is not valid JSON", false
	}
	if len(spec.Resources) == 0 || len(spec.Tasks) == 0 {
		return "jobspec missing resources/tasks", false
	}
	return "", true
}

func backendFor(m graph.ManagerType) EmuBackend {
	switch m {
	case graph.FluxOperator:
		return fluxOperatorBackend{}
	case graph.SlurmOperator:
		return slurmOperatorBackend{}
	case graph.K8sJob:
		return k8sJobBackend{}
	case graph.FluxURI:
		return fluxURIBackend{}
	default:
		return k8sJobBackend{}
	}
}

// EmulatedDriver adapts a per-backend Emulator to the Driver interface.
type EmulatedDriver struct {
	backend EmuBackend
	em      *Emulator
}

// NewEmulatedDriver builds an emulated driver in the mode of manager m.
func NewEmulatedDriver(m graph.ManagerType, cfg EmulatorConfig) *EmulatedDriver {
	b := backendFor(m)
	return &EmulatedDriver{backend: b, em: newEmulator(b, cfg)}
}

// NewEmulatedRegistry registers an emulated driver for every manager type.
func NewEmulatedRegistry(cfg EmulatorConfig) *Registry {
	return NewRegistry(
		NewEmulatedDriver(graph.FluxOperator, cfg),
		NewEmulatedDriver(graph.SlurmOperator, cfg),
		NewEmulatedDriver(graph.K8sJob, cfg),
		NewEmulatedDriver(graph.FluxURI, cfg),
	)
}

func (d *EmulatedDriver) Type() graph.ManagerType { return d.backend.Manager() }

func (d *EmulatedDriver) Submit(target graph.ClusterGraph, c Content) (string, error) {
	if c.Kind != d.backend.WantKind() {
		return "", fmt.Errorf("%s expects a %s, got %q", d.backend.Manager(), d.backend.WantKind(), c.Kind)
	}
	return d.em.submit(c)
}

func (d *EmulatedDriver) Status(target graph.ClusterGraph, handle string) (queue.State, string, error) {
	st, note, _ := d.em.current(handle)
	return st, note, nil
}

func (d *EmulatedDriver) Cancel(target graph.ClusterGraph, handle string) error {
	d.em.cancel(handle)
	return nil
}

func (d *EmulatedDriver) Logs(target graph.ClusterGraph, handle string) (string, error) {
	return d.em.logs(handle), nil
}
