package manager

// River provides the durable, staged pipeline behind the Dispatcher seam. Each
// stage is its own named queue with independent workers and retry, so a backlog
// or retry loop in one stage never holds up another:
//
//   schedule (scheduleOnce) --Allocate--> [dispatch] --success--> [monitor]
//                                              |--repairable--> [repair] --fixed--> [dispatch]
//                                              |--transient---> (river backoff, same queue)
//                                              |--wrong-target-> cancel+free -> schedule
//                                              '--hard-fail----> free + Failed
//
// The per-job worker calls the transport-agnostic handlers in pipeline.go and
// enqueues whatever next stage they return. river needs a SQL database
// (Postgres or SQLite); pure in-memory has no river — that is queue.Memory +
// inline dispatch.

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/converged-computing/fleetq/pkg/queue"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/riverdriver/riversqlite"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

// Stage queue names.
const (
	qDispatch = "dispatch"
	qRepair   = "repair"
	qMonitor  = "monitor"
)

const repairMaxAttempts = 3

// Each stage carries just the fleetq job id (the durable record is in
// fleetq_jobs; handlers reload it). Repair also carries the error to fix.
type dispatchArgs struct {
	JobID string `json:"job_id"`
}
type repairArgs struct {
	JobID   string `json:"job_id"`
	LastErr string `json:"last_err"`
}
type monitorArgs struct {
	JobID string `json:"job_id"`
}

func (dispatchArgs) Kind() string { return "fleetq_dispatch" }
func (repairArgs) Kind() string   { return "fleetq_repair" }
func (monitorArgs) Kind() string  { return "fleetq_monitor" }

type dispatchWorker struct {
	river.WorkerDefaults[dispatchArgs]
	m *Manager
}
type repairWorker struct {
	river.WorkerDefaults[repairArgs]
	m *Manager
}
type monitorWorker struct {
	river.WorkerDefaults[monitorArgs]
	m *Manager
}

func (w *dispatchWorker) Work(ctx context.Context, job *river.Job[dispatchArgs]) error {
	next, retry := w.m.handleDispatch(job.Args.JobID)
	if retry {
		return fmt.Errorf("dispatch transient failure; retrying %s", job.Args.JobID)
	}
	return w.m.enqueueStage(ctx, next, job.Args.JobID)
}

func (w *repairWorker) Work(ctx context.Context, job *river.Job[repairArgs]) error {
	next, retry := w.m.handleRepair(job.Args.JobID, job.Args.LastErr, job.Attempt, job.MaxAttempts)
	if retry {
		return fmt.Errorf("repair not yet resolved; retrying %s", job.Args.JobID)
	}
	return w.m.enqueueStage(ctx, next, job.Args.JobID)
}

func (w *monitorWorker) Work(ctx context.Context, job *river.Job[monitorArgs]) error {
	done, snooze := w.m.handleMonitor(job.Args.JobID)
	if done {
		return nil
	}
	// Re-poll later. ScheduledAt re-insert works on every backend (no reliance
	// on a specific snooze API version).
	_, err := w.m.riverInsert.Insert(ctx, monitorArgs{JobID: job.Args.JobID},
		&river.InsertOpts{Queue: qMonitor, ScheduledAt: time.Now().Add(snooze)})
	return err
}

// enqueueStage inserts the job into the next stage's queue. "" means the job
// reached a terminal or was handed back to scheduling — nothing to enqueue.
func (m *Manager) enqueueStage(ctx context.Context, next, id string) error {
	if next == "" {
		return nil
	}
	if m.riverInsert == nil {
		return fmt.Errorf("no river client wired for stage %q", next)
	}
	var (
		args river.JobArgs
		opts = &river.InsertOpts{Queue: next}
	)
	switch next {
	case qDispatch:
		args = dispatchArgs{JobID: id}
	case qMonitor:
		args = monitorArgs{JobID: id}
	case qRepair:
		note := ""
		if j, ok := m.Queue.Get(id); ok {
			note = j.Note
		}
		args = repairArgs{JobID: id, LastErr: note}
		opts.MaxAttempts = repairMaxAttempts
	default:
		return fmt.Errorf("unknown stage %q", next)
	}
	_, err := m.riverInsert.Insert(ctx, args, opts)
	return err
}

// riverInserter is the database-agnostic slice of *river.Client we use; both
// pgx and sqlite clients satisfy it.
type riverInserter interface {
	Insert(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error)
}

// RiverDispatcher implements Dispatcher by enqueuing onto the dispatch stage.
type RiverDispatcher struct{ ins riverInserter }

func (d *RiverDispatcher) Dispatch(j queue.Job) error {
	_, err := d.ins.Insert(context.Background(), dispatchArgs{JobID: j.ID}, &river.InsertOpts{Queue: qDispatch})
	return err
}

// ownsMonitoring reports that the staged pipeline runs its own monitor stage,
// so the manager skips inline monitoring.
func (d *RiverDispatcher) ownsMonitoring() bool { return true }

func workersFor(m *Manager) *river.Workers {
	ws := river.NewWorkers()
	river.AddWorker(ws, &dispatchWorker{m: m})
	river.AddWorker(ws, &repairWorker{m: m})
	river.AddWorker(ws, &monitorWorker{m: m})
	return ws
}

// Per-stage concurrency. Independent MaxWorkers is the isolation: a repair
// backlog (4) cannot starve dispatch (8).
var riverQueues = map[string]river.QueueConfig{
	qDispatch: {MaxWorkers: 8},
	qRepair:   {MaxWorkers: 4},
	qMonitor:  {MaxWorkers: 4},
}

// NewRiverEnginePostgres wires the staged pipeline over Postgres (pgx).
func NewRiverEnginePostgres(ctx context.Context, m *Manager, pool *pgxpool.Pool) (*RiverDispatcher, func(context.Context) error, error) {
	drv := riverpgxv5.New(pool)
	migrator, err := rivermigrate.New(drv, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("river(pg) migrate init: %w", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return nil, nil, fmt.Errorf("river(pg) migrate up: %w", err)
	}
	client, err := river.NewClient(drv, &river.Config{Queues: riverQueues, Workers: workersFor(m)})
	if err != nil {
		return nil, nil, fmt.Errorf("river(pg) client: %w", err)
	}
	m.riverInsert = client
	if err := client.Start(ctx); err != nil {
		return nil, nil, fmt.Errorf("river(pg) start: %w", err)
	}
	return &RiverDispatcher{ins: client}, client.Stop, nil
}

// NewRiverEngineSQLite wires the staged pipeline over SQLite (file or :memory:).
func NewRiverEngineSQLite(ctx context.Context, m *Manager, db *sql.DB) (*RiverDispatcher, func(context.Context) error, error) {
	drv := riversqlite.New(db)
	migrator, err := rivermigrate.New(drv, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("river(sqlite) migrate init: %w", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return nil, nil, fmt.Errorf("river(sqlite) migrate up: %w", err)
	}
	client, err := river.NewClient(drv, &river.Config{Queues: riverQueues, Workers: workersFor(m)})
	if err != nil {
		return nil, nil, fmt.Errorf("river(sqlite) client: %w", err)
	}
	m.riverInsert = client
	if err := client.Start(ctx); err != nil {
		return nil, nil, fmt.Errorf("river(sqlite) start: %w", err)
	}
	return &RiverDispatcher{ins: client}, client.Stop, nil
}
