package manager_test

import (
	"context"
	"io"
	"log"
	"path/filepath"
	"testing"
	"time"

	"github.com/converged-computing/fleetq/pkg/cluster"
	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/manager"
	"github.com/converged-computing/fleetq/pkg/matcher"
	"github.com/converged-computing/fleetq/pkg/policy"
	"github.com/converged-computing/fleetq/pkg/queue"
	"github.com/converged-computing/fleetq/pkg/transform"
)

// TestStagedPipelineOverSQLite runs a job end-to-end through the durable river
// staged pipeline (dispatch -> monitor -> terminal) backed by SQLite, proving
// the real transport works — not just the transport-agnostic handlers.
func TestStagedPipelineOverSQLite(t *testing.T) {
	ctx := context.Background()
	dsn := "file:" + filepath.Join(t.TempDir(), "fleetq.sqlite3") + "?_txlock=immediate"

	db, err := queue.NewSQLiteDB(dsn)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	sq, err := queue.NewSQLite(ctx, db)
	if err != nil {
		t.Fatalf("init sqlite queue: %v", err)
	}

	f := loadFleet(t)               // one real cluster "c1" (K8sJob, 4x32, software=lammps)
	cfg := cluster.EmulatorConfig{} // zero timing: completes on first status poll
	m := &manager.Manager{
		Fleet:   f,
		Queue:   sq,
		Matcher: matcher.NewSim(f),
		Policy:  policy.FCFS{},
		Trans:   transform.Stub{},
		Drivers: cluster.NewRegistry(cluster.NewEmulatedDriver(graph.K8sJob, cfg)),
		Tick:    5 * time.Millisecond,
		Logger:  log.New(io.Discard, "", 0),
	}

	disp, stop, err := manager.NewRiverEngineSQLite(ctx, m, db)
	if err != nil {
		t.Fatalf("start river engine: %v", err)
	}
	defer func() { _ = stop(ctx) }()
	m.Dispatcher = disp

	id, err := m.Submit(lammps("j1", 2))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Drive scheduling (which enqueues onto the dispatch stage); river workers
	// carry it through dispatch -> monitor -> terminal asynchronously.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		m.Step() // river path: Step only schedules; monitor is a river stage
		if j, ok := m.Queue.Get(id); ok && j.State.Terminal() {
			if j.State != queue.Completed {
				t.Fatalf("job terminal but not completed: %s (%q)", j.State, j.Note)
			}
			if j.AllocID != "" {
				t.Fatalf("allocation not freed on terminal: %q", j.AllocID)
			}
			return // success: dispatched, ran, monitored to completion, freed
		}
		time.Sleep(50 * time.Millisecond)
	}
	j, _ := m.Queue.Get(id)
	t.Fatalf("job never completed via river+sqlite (last=%s note=%q)", j.State, j.Note)
}
