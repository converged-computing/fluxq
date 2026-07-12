package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/converged-computing/fleetq/pkg/api"
	"github.com/converged-computing/fleetq/pkg/cluster"
	"github.com/converged-computing/fleetq/pkg/graph"
	"github.com/converged-computing/fleetq/pkg/manager"
	"github.com/converged-computing/fleetq/pkg/policy"
	"github.com/converged-computing/fleetq/pkg/queue"
	"github.com/converged-computing/fleetq/pkg/reconcile"
	"github.com/converged-computing/fleetq/pkg/transform"
)

// runServe starts the fleet: manager loops + HTTP API. It comes up with zero
// clusters unless --fleet preloads a JGF directory. It blocks until interrupted.
func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	queueKind := fs.String("queue", "memory", "queue backend: memory | sqlite | postgres")
	dsn := fs.String("dsn", "", "DB DSN (sqlite file/':memory:' or postgres URL); default per backend / $DATABASE_URL")
	addr := fs.String("addr", ":8080", "HTTP listen address")
	fleetDir := fs.String("fleet", "", "optional JGF directory to preload clusters from")
	transformKind := fs.String("transform", "stub", "artifact transform: stub (deterministic templates) | agent (Claude via $ANTHROPIC_API_KEY)")
	dev := fs.Bool("dev", false, "dev/demo mode: dispatch every registered cluster through the emulator (no real backend, token, or cluster needed)")
	_ = fs.Parse(args)

	logger := log.New(os.Stdout, "", log.Ltime)
	ctx := context.Background()

	// Dispatch is agnostic: emulated by default, REAL per-cluster when a cluster
	// is registered with backend dispatch config (--config k=v). `drivers`
	// serves config-less clusters (emulated); `realDrivers` serves clusters that
	// carry connection metadata, using the backend driver for their manager.
	emuCfg := cluster.EmulatorConfig{Pending: 300 * time.Millisecond, Run: 1200 * time.Millisecond}
	drivers := cluster.NewEmulatedRegistry(emuCfg)
	// Real dispatch: Kubernetes is always available (client-go, pure Go). Flux
	// dispatch links libflux and is compiled in only with -tags fluxcore;
	// otherwise realFluxDriver() is nil and flux stays emulated-only.
	real := []cluster.Driver{cluster.NewK8sDriver()}
	if fd := realFluxDriver(); fd != nil {
		real = append(real, fd)
	}
	realDrivers := cluster.NewRegistry(real...)
	if *dev {
		realDrivers = nil
		logger.Println("dispatch: DEV MODE — all clusters run through the emulator (no real backends)")
	} else {
		logger.Println("dispatch: emulated by default; register a cluster with --config (flux: uri=local|ssh://…) to dispatch for real")
	}

	// Backend selection (see README): memory (default), sqlite, postgres.
	var (
		q         queue.Queue = queue.NewMemory()
		makeRiver func(*manager.Manager) (manager.Dispatcher, func(context.Context) error, error)
	)
	switch *queueKind {
	case "memory":
		logger.Println("queue: in-memory (inline dispatch)")
	case "sqlite":
		d := *dsn
		if d == "" {
			d = "file:fleetq.sqlite3?_txlock=immediate"
		}
		db, err := queue.NewSQLiteDB(d)
		if err != nil {
			return fmt.Errorf("open sqlite %q: %w", d, err)
		}
		sq, err := queue.NewSQLite(ctx, db)
		if err != nil {
			return fmt.Errorf("init sqlite queue: %w", err)
		}
		q = sq
		makeRiver = func(m *manager.Manager) (manager.Dispatcher, func(context.Context) error, error) {
			return manager.NewRiverEngineSQLite(ctx, m, db)
		}
		logger.Printf("queue: river + SQLite (durable, no server) %s", d)
	case "postgres":
		d := *dsn
		if d == "" {
			if d = os.Getenv("DATABASE_URL"); d == "" {
				d = "postgres://fleetq:fleetq@localhost:5432/fleetq"
			}
		}
		pool, err := queue.NewPool(ctx, d)
		if err != nil {
			return fmt.Errorf("connect postgres %q: %w", d, err)
		}
		pg, err := queue.NewPostgres(ctx, pool)
		if err != nil {
			return fmt.Errorf("init postgres queue: %w", err)
		}
		q = pg
		makeRiver = func(m *manager.Manager) (manager.Dispatcher, func(context.Context) error, error) {
			return manager.NewRiverEnginePostgres(ctx, m, pool)
		}
		logger.Println("queue: river + Postgres (durable)")
	default:
		return fmt.Errorf("unknown --queue %q (want memory|sqlite|postgres)", *queueKind)
	}

	m := &manager.Manager{
		Fleet:       graph.NewFleet(), // start EMPTY; clusters register at runtime
		Queue:       q,
		Matcher:     newMatcher(logger, graph.NewFleet()),
		Policy:      policy.Backfill{Depth: 1},
		Trans:       chooseTransform(*transformKind, logger),
		Drivers:     drivers,
		RealDrivers: realDrivers,
		Tick:        300 * time.Millisecond,
		Logger:      logger,
		Reconciler:  reconcile.RepackReconciler{},
	}

	if makeRiver != nil {
		disp, stopRiver, err := makeRiver(m)
		if err != nil {
			return fmt.Errorf("start river engine: %w", err)
		}
		m.Dispatcher = disp
		defer func() { _ = stopRiver(ctx) }()
		logger.Println("dispatch: river workers (retryable)")
	}

	// Optional: preload clusters from a JGF directory.
	if *fleetDir != "" {
		f, err := graph.DirectoryLoader{}.LoadFleet(*fleetDir)
		if err != nil {
			return fmt.Errorf("preload --fleet %s: %w", *fleetDir, err)
		}
		for _, cg := range f.Clusters() {
			if err := m.RegisterCluster(cg); err != nil {
				return fmt.Errorf("register %s: %w", cg.ID, err)
			}
		}
	}
	logger.Printf("fleet ready with %d cluster(s)", m.Fleet.Len())

	stop := make(chan struct{})
	go m.Run(stop)

	srv := &http.Server{Addr: *addr, Handler: api.NewServer(m).Routes()}
	go func() {
		logger.Printf("API listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("http: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Println("shutting down")
	close(stop)
	ctxSh, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctxSh)
	return nil
}

// chooseTransform selects the artifact transformer. "agent" uses Claude (token
// from $ANTHROPIC_API_KEY) and enables the repair stage; "stub" is the
// deterministic default that needs no token.
func chooseTransform(kind string, logger *log.Logger) transform.Transformer {
	if kind == "agent" {
		a := transform.NewAgentTransformer()
		if a.APIKey == "" {
			logger.Println("WARNING: --transform agent but $ANTHROPIC_API_KEY is empty; generations will error")
		} else {
			logger.Printf("transform: Claude agent (model %s)", a.Model)
		}
		return a
	}
	logger.Println("transform: deterministic stub")
	return transform.Stub{}
}
