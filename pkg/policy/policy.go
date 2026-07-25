// Package policy is the pluggable ordering discipline — the analog of Flux
// qmanager's queue policies. Since placement is plain MatchAllocate with
// stay-queued-if-full (no speculative reservation), a policy now only decides
// (a) the order jobs are tried and (b) whether an unplaceable job blocks the
// ones behind it (FCFS) or lets them backfill ahead (Backfill).
package policy

import "github.com/converged-computing/fluxq/pkg/queue"

// Policy orders the provisional queue and says whether the head blocks.
type Policy interface {
	Name() string
	// Order returns the provisional jobs in the order to try (input is
	// oldest-first). Most policies keep submit order.
	Order(provisional []queue.Job) []queue.Job
	// HeadOfLineBlock: if true, the first job that cannot be placed this pass
	// stops the pass (strict FCFS). If false, later jobs may still place
	// (backfill).
	HeadOfLineBlock() bool
}

// FCFS: strict submit order, head-of-line blocking.
type FCFS struct{}

func (FCFS) Name() string                    { return "fcfs" }
func (FCFS) Order(p []queue.Job) []queue.Job { return p }
func (FCFS) HeadOfLineBlock() bool           { return true }

// Backfill: submit order, but a blocked job does not stop smaller jobs behind
// it from running. (Reservation-based starvation protection is deferred: with
// plain MatchAllocate we do not speculatively reserve; a full cluster simply
// keeps the job queued.)
type Backfill struct {
	Depth int // retained for compatibility; unused without reservations
}

func (Backfill) Name() string                    { return "backfill" }
func (Backfill) Order(p []queue.Job) []queue.Job { return p }
func (Backfill) HeadOfLineBlock() bool           { return false }
