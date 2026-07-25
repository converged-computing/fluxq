package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the durable, default Queue implementation. It stores each Job as
// a row (full record as JSONB plus the columns the loops filter on) so state
// survives restarts. It pairs with the river dispatcher (see pkg/manager): the
// queue is the durable provisional/receipt store; river owns durable, retried
// dispatch. This is the fluxnetes pattern — a Postgres-backed provisional set
// that the policy reads from.
type Postgres struct {
	pool *pgxpool.Pool
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS fluxq_jobs (
    id           text PRIMARY KEY,
    state        text NOT NULL,
    submitted_at timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    data         jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS fluxq_jobs_state_idx ON fluxq_jobs (state, submitted_at);
`

// NewPostgres connects a pool and ensures the schema exists. The pool is shared
// with river (same database), so one connection config drives both.
func NewPostgres(ctx context.Context, pool *pgxpool.Pool) (*Postgres, error) {
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		return nil, fmt.Errorf("create fluxq schema: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

// Pool exposes the underlying pool (river shares it).
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

func (p *Postgres) upsert(ctx context.Context, j Job) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	_, err = p.pool.Exec(ctx, `
        INSERT INTO fluxq_jobs (id, state, submitted_at, updated_at, data)
        VALUES ($1, $2, $3, now(), $4)
        ON CONFLICT (id) DO UPDATE
          SET state = EXCLUDED.state, updated_at = now(), data = EXCLUDED.data`,
		j.ID, string(j.State), j.SubmittedAt, b)
	return err
}

func (p *Postgres) Enqueue(j Job) error {
	return p.upsert(context.Background(), j)
}

func (p *Postgres) Update(j Job) error {
	return p.upsert(context.Background(), j)
}

func (p *Postgres) Get(id string) (Job, bool) {
	var b []byte
	err := p.pool.QueryRow(context.Background(),
		`SELECT data FROM fluxq_jobs WHERE id = $1`, id).Scan(&b)
	if err != nil {
		return Job{}, false
	}
	var j Job
	if json.Unmarshal(b, &j) != nil {
		return Job{}, false
	}
	return j, true
}

func (p *Postgres) query(where string, args ...any) []Job {
	rows, err := p.pool.Query(context.Background(),
		`SELECT data FROM fluxq_jobs `+where+` ORDER BY submitted_at`, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var b []byte
		if rows.Scan(&b) != nil {
			continue
		}
		var j Job
		if json.Unmarshal(b, &j) == nil {
			out = append(out, j)
		}
	}
	return out
}

func (p *Postgres) Provisional() []Job {
	return p.query(`WHERE state = $1`, string(Submitted))
}

func (p *Postgres) Active() []Job {
	return p.query(`WHERE state = ANY($1)`,
		[]string{string(Matched), string(Dispatching), string(Running)})
}

func (p *Postgres) All() []Job { return p.query(``) }

// compile-time check
var _ Queue = (*Postgres)(nil)

// helper so callers can build a pool from a DSN without importing pgxpool.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	// keep a modest pool; river also opens connections on the same DB.
	return pgxpool.NewWithConfig(ctx, cfg)
}

// silence unused import if pgx types are needed later
var _ = pgx.ErrNoRows
