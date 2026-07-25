package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver (no cgo)
)

// SQLite is the lightweight durable Queue: a local file (or :memory:) with no
// server, paired with river's SQLite driver for dispatch. Same durability
// semantics as Postgres for a file DB, but nothing to run. It implements the
// same queue.Queue interface, so the backend is a drop-in choice.
type SQLite struct {
	db *sql.DB
}

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS fluxq_jobs (
    id           TEXT PRIMARY KEY,
    state        TEXT NOT NULL,
    submitted_at TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    data         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS fluxq_jobs_state_idx ON fluxq_jobs (state, submitted_at);
`

// NewSQLiteDB opens a pure-Go SQLite database. Use a file DSN like
// "file:fluxq.sqlite3?_txlock=immediate" for durability, or ":memory:" for an
// ephemeral store. A single open connection avoids SQLITE_BUSY (river's
// recommendation) and keeps :memory: coherent across goroutines.
func NewSQLiteDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

// NewSQLite ensures the schema and returns the store. The *sql.DB is shared with
// river (same database), so one handle drives both.
func NewSQLite(ctx context.Context, db *sql.DB) (*SQLite, error) {
	if _, err := db.ExecContext(ctx, sqliteSchema); err != nil {
		return nil, fmt.Errorf("create fluxq schema: %w", err)
	}
	return &SQLite{db: db}, nil
}

// DB exposes the handle (river shares it).
func (s *SQLite) DB() *sql.DB { return s.db }

func (s *SQLite) upsert(j Job) error {
	b, err := json.Marshal(j)
	if err != nil {
		return err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = s.db.Exec(`
        INSERT INTO fluxq_jobs (id, state, submitted_at, updated_at, data)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE
          SET state = excluded.state, updated_at = excluded.updated_at, data = excluded.data`,
		j.ID, string(j.State), j.SubmittedAt.UTC().Format(time.RFC3339Nano), now, string(b))
	return err
}

func (s *SQLite) Enqueue(j Job) error { return s.upsert(j) }
func (s *SQLite) Update(j Job) error  { return s.upsert(j) }

func (s *SQLite) Get(id string) (Job, bool) {
	var data string
	err := s.db.QueryRow(`SELECT data FROM fluxq_jobs WHERE id = ?`, id).Scan(&data)
	if err != nil {
		return Job{}, false
	}
	var j Job
	if json.Unmarshal([]byte(data), &j) != nil {
		return Job{}, false
	}
	return j, true
}

func (s *SQLite) query(where string, args ...any) []Job {
	rows, err := s.db.Query(`SELECT data FROM fluxq_jobs `+where+` ORDER BY submitted_at`, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var data string
		if rows.Scan(&data) != nil {
			continue
		}
		var j Job
		if json.Unmarshal([]byte(data), &j) == nil {
			out = append(out, j)
		}
	}
	return out
}

func (s *SQLite) Provisional() []Job { return s.query(`WHERE state = ?`, string(Submitted)) }
func (s *SQLite) Active() []Job {
	return s.query(`WHERE state IN (?, ?, ?)`, string(Matched), string(Dispatching), string(Running))
}
func (s *SQLite) All() []Job { return s.query(``) }

var _ Queue = (*SQLite)(nil)
