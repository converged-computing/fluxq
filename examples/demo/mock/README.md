# fleetq demo

## 0. Build

```bash
go build -o fleetq ./cmd/fleetq
```

## 1. Start the server (in-memory, dev dispatch)

```bash
./fleetq serve --dev --addr :8080
```

Leave it running; use a second terminal for the rest. All client commands take
`--server http://localhost:8080` (the default).

## 2. Register a cluster and give it capacity

Registering a cluster returns a **secret** that gates edits to that cluster.
The CLI prints an `export` line — copy it.

```bash
./fleetq cluster register --name alpha --manager flux-operator
```
```bash
export FLEETQ_SECRET=<paste from above>
```

Attach the two subsystems from the bundled quickstart graphs. `containment` is
**countable** (allocated capacity, so `--descriptive=false`); `software` is
**descriptive** (satisfy-only capability, `--descriptive=true`):

```bash
./fleetq cluster subsystem register --cluster alpha --name containment --descriptive=false --file data/quickstart/alpha.containment.json
./fleetq cluster subsystem register --cluster alpha --name software --descriptive=true  --file data/quickstart/alpha.software.json
```

## 3. See what can run where (no allocation)

`satisfy` runs the matcher satisfy-only across the fleet — feasibility and free
capacity, allocating nothing:

```bash
./fleetq satisfy --file examples/job-lammps.json
```

## 4. Submit and watch it complete

```bash
./fleetq submit --file examples/job-lammps.json
```

After a couple of seconds:

```bash
./fleetq jobs
```

```
JOBID     NAME        STATE      CLUSTER  HANDLE                     NOTE
job-0001  lammps-job  COMPLETED  alpha    minicluster/flux-sample-1  [emu] MiniCluster COMPLETED; exit 0
```

The server log shows the full lifecycle — match, dispatch, monitor, free:

```
match job-0001 -> cluster alpha (alloc alloc-1)
dispatch job-0001 -> flux-operator:alpha as minicluster/flux-sample-1
status job-0001 -> RUNNING ([emu] MiniCluster created; pods pending)
status job-0001 -> RUNNING ([emu] MiniCluster RUNNING)
status job-0001 -> COMPLETED ([emu] MiniCluster COMPLETED; exit 0)
free job-0001 (alloc alloc-1) back to cluster alpha
```

Inspect one job's full record (the "receipt spine" — spec, placement, native
handle, last note):

```bash
./fleetq job job-0001
```

## 5. Introspection

```bash
./fleetq managers    # which managers can dispatch for real vs. emulated
./fleetq jobs        # the "flux jobs" view
```

## 6. The durable backend (staged river pipeline)

Swap the in-memory queue for SQLite. Dispatch and monitoring then run as
**separate river stages** with independent workers and retry — the same pipeline
that backs Postgres in production, on a single local file, no server:

```bash
./fleetq serve --dev --queue sqlite --dsn "file:demo.sqlite3?_txlock=immediate" --addr :8080
```

Register a cluster and submit exactly as before (e.g. `gamma` is `k8s-job` and
advertises `gromacs`):

```bash
./fleetq cluster register --name gamma --manager k8s-job
./fleetq cluster subsystem register --cluster gamma --name containment --descriptive=false --file data/quickstart/gamma.containment.json
./fleetq cluster subsystem register --cluster gamma --name software --descriptive=true  --file data/quickstart/gamma.software.json
./fleetq submit --file examples/job-gromacs.json
```

The job state is durable: `demo.sqlite3` persists the queue and the river stages,
so an interrupted server resumes in-flight work on restart instead of losing it.

## 7. Optional: the Claude agent transform

By default the transform is deterministic templates (`--transform stub`). With a
Claude token you can instead have an agent generate the native artifact from the
jobspec and the chosen cluster:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export ANTHROPIC_MODEL=claude-sonnet-4-5
./fleetq serve --dev --transform agent --addr :8080
```

```
transform: Claude agent (model claude-sonnet-4-5)
```

Then register a cluster and submit as in steps 2–4. On each dispatch the agent
turns the jobspec into a manifest (or a flux jobspec), and — this is the point —
the output is **not trusted**: it passes a deterministic validator (parse +
policy + capability checks) before the credentialed submit. If the artifact is
rejected, the job enters the **repair** stage, where the agent gets the exact
error and the failed artifact and tries a bounded number of fixes; if it can't,
the job fails as *unsatisfied* with the reason attached, and you resubmit.

> This path calls the real Anthropic API (needs the key and network). Without a
> key the server still starts but generations error — use `--transform stub` for
> the offline demo. A server-side `apply --dry-run=server` is the second half of
> the validator and engages when you point a real Kubernetes cluster at it
> (drop `--dev`, register `k8s-job` with `--config context=…|kubeconfig=…`).

## Going real

Drop `--dev` and register clusters with backend connection config. TODO.
