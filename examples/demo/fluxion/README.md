# fleetq demo (real Fluxion)

A walkthrough of the whole pipeline — **text jobspec → match → dispatch →
monitor → complete** — using the **real Fluxion scheduler** (flux-sched reapi
bindings) for matching and allocation. Dispatch runs through the emulator
(`--dev`), so you get genuine Fluxion placement decisions with simulated
execution: no real cluster or token needed, just the flux-sched libraries.

Every command and output below was run as-is; a turnkey script is in
[`demo.sh`](./demo.sh). For a pure-Go variant that needs no flux-sched
libraries (dev-double matcher), see [`../mock`](../mock).

## Prerequisites

Fluxion links flux-sched via CGO, so this demo needs the flux-sched libraries
and headers. The repository's `.devcontainer` provides them at
`/opt/flux-sched`; run the demo inside it (or set `FLUX_SCHED_ROOT` to your
install). Everything else is pure Go.

## What the demo shows

- Registering clusters of different managers (`flux-operator`,
  `slurm-operator`, `k8s-job`) and attaching capacity (`containment`) and
  capabilities (`software`) as subsystems.
- **Real Fluxion** answering feasibility (`satisfy`) and committing allocations
  (`MatchAllocate` over the containment graph).
- A job flowing to completion.
- The same run on the **durable SQLite** backend (staged river pipeline).
- Optionally, the **Claude agent** transform, if you have a token.

## 0. Build with Fluxion

```bash
make fluxion            # go build -tags fluxion, CGO env from FLUX_SCHED_ROOT
```

This produces `./bin/fleetq`. At **runtime** the reapi shared libraries must be
on the loader path (the server spawns reapi worker subprocesses):

```bash
export LD_LIBRARY_PATH=/opt/flux-sched/resource:/opt/flux-sched/resource/reapi/bindings:/opt/flux-sched/resource/libjobspec:/usr/lib
```

## 1. Start the server (real Fluxion matcher, dev dispatch)

```bash
./bin/fleetq serve --dev --addr :8080
```

```
dispatch: DEV MODE — all clusters run through the emulator (no real backends)
queue: in-memory (inline dispatch)
matcher: REAL Fluxion (flux-sched reapi bindings)
transform: deterministic stub
API listening on :8080
```

That `matcher: REAL Fluxion` line is the point of this demo — matching and
allocation are the actual scheduler, not the dev double. Leave the server
running; use a second terminal for the rest.

## 2. Register a cluster and give it capacity

Registration returns a **secret** that gates edits to that cluster; the CLI
prints an `export` line — copy it.

```bash
./bin/fleetq cluster register --name alpha --manager flux-operator
```

```
registered cluster "alpha" (handle flux-operator://alpha)
secret: e10d6f7c18...      (48 hex chars)
save it for edits:  export FLEETQ_SECRET=e10d6f7c18...
note: the cluster is empty. Add a containment subsystem (JGF) before it can schedule.
```

```bash
export FLEETQ_SECRET=<paste from above>
```

Attach the two subsystems from the bundled quickstart graphs. `containment` is
**countable** (Fluxion allocates it, `--descriptive=false`); `software` is
**descriptive** (satisfy-only capability, `--descriptive=true`):

```bash
./bin/fleetq cluster subsystem register --cluster alpha --name containment \
    --descriptive=false --file data/quickstart/alpha.containment.json

./bin/fleetq cluster subsystem register --cluster alpha --name software \
    --descriptive=true  --file data/quickstart/alpha.software.json
```

```
registered subsystem: {"cluster":"alpha","descriptive":false,"subsystem":"containment"}
registered subsystem: {"cluster":"alpha","descriptive":true,"subsystem":"software"}
```

Fluxion ingests the containment JGF as its resource graph; subsequent matches
traverse that graph.

## 3. See what can run where (satisfy-only, no allocation)

```bash
./bin/fleetq satisfy --file examples/job-lammps.json
```

```
CLUSTER          SCORE   FREE-NOW FREE   MATCHED
alpha            11.50   true     0      software
```

Fluxion reports the job is feasible on alpha (software `lammps` matched, and the
containment graph can hold it). `satisfy` allocates nothing.

## 4. Submit and watch it complete

```bash
./bin/fleetq submit --file examples/job-lammps.json
```

```
{"id":"job-0001"}
```

```bash
./bin/fleetq jobs
```

```
JOBID     NAME        STATE      CLUSTER  HANDLE                     NOTE
job-0001  lammps-job  COMPLETED  alpha    minicluster/flux-sample-1  [emu] MiniCluster COMPLETED; exit 0
```

The server log shows Fluxion committing a real allocation, then the emulated
dispatch/monitor lifecycle:

```
match job-0001 -> cluster alpha (alloc 1)
dispatch job-0001 -> flux-operator:alpha as minicluster/flux-sample-1
status job-0001 -> RUNNING ([emu] MiniCluster created; pods pending)
status job-0001 -> RUNNING ([emu] MiniCluster RUNNING)
status job-0001 -> COMPLETED ([emu] MiniCluster COMPLETED; exit 0)
```

Inspect the full job record (spec, placement, native handle, last note):

```bash
./bin/fleetq job job-0001
```

## 5. Introspection

```bash
./bin/fleetq managers    # which managers dispatch for real vs. emulated
./bin/fleetq jobs        # the "flux jobs" view
```

```
MANAGER          REAL DISPATCH  EMULATED
flux-operator    no             yes
slurm-operator   no             yes
k8s-job          yes            yes
flux-uri         no             yes
```

## 6. The durable backend (staged river pipeline)

Swap the in-memory queue for SQLite; dispatch and monitoring then run as
separate river stages with independent workers and retry — same Fluxion matcher,
now with a durable queue on a single local file:

```bash
./bin/fleetq serve --dev --queue sqlite --dsn "file:demo.sqlite3?_txlock=immediate" --addr :8080
```

```
queue: river + SQLite (durable, no server) file:demo.sqlite3?_txlock=immediate
matcher: REAL Fluxion (flux-sched reapi bindings)
dispatch: river workers (retryable)
```

Register a cluster and submit exactly as before. Job state persists in
`demo.sqlite3`, so an interrupted server resumes in-flight work on restart.

## 7. Optional: the Claude agent transform

Add a token to have an agent generate the native artifact from the jobspec and
the (Fluxion-chosen) cluster, instead of the deterministic templates:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export ANTHROPIC_MODEL=claude-sonnet-4-5   # optional; a model your key can use
./bin/fleetq serve --dev --transform agent --addr :8080
```

```
matcher: REAL Fluxion (flux-sched reapi bindings)
transform: Claude agent (model claude-sonnet-4-5)
```

The agent's output is **not trusted**: it passes a deterministic validator
(parse + policy + capability checks) before the credentialed submit. A rejected
artifact enters the **repair** stage — the agent gets the exact error and the
failed artifact and tries a bounded number of fixes; if it can't, the job fails
*unsatisfied* with the reason attached, and you resubmit.

> This path calls the real Anthropic API (needs the key and network). Without a
> key the server starts but generations error — use the default
> `--transform stub` for the offline demo.

## Going real (dispatch)

Fluxion is already the real scheduler here; to also dispatch for real, drop
`--dev` and register clusters with backend connection config:

- **Kubernetes** (client-go, no extra build): `k8s-job`, register with
  `--config context=<kube-context>` or `--config kubeconfig=<path>`.
- **Flux**: build with `-tags "fluxion fluxcore"` (adds libflux dispatch),
  register `flux-uri` with `--config uri=local` (or `ssh://…`).
- A manager with no real driver stays emulated; register it with
  `--config emulate=true` to keep it satisfy-only.
