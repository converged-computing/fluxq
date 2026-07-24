#!/usr/bin/env bash
# Self-contained fleetq demo using the REAL Fluxion scheduler (flux-sched reapi)
# for matching, with emulated dispatch (--dev) so no real cluster/token is
# needed. Requires the flux-sched libraries (the repo .devcontainer provides
# them at /opt/flux-sched). Run from the repo root: examples/demo/demo.sh
set -euo pipefail

FLUX_SCHED_ROOT="${FLUX_SCHED_ROOT:-/opt/flux-sched}"
ADDR=":8080"
SERVER="http://localhost:8080"
QUEUE="${QUEUE:-memory}"       # QUEUE=sqlite for the durable staged pipeline
TRANSFORM="${TRANSFORM:-stub}" # TRANSFORM=agent + $ANTHROPIC_API_KEY for the agent
BIN=./bin/fleetq

echo ">> building fleetq with Fluxion (make fluxion)"
make fluxion FLUX_SCHED_ROOT="$FLUX_SCHED_ROOT"

# reapi shared libs must be on the loader path at runtime (worker subprocesses)
export LD_LIBRARY_PATH="$FLUX_SCHED_ROOT/resource:$FLUX_SCHED_ROOT/resource/reapi/bindings:$FLUX_SCHED_ROOT/resource/libjobspec:/usr/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

ARGS=(serve --dev --transform "$TRANSFORM" --addr "$ADDR")
if [ "$QUEUE" = "sqlite" ]; then
  ARGS+=(--queue sqlite --dsn "file:demo.sqlite3?_txlock=immediate")
fi

echo ">> starting server: fleetq ${ARGS[*]}"
"$BIN" "${ARGS[@]}" >/tmp/fleetq-demo.log 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
sleep 2
grep -i "matcher:" /tmp/fleetq-demo.log || true

echo ">> registering cluster alpha (flux-operator)"
REG=$("$BIN" cluster register --name alpha --manager flux-operator --server "$SERVER")
echo "$REG"
export FLEETQ_SECRET=$(echo "$REG" | awk -F= '/export FLEETQ_SECRET=/{print $2}')

echo ">> attaching containment (countable) + software (descriptive)"
"$BIN" cluster subsystem register --cluster alpha --name containment \
  --descriptive=false --file data/quickstart/alpha.containment.json --server "$SERVER"
"$BIN" cluster subsystem register --cluster alpha --name software \
  --descriptive=true  --file data/quickstart/alpha.software.json --server "$SERVER"

echo ">> satisfy (real Fluxion feasibility, no allocation)"
"$BIN" satisfy --file examples/job-lammps.json --server "$SERVER"

echo ">> submit"
"$BIN" submit --file examples/job-lammps.json --server "$SERVER"

echo ">> waiting for completion"
for _ in $(seq 1 15); do
  sleep 1
  state=$("$BIN" jobs --server "$SERVER" | awk 'NR==2{print $3}')
  { [ "$state" = "COMPLETED" ] || [ "$state" = "FAILED" ]; } && break
done

echo ">> final state"
"$BIN" jobs --server "$SERVER"
echo ">> done (server log: /tmp/fleetq-demo.log)"
