#!/usr/bin/env bash
# Self-contained fleetq demo: no real cluster, no token. Brings up a dev-mode
# server, registers a cluster with capacity + software, and runs a job to
# completion through the emulator. Run from the repo root: examples/demo/demo.sh
set -euo pipefail

ADDR=":8080"
SERVER="http://localhost:8080"
QUEUE="${QUEUE:-memory}"   # set QUEUE=sqlite to use the durable staged pipeline

echo ">> building fleetq"
go build -o ./fleetq ./cmd/fleetq

ARGS=(serve --dev --transform "${TRANSFORM:-stub}" --addr "$ADDR")
if [ "$QUEUE" = "sqlite" ]; then
  ARGS+=(--queue sqlite --dsn "file:demo.sqlite3?_txlock=immediate")
fi

echo ">> starting server: fleetq ${ARGS[*]}"
./fleetq "${ARGS[@]}" >/tmp/fleetq-demo.log 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
sleep 2

echo ">> registering cluster alpha (flux-operator)"
REG=$(./fleetq cluster register --name alpha --manager flux-operator --server "$SERVER")
echo "$REG"
export FLEETQ_SECRET=$(echo "$REG" | awk -F= '/export FLEETQ_SECRET=/{print $2}')

echo ">> attaching containment (countable) + software (descriptive)"
./fleetq cluster subsystem register --cluster alpha --name containment \
  --descriptive=false --file data/quickstart/alpha.containment.json --server "$SERVER"
./fleetq cluster subsystem register --cluster alpha --name software \
  --descriptive=true  --file data/quickstart/alpha.software.json --server "$SERVER"

echo ">> satisfy (feasibility, no allocation)"
./fleetq satisfy --file examples/job-lammps.json --server "$SERVER"

echo ">> submit"
./fleetq submit --file examples/job-lammps.json --server "$SERVER"

echo ">> waiting for completion"
for _ in $(seq 1 15); do
  sleep 1
  line=$(./fleetq jobs --server "$SERVER" | awk 'NR==2')
  state=$(echo "$line" | awk '{print $3}')
  [ "$state" = "COMPLETED" ] || [ "$state" = "FAILED" ] && break
done

echo ">> final state"
./fleetq jobs --server "$SERVER"
echo ">> done (server log: /tmp/fleetq-demo.log)"
