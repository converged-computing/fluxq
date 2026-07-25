# Build/run fluxq. The DEFAULT queue is river + Postgres (durable). Use
# `--memory` (or `make run-memory`) for an offline run with no database.
# `make fluxion` = REAL Fluxion matcher (inside the flux-sched .devcontainer).
FLUX_SCHED_ROOT ?= /opt/flux-sched
REAPI_BINDINGS  ?= github.com/flux-framework/flux-sched/resource/reapi/bindings/go@v0.0.0-20260526195258-f0e815f1f354
DATABASE_URL    ?= postgres://fluxq:fluxq@localhost:5432/fluxq
export DATABASE_URL

BUILDENVVAR = CGO_CFLAGS="-I$(FLUX_SCHED_ROOT)" \
  CGO_LDFLAGS="-L$(FLUX_SCHED_ROOT)/resource -L$(FLUX_SCHED_ROOT)/resource/libjobspec \
    -L$(FLUX_SCHED_ROOT)/resource/reapi/bindings -lresource -ljobspec_conv -lreapi_cli \
    -lflux-idset -lstdc++ -lczmq -ljansson -lhwloc -lboost_system -lflux-hostlist \
    -lboost_graph -lyaml-cpp"

.PHONY: all data test build run run-memory fluxion fluxion-test fluxcore fluxcore-test bench fluxion-bench db-up db-down clean
.DEFAULT_GOAL := build

all:             ## full build: REAL matcher + REAL flux dispatch (-tags fluxion,fluxcore); needs the flux-sched devcontainer
	go get $(REAPI_BINDINGS)
	mkdir -p bin && export $(BUILDENVVAR); go build -tags "fluxion fluxcore" -ldflags '-w' -o bin/fluxq ./cmd/fluxq

data:            ## regenerate the JGF dataset
	go run ./cmd/fluxq-datagen

test:            ## unit + integration tests (use the in-memory queue; offline)
	go test ./...

build:           ## build the demo (dev-double matcher)
	mkdir -p bin && go build -o bin/fluxq ./cmd/fluxq

serve:           ## start the fleet (empty; memory queue). Register clusters via the CLI.
	go run ./cmd/fluxq serve

serve-sqlite:    ## start the fleet on river + SQLite (durable, no server)
	go run ./cmd/fluxq serve --queue sqlite

serve-postgres:  ## start the fleet on river + Postgres (needs `make db-up`)
	go run ./cmd/fluxq serve --queue postgres

fluxion:         ## REAL Fluxion build (inside .devcontainer); still river+Postgres queue
	go get $(REAPI_BINDINGS)
	mkdir -p bin && export $(BUILDENVVAR); go build -tags fluxion -ldflags '-w' -o bin/fluxq ./cmd/fluxq

fluxion-test:    ## tests with the real matcher compiled in
	go get $(REAPI_BINDINGS)
	export $(BUILDENVVAR); go test -tags fluxion ./...

fluxcore:        ## build with REAL libflux dispatch (-tags fluxcore); k8s dispatch already works without it
	mkdir -p bin && go build -tags fluxcore -ldflags '-w' -o bin/fluxq ./cmd/fluxq

fluxcore-test:   ## test libflux dispatch against a live broker (needs flux)
	flux start bash -c 'go test -tags fluxcore -run Flux -v ./pkg/cluster/'

bench:           ## SimMatcher match benchmarks (offline, no toolchain)
	@go test -run x -bench . -benchmem ./pkg/matcher/ > /tmp/fluxq-bench.txt 2>&1 || (cat /tmp/fluxq-bench.txt; exit 1)
	@go run ./cmd/fluxq-benchfmt /tmp/fluxq-bench.txt

fluxion-bench:   ## real Fluxion match benchmarks (inside .devcontainer); compare with `make bench`
	go get $(REAPI_BINDINGS)
	@export $(BUILDENVVAR); go test -tags fluxion -run x -bench . -benchmem ./pkg/matcher/ > /tmp/fluxq-bench.txt 2>&1 || (cat /tmp/fluxq-bench.txt; exit 1)
	@go run ./cmd/fluxq-benchfmt /tmp/fluxq-bench.txt

db-up:           ## initialize + start a local Postgres and create the fluxq db/role
	@PGBIN=$$(ls -d /usr/lib/postgresql/*/bin | head -1); \
	[ -d /var/lib/pgdata/base ] || (sudo mkdir -p /var/lib/pgdata && sudo chown postgres:postgres /var/lib/pgdata && sudo -u postgres "$$PGBIN/initdb" -D /var/lib/pgdata); \
	sudo -u postgres "$$PGBIN/pg_ctl" -D /var/lib/pgdata -l /tmp/pglog start || true; sleep 2; \
	sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='fluxq'" | grep -q 1 || sudo -u postgres psql -c "CREATE ROLE fluxq LOGIN SUPERUSER PASSWORD 'fluxq'"; \
	sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='fluxq'" | grep -q 1 || sudo -u postgres psql -c "CREATE DATABASE fluxq OWNER fluxq"

kind-up:          ## create a local kind cluster (context: kind-fluxq) for k8s dispatch
	kind create cluster --config .devcontainer/kind-config.yaml

kind-down:        ## delete the local kind cluster
	kind delete cluster --name fluxq

db-down:         ## stop the local Postgres
	@PGBIN=$$(ls -d /usr/lib/postgresql/*/bin | head -1); sudo -u postgres "$$PGBIN/pg_ctl" -D /var/lib/pgdata stop || true

clean:
	rm -rf bin
