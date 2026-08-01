# fluxq with the REAL Fluxion matcher.
#
# Same base as .devcontainer: flux-sched lives at /opt/flux-sched, which the
# `fluxion` build tag links against via CGO. Without it the server logs
# "matcher: DEV DOUBLE (not Fluxion)" and is not scheduling for real.
#
#   docker build -t ghcr.io/converged-computing/fluxq:latest .
#   docker run -p 8080:8080 -v $PWD/data:/data ghcr.io/converged-computing/fluxq:latest
#
# The default command uses the sqlite queue (durable, no database server). Pass
# your own args to override, e.g. `--queue postgres --dsn ...`.
FROM vanessa/fluxion-quantum:latest

LABEL org.opencontainers.image.source=https://github.com/converged-computing/fluxq
LABEL org.opencontainers.image.description="fluxq control plane (real Fluxion matcher)"

USER root
ENV PATH=$PATH:/usr/local/go/bin \
    GOFLAGS=-buildvcs=false \
    LD_LIBRARY_PATH=/usr/lib:/opt/flux-sched/resource:/opt/flux-sched/resource/reapi/bindings:/opt/flux-sched/resource/libjobspec

WORKDIR /src
COPY . /src
RUN make fluxion && install -m0755 bin/fluxq /usr/local/bin/fluxq && rm -rf /src/bin

# sqlite job history; bind a host directory to keep it across restarts
VOLUME /data
EXPOSE 8080

ENTRYPOINT ["fluxq"]
CMD ["serve", "--queue", "sqlite", "--dsn", "file:/data/fluxq.sqlite3?_txlock=immediate", "--addr", ":8080"]
