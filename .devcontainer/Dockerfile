# fluxq devcontainer: real Fluxion (flux-sched at /opt/flux-sched) AND Postgres
# for the default river-backed queue.
FROM vanessa/fluxion-quantum:latest

LABEL maintainer="fluxq"
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000
USER root
RUN (groupadd -g ${USER_GID} ${USERNAME} || true) && \
    (adduser --disabled-password --uid ${USER_UID} --gid ${USER_GID} --gecos "" ${USERNAME} || true) && \
    echo "${USERNAME} ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/${USERNAME}

# Postgres for the default river queue (start with `make db-up`).
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip postgresql postgresql-client && rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/usr/lib:/opt/flux-sched/resource:/opt/flux-sched/resource/reapi/bindings:/opt/flux-sched/resource/libjobspec
ENV DATABASE_URL=postgres://fluxq:fluxq@localhost:5432/fluxq
USER $USERNAME
