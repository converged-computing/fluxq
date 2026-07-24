# fleetq (Python client)

The Python client for fleetq, shipped in the fleetq repo and versioned with it.
A subcommand dispatcher — room to grow (submit, clusters, …) — with `select`
as the first subcommand.

- `python -m fleetq select …`  (or the `fleetq-select` entrypoint)

`select` reads the manifest tree (raw container facts) + a cluster snapshot, lets
an agent choose the best container per application (clusters in mind), and writes
one jobspec per app — containment resources + `requires` (software/network)
derived deterministically from the chosen container's linked libraries. It
chooses a container, not a cluster, and does not submit.

```bash
pip install ./client/py[aws]

# offline snapshot:
curl -s http://localhost:8080/v1/clusters > clusters.json
fleetq-select --backend aws --model us.anthropic.claude-sonnet-5 \
  --manifests-dir manifests --clusters clusters.json \
  --goal "run LAMMPS efficiently, GPU where available" --out-dir jobspecs

# or live:
fleetq-select --clusters http://localhost:8080 --manifests-dir manifests
```

GPUs are requested as countable containment; interchangeable interconnects use a
`requires` `anyof`, matched per-subsystem by fleetq.

SPDX-License-Identifier: MIT
