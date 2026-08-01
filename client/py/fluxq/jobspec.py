"""A Flux v1 jobspec, matching fluxq's pkg/jobspec.Jobspec (RFC 25): resources
+ tasks + attributes, image under attributes.user.image, plus the `requires`
subsystem map fluxq matches per-subsystem. Kept next to the Go struct on
purpose — this is the same wire contract, authored from Python.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any


def containment(nodes: int, needs_gpu: bool = False) -> list[dict]:
    """Whole-node containment: one slot per node, each slot holding an exclusive node.

        slot(count=N, label=default) -> node(count=1, exclusive)

    The slot sits ABOVE the node because a slot is the unit a task runs in, and
    here one task gets one whole node. Nothing below the node is specified: the
    selector chooses a CONTAINER and a NODE COUNT, and cannot know a node's cores
    or GPUs because the cluster is not chosen yet. The transform derives ranks
    from whatever cluster the job matches.

    A GPU need is PRESENCE (1 gpu on the node), not a count, for the same reason.
    """
    node: dict = {"type": "node", "count": 1, "exclusive": True}
    if needs_gpu:
        node["with"] = [{"type": "gpu", "count": 1}]
    return [{"type": "slot", "count": nodes, "label": "default", "with": [node]}]


def anyof(types: list[str]) -> dict:
    """An OR entry inside a requires section (fluxq's reserved `anyof`)."""
    return {"type": "anyof", "with": [{"type": t} for t in types]}


def build_jobspec(
    name: str,
    image: str,
    command: list[str],
    nodes: int = 1,
    needs_gpu: bool = False,
    duration_s: int = 3600,
    requires: dict | None = None,
    container: dict | None = None,
) -> dict:
    system: dict[str, Any] = {"duration": int(duration_s)}
    if name:
        system["job"] = {"name": name}
    user: dict[str, Any] = {"image": image}
    if container:
        # Free-form context for the transform agent (mpi flavor, arch, accelerator).
        # Flux does not interpret attributes.user, so none of this affects matching.
        user["container"] = container
    js: dict[str, Any] = {
        "version": 1,
        "resources": containment(nodes, needs_gpu),
        # One task per node slot. The transform expands ranks to the chosen
        # cluster's actual cores; the command carries no launcher.
        "tasks": [{"command": command, "slot": "default", "count": {"per_slot": 1}}],
        "attributes": {"system": system, "user": user},
    }
    if requires:
        js["requires"] = requires
    return js


@dataclass
class SelectedJob:
    application: str
    jobspec: dict
    chosen_reference: str
    reasoning: str = ""
    alternatives: list[str] = field(default_factory=list)

    def provenance(self) -> dict:
        """How this jobspec was authored. Audit data written beside the jobspec,
        never inside it."""
        return {
            "application": self.application,
            "chosen_reference": self.chosen_reference,
            "reasoning": self.reasoning,
            "alternatives": self.alternatives,
        }


def save_jobspecs(jobs: list[SelectedJob], root: str) -> list[str]:
    """Write each selection as TWO files:

      jobspec.json     the jobspec and nothing else — submit it as-is
      provenance.json  how it was authored (chosen container, alternatives,
                       reasoning); audit data, never part of the jobspec

    Deployment metadata the transform needs (the container image, and anything
    added later) travels inside the jobspec's `attributes.user`, which Flux
    defines as free-form and the matcher ignores — so the jobspec stays purely
    about scheduling while still carrying what a manifest needs.
    """
    written = []
    for job in jobs:
        app = re.sub(r"[^A-Za-z0-9._-]", "_", job.application) or "app"
        d = os.path.join(root, app)
        os.makedirs(d, exist_ok=True)

        path = os.path.join(d, "jobspec.json")
        with open(path, "w") as f:
            json.dump(job.jobspec, f, indent=2, sort_keys=True)
        written.append(path)

        with open(os.path.join(d, "provenance.json"), "w") as f:
            json.dump(job.provenance(), f, indent=2, sort_keys=True)
    return written
