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
    """Whole-node containment: one slot per node, each holding an exclusive node.

    Nothing below the node is specified. The selector chooses a container and a
    node count; it cannot know a node's cores because the cluster is not chosen.
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
        # Context for the transform. Flux does not interpret attributes.user.
        user["container"] = container
    js: dict[str, Any] = {
        "version": 1,
        "resources": containment(nodes, needs_gpu),
        # One task per node slot; the transform expands ranks to the real cores.
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
        """How this jobspec was authored, written beside it rather than in it."""
        return {
            "application": self.application,
            "chosen_reference": self.chosen_reference,
            "reasoning": self.reasoning,
            "alternatives": self.alternatives,
        }


def save_jobspecs(jobs: list[SelectedJob], root: str) -> list[str]:
    """Write each selection as jobspec.json plus provenance.json.

    The jobspec carries nothing but the job. Deployment metadata the transform
    needs travels in attributes.user, which Flux leaves uninterpreted.
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
