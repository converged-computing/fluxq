"""A Flux v1 jobspec, matching fleetq's pkg/jobspec.Jobspec (RFC 25): resources
+ tasks + attributes, image under attributes.user.image, plus the `requires`
subsystem map fleetq matches per-subsystem. Kept next to the Go struct on
purpose — this is the same wire contract, authored from Python.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any


def containment(nodes: int, cores_per_node: int, gpus_per_node: int = 0,
                mem_gb_per_node: int = 0) -> list[dict]:
    """node -> slot(default) -> core[+gpu][+memory], mirroring fleetq Containment()."""
    slot_with: list[dict] = [{"type": "core", "count": cores_per_node}]
    if gpus_per_node > 0:
        slot_with.append({"type": "gpu", "count": gpus_per_node})
    if mem_gb_per_node > 0:
        slot_with.append({"type": "memory", "count": mem_gb_per_node, "unit": "GB"})
    return [{"type": "node", "count": nodes,
             "with": [{"type": "slot", "count": 1, "label": "default", "with": slot_with}]}]


def anyof(types: list[str]) -> dict:
    """An OR entry inside a requires section (fleetq's reserved `anyof`)."""
    return {"type": "anyof", "with": [{"type": t} for t in types]}


def build_jobspec(name: str, image: str, command: list[str], nodes: int = 1,
                  cores_per_node: int = 1, gpus_per_node: int = 0, duration_s: int = 3600,
                  requires: dict | None = None) -> dict:
    system: dict[str, Any] = {"duration": int(duration_s)}
    if name:
        system["job"] = {"name": name}
    js: dict[str, Any] = {
        "version": 1,
        "resources": containment(nodes, cores_per_node, gpus_per_node),
        "tasks": [{"command": command, "slot": "default", "count": {"per_slot": 1}}],
        "attributes": {"system": system, "user": {"image": image}},
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

    def document(self) -> dict:
        return {"jobspec": self.jobspec,
                "provenance": {"application": self.application,
                               "chosen_reference": self.chosen_reference,
                               "reasoning": self.reasoning,
                               "alternatives": self.alternatives}}


def save_jobspecs(jobs: list[SelectedJob], root: str) -> list[str]:
    written = []
    for job in jobs:
        app = re.sub(r"[^A-Za-z0-9._-]", "_", job.application) or "app"
        path = os.path.join(root, app, "jobspec.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(job.document(), f, indent=2, sort_keys=True)
        written.append(path)
    return written
