"""Turn a chosen container's manifest facts + the fleet vocabulary into a
jobspec's `requires`. Two kinds of dimension:

  FACTS stamped by the harness (un-pollutable), read from the manifest:
    - architecture: the manifest `arch` (amd64 / arm64)
    - gpu vendor:   from capability.accelerator (cuda -> nvidia, rocm/hip -> amd)

  JUDGMENT flagged by the agent, validated here against the vocabulary:
    - network: which fabric(s) the build wants (capability.fabric_* is unreliable
               in current manifests, so the agent reads the tag/build; we only
               check the values are in the fleet's vocabulary)
    - memory:  the range bucket the agent estimates for this app at this size

A dimension is only emitted when the vocabulary offers it (e.g. no gpu-vendor
dimension on a single-vendor fleet), so the selector requires along exactly the
axes the fleet can discriminate.
"""

from __future__ import annotations

from .jobspec import anyof


def is_gpu(capability: dict) -> bool:
    """GPU-capable per the manifest's structured capability (accelerator set, or
    gpu libraries linked) — not a fragile scan of NEEDED."""
    acc = (capability or {}).get("accelerator") or "none"
    return acc != "none" or bool((capability or {}).get("gpu_libs"))


def gpu_vendor(capability: dict) -> str | None:
    """Vendor as a FACT from the accelerator/gpu libs. cuda -> nvidia;
    rocm/hip -> amd. None if not determinable (presence still matched in
    containment)."""
    acc = ((capability or {}).get("accelerator") or "").lower()
    if acc == "cuda":
        return "nvidia"
    if acc in ("rocm", "hip"):
        return "amd"
    libs = " ".join((capability or {}).get("gpu_libs", [])).lower()
    if "cudart" in libs or "cublas" in libs or "libcuda" in libs:
        return "nvidia"
    if "amdhip" in libs or "rocm" in libs or "hip" in libs:
        return "amd"
    return None


def validate(values: list[str], allowed: list[str]) -> list[str]:
    """Keep only values the vocabulary offers; drop the rest (caller decides
    whether an empty result is an error)."""
    allow = set(allowed or [])
    return [v for v in values if v in allow]


def section(values: list[str]) -> list | None:
    """A requires section from validated values: anyof(...) for several, a single
    type for one, None for none."""
    vals = [v for v in values if v]
    if not vals:
        return None
    return [anyof(vals)] if len(vals) > 1 else [{"type": vals[0]}]


# Launchers the RUNTIME provides. A jobspec declares parallelism in
# tasks[].count; wrapping the command in a launcher double-launches under flux.
LAUNCHERS = ("mpirun", "mpiexec", "srun", "flux", "jsrun", "aprun", "oshrun", "prun")


def launcher_in(command: list[str]) -> str | None:
    """The launcher a command starts with, if any (so it can be rejected)."""
    if not command:
        return None
    head = str(command[0]).rsplit("/", 1)[-1].lower()
    return head if head in LAUNCHERS else None
