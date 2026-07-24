"""What the jobspec must require FROM THE CLUSTER — never what the container
already provides.

The application and its libraries live in the image, so the app is not a cluster
capability and is never required. What we do require is hardware the cluster must
provide: GPUs (as countable containment resources) and a compatible network
fabric (as a requires.network subsystem, matched per-subsystem by fleetq).

The interconnect a build targets is usually NOT visible in its ELF NEEDED list
(MPI dlopen's the provider at runtime), so the fabric requirement is flagged by
the agent from the container's tag/provenance and validated here against the
network vocabulary your clusters register.
"""

from __future__ import annotations

from .jobspec import anyof

# GPU linkage => GPU-capable (requested as countable containment gpu vertices).
GPU_MARKERS = ("libcudart", "libcuda", "libamdhip", "libhip", "librocm")

# Interconnect capabilities the cluster `network` subsystem advertises. Must
# match your fleetq registration (datagen: efa / infiniband / ethernet).
KNOWN_NETWORKS = ("efa", "infiniband", "ethernet")


def is_gpu(needed: list[str]) -> bool:
    j = [n.lower() for n in needed]
    return any(any(m in n for m in GPU_MARKERS) for n in j)


def unknown_networks(options: list[str]) -> list[str]:
    return [o for o in options if o not in KNOWN_NETWORKS]


def network_section(options: list[str]) -> list | None:
    """A requires.network section from validated interconnect capabilities:
    anyof(...) when several are acceptable (let fleetq pick), else a single type."""
    opts = [o for o in options if o]
    if not opts:
        return None
    return [anyof(opts)] if len(opts) > 1 else [{"type": opts[0]}]
