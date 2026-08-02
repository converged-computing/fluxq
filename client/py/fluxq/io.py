"""Reading inputs as loose data: the manifest tree (artifact-secretary output)
and the cluster snapshot (a file, or a live GET /v1/clusters). No dependency on
artifact-secretary — manifests are read as JSON."""

from __future__ import annotations

import json
import os
import urllib.request
from typing import Any


def read_json(path: str) -> Any:
    with open(path) as f:
        return json.load(f)


def load_manifests(manifests_dir: str) -> dict[str, list[dict]]:
    """Group profiled builds by application -> [{reference, digest, arch, needed,
    provenance}]. Reads every manifest.json under the tree as data."""
    catalog: dict[str, list[dict]] = {}
    for dirpath, _, files in os.walk(manifests_dir):
        if "manifest.json" not in files:
            continue
        doc = read_json(os.path.join(dirpath, "manifest.json"))
        entry = doc.get("entry", doc)
        repro = entry.get("reproduce", {})
        # The image's libc decides which flux view can be mounted into it, so it
        # travels with the variant rather than staying in the manifest.
        platform = entry.get("platform", {})
        ref = repro.get("reference", "")
        # Group by container REPO (metric-lammps-cpu), NOT the free-text
        # `application` field — that field is inconsistent across a repo's
        # variants (many names for one app), which would split one app into many
        # jobspecs. The free-text name is kept per-variant for the agent's context.
        repo = ref.split("@")[0].rsplit(":", 1)[0].rsplit("/", 1)[-1] or "unknown"
        for art in entry.get("artifacts", []):
            catalog.setdefault(repo, []).append(
                {
                    "reference": ref,
                    "digest": repro.get("digest", ""),
                    "arch": art.get("arch", ""),
                    "application": art.get("application", ""),
                    "capability": art.get("capability", {}),
                    "needed": art.get("needed", []),
                    "provenance": art.get("provenance", {}),
                    "platform": platform,
                }
            )
    return catalog


def load_clusters(source: Any) -> list[dict]:
    """A path to JSON, a fluxq base URL (GET /v1/clusters), a raw list, or
    {'clusters': [...]}. Includes each cluster's `capabilities` (needs the
    infoOf capabilities field)."""
    if isinstance(source, str) and source.startswith(("http://", "https://")):
        url = source.rstrip("/") + "/v1/clusters"
        with urllib.request.urlopen(url) as r:  # nosec - operator-provided fluxq URL
            source = json.loads(r.read().decode())
    elif isinstance(source, str):
        source = read_json(source)
    if isinstance(source, dict):
        source = source.get("clusters", [])
    return source or []


def load_vocabulary(source):
    """The fleet's allowed label set: a file, a fluxq base URL (GET
    /v1/vocabulary), or an already-loaded dict. Returns {dimension: [values]}."""
    if isinstance(source, str) and source.startswith(("http://", "https://")):
        url = source.rstrip("/") + "/v1/vocabulary"
        with urllib.request.urlopen(url) as r:  # nosec - operator-provided fluxq URL
            source = json.loads(r.read().decode())
    elif isinstance(source, str):
        source = read_json(source)
    dims = source.get("dimensions", source) if isinstance(source, dict) else []
    if isinstance(dims, list):  # [{name, values}, ...] -> {name: values}
        return {d["name"]: d.get("values", []) for d in dims}
    return dims or {}
