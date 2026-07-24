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
        for art in entry.get("artifacts", []):
            app = art.get("application") or "unknown"
            catalog.setdefault(app, []).append({
                "reference": repro.get("reference", ""),
                "digest": repro.get("digest", ""),
                "arch": art.get("arch", ""),
                "needed": art.get("needed", []),
                "provenance": art.get("provenance", {}),
            })
    return catalog


def load_clusters(source: Any) -> list[dict]:
    """A path to JSON, a fleetq base URL (GET /v1/clusters), a raw list, or
    {'clusters': [...]}. Includes each cluster's `capabilities` (needs the
    infoOf capabilities field)."""
    if isinstance(source, str) and source.startswith(("http://", "https://")):
        url = source.rstrip("/") + "/v1/clusters"
        with urllib.request.urlopen(url) as r:  # nosec - operator-provided fleetq URL
            source = json.loads(r.read().decode())
    elif isinstance(source, str):
        source = read_json(source)
    if isinstance(source, dict):
        source = source.get("clusters", [])
    return source or []
