"""fluxq — Python client. A subcommand dispatcher; `select` authors jobspecs by
choosing the best container per application. Structured so more subcommands
(submit, clusters, ...) slot in as the client grows.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys

import anyio
from behalf import make_runner, run_task

from .jobspec import save_jobspecs
from .select import SelectorTask

# backend -> the SDK module that must be importable to use it.
_BACKEND_SDK = {"claude": "claude_agent_sdk", "aws": "strands", "gemini": "google.adk"}
_BACKEND_ORDER = ["claude", "aws", "gemini"]


def _installed(module: str) -> bool:
    # find_spec raises (not returns None) for a dotted name whose parent package
    # is missing, e.g. "google.adk" when google isn't installed at all.
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


def available_backends() -> list[str]:
    return [b for b in _BACKEND_ORDER if _installed(_BACKEND_SDK[b])]


def resolve_backend(explicit: str | None) -> str:
    """Use the requested backend, or the first one whose SDK is installed. No
    hard default on claude — if you installed [aws], that's what you get."""
    if explicit:
        return explicit
    found = available_backends()
    if not found:
        raise SystemExit(
            "no agent backend installed — pip install fluxq-client[aws] "
            "(or [claude] / [gemini])"
        )
    return found[0]


def _add_select(sub) -> None:
    s = sub.add_parser(
        "select", help="choose the best container per app and author jobspecs"
    )
    s.add_argument(
        "--backend",
        choices=list(_BACKEND_SDK),
        default=None,
        help="agent backend (default: the first installed one)",
    )
    s.add_argument("--model", default=None, help="model name for the chosen backend")
    s.add_argument(
        "--manifest", help="saved run manifest JSON (skips the conversation)"
    )
    s.add_argument(
        "--manifests-dir", default="manifests", help="root of the manifest tree"
    )
    s.add_argument(
        "--clusters", help="clusters JSON file OR a fluxq base URL (GET /v1/clusters)"
    )
    s.add_argument("--goal", default="Choose the best container per application.")
    s.add_argument(
        "--out-dir", default="jobspecs", help="where to write the jobspec tree"
    )
    s.add_argument(
        "--duration", type=int, default=3600, help="default job duration (seconds)"
    )
    s.set_defaults(func=cmd_select)


def cmd_select(args) -> None:
    backend = resolve_backend(args.backend)
    manifest = (
        json.load(open(args.manifest))
        if args.manifest
        else {
            "manifests_dir": args.manifests_dir,
            "clusters": args.clusters,
            "goal": args.goal,
            "out_dir": args.out_dir,
            "duration_s": args.duration,
        }
    )
    outcome = anyio.run(
        run_task, SelectorTask(), make_runner(backend, args.model), manifest
    )
    paths = save_jobspecs(outcome.result or [], args.out_dir)
    print(f"\nwrote {len(paths)} jobspec(s) under {args.out_dir}/ (backend: {backend})")
    for path in paths:
        print(f"  {path}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fluxq", description="fluxq Python client")
    sub = p.add_subparsers(dest="cmd", required=True)
    _add_select(sub)
    return p


def main(argv=None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


def select_entry(argv=None) -> None:
    """Console-script entry `fluxq-select` — the select subcommand directly."""
    main(["select", *(sys.argv[1:] if argv is None else argv)])


if __name__ == "__main__":
    main()
