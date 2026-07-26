"""SelectorTask: choose the best container per application and author a fluxq
jobspec, reconciling the manifest's terms against the fleet's vocabulary.

The agent is handed (manifests, vocabulary). It chooses a container and supplies
JUDGMENT (command from the container URI, node count, memory bucket, which
fabric); the harness stamps FACTS from the manifest (architecture, gpu vendor,
image) and validates the judgment against the vocabulary. Requires are emitted
only along dimensions the vocabulary offers. It chooses a container, not a
cluster, and does not submit.
"""

from __future__ import annotations

import json
from typing import Any

from behalf import AgentRunner, ConfirmFn, Task, ToolSpec

from .io import load_clusters, load_manifests, load_vocabulary
from .jobspec import SelectedJob, build_jobspec
from .requires import gpu_vendor, is_gpu, section, validate

SELECT = """You turn each profiled application into ONE fluxq jobspec, choosing the
best container and reconciling the manifest against the fleet vocabulary.

First call get_vocabulary — it lists the dimensions the fleet can match on and
the ONLY allowed values for each (architecture, network, gpu, memory ranges).
You must classify into those exact values; a value not in the vocabulary will not
match anything.

For each application: get_variants shows candidate containers with structured
facts — `arch`, and `capability` {accelerator, gpu_libs, fabric_efa/libfabric/
verbs, mpi}. Pick the variant that best serves the goal. Then call record_jobspec
with:
  - reference: the chosen container (from get_variants)
  - command: how to run this app (derive from the container/app — e.g. LAMMPS ->
    lmp -in <deck>; osu -> the benchmark binary). Size the input to the node count.
  - nodes: the target size (given to you below)
  - gpus_per_node: >0 ONLY if the container is GPU-capable (capability.accelerator
    is cuda, or gpu_libs present)
  - network: the acceptable fabric(s) for this build, chosen from the vocabulary's
    network values (a build wanting a fast fabric can list several to OR over; a
    portable build: omit). capability.fabric_* is often unreliable, so read the
    tag/build too.
  - memory: the memory RANGE (from the vocabulary's memory values) you estimate
    this app needs at this size — think about whether it could OOM on a small node.

Do NOT supply architecture or gpu vendor — those are facts stamped from the
manifest. Do NOT require the application itself (it is in the container).

If a container is NOT a real HPC or AI/ML application (e.g. a base image, or an
infrastructure component like flux itself), call skip_application instead of
recording a jobspec."""

SETUP = """You are setting up a selection run. Ask only what you can't infer: the
goal, and optionally which applications to limit to. Then finalize a manifest
with manifests_dir, clusters, vocabulary, goal, out_dir."""


def _text(obj: Any) -> dict:
    return {"content": [{"type": "text", "text": json.dumps(obj, indent=2)}]}


class SelectorTask(Task):
    name = "select"

    def manifest_schema(self) -> dict:
        return {
            "manifests_dir": str,
            "clusters": str,
            "vocabulary": str,
            "goal": str,
            "out_dir": str,
            "duration_s": int,
        }

    def setup_system_prompt(self) -> str:
        return SETUP

    def execute_system_prompt(self, manifest: dict) -> str:
        return SELECT

    def selection_tools(
        self, catalog, clusters, vocab, sink, skipped, manifest
    ) -> list[ToolSpec]:
        async def list_applications(a):
            return _text(
                [{"application": k, "variants": len(v)} for k, v in catalog.items()]
            )

        async def get_variants(a):
            out = []
            for v in catalog.get(a.get("application", ""), []):
                out.append(
                    {
                        "reference": v["reference"],
                        "arch": v.get("arch", ""),
                        "application": v.get("application", ""),
                        "capability": v.get("capability", {}),
                        "provenance": v.get("provenance", {}),
                    }
                )
            return _text(out)

        async def get_vocabulary(a):
            return _text(vocab)

        async def list_clusters(a):
            return _text(clusters)

        async def skip_application(a):
            skipped.append(
                {"application": a.get("application", ""), "reason": a.get("reason", "")}
            )
            return _text(f"skipped {a.get('application','')}: {a.get('reason','')}")

        async def record_jobspec(a):
            app, ref = a["application"], a["reference"]
            variants = {v["reference"]: v for v in catalog.get(app, [])}
            if ref not in variants:
                return _text({"error": f"{ref!r} is not a profiled variant of {app!r}"})
            var = variants[ref]
            cap = var.get("capability", {})
            gpus = int(a.get("gpus_per_node", 0))
            if gpus > 0 and not is_gpu(cap):
                return _text(
                    {
                        "error": f"requested {gpus} gpus but {ref} is not GPU-capable "
                        f"(accelerator={cap.get('accelerator')!r}, no gpu_libs)"
                    }
                )

            requires: dict[str, list] = {}
            # FACTS, stamped by the harness (only where the vocabulary discriminates)
            arch = var.get("arch", "")
            if arch and arch in vocab.get("architecture", []):
                requires["architecture"] = [{"type": arch}]
            if is_gpu(cap):
                vend = gpu_vendor(cap)
                if vend and vend in vocab.get("gpu", []):
                    requires["gpu"] = [{"type": vend}]
            # JUDGMENT, validated against the vocabulary
            net = validate(a.get("network") or [], vocab.get("network", []))
            if net and (sec := section(net)):
                requires["network"] = sec
            mem = a.get("memory")
            if mem:
                if mem not in vocab.get("memory", []):
                    return _text(
                        {
                            "error": f"memory {mem!r} not in vocabulary "
                            f"{vocab.get('memory', [])}"
                        }
                    )
                requires["memory"] = [{"type": mem}]

            js = build_jobspec(
                name=app.lower(),
                image=ref,
                command=a["command"],
                nodes=a.get("nodes", 1),
                cores_per_node=a.get("cores_per_node", 1),
                gpus_per_node=gpus,
                duration_s=a.get("duration_s", manifest.get("duration_s", 3600)),
                requires=requires or None,
            )
            sink.append(
                SelectedJob(
                    application=app,
                    jobspec=js,
                    chosen_reference=ref,
                    reasoning=a.get("reasoning", ""),
                    alternatives=[r for r in variants if r != ref],
                )
            )
            return _text(f"recorded {app} using {ref} (requires={requires})")

        return [
            ToolSpec(
                "list_applications",
                "Profiled applications and variant counts.",
                {},
                list_applications,
            ),
            ToolSpec(
                "get_vocabulary",
                "The fleet's dimensions and the ONLY allowed values for each.",
                {},
                get_vocabulary,
            ),
            ToolSpec(
                "get_variants",
                "Candidate containers for an application with arch + capability facts.",
                {"application": str},
                get_variants,
            ),
            ToolSpec(
                "list_clusters",
                "Available clusters (context; you don't pick one).",
                {},
                list_clusters,
            ),
            ToolSpec(
                "skip_application",
                "Skip a container that is not a real HPC/AI-ML app.",
                {"application": str, "reason": str},
                skip_application,
            ),
            ToolSpec(
                "record_jobspec",
                "Emit one jobspec. reference + command + nodes + gpus_per_node + network (from vocab) "
                "+ memory (a vocab range). Image, architecture and gpu vendor are stamped from the "
                "manifest; never supply them and never require the application.",
                {
                    "application": str,
                    "reference": str,
                    "command": list,
                    "nodes": int,
                    "cores_per_node": int,
                    "gpus_per_node": int,
                    "duration_s": int,
                    "network": list,
                    "memory": str,
                    "reasoning": str,
                },
                record_jobspec,
            ),
        ]

    async def execute(
        self, runner: AgentRunner, manifest: dict, confirm_fn: ConfirmFn
    ) -> list[SelectedJob]:
        catalog = load_manifests(manifest["manifests_dir"])
        clusters = load_clusters(manifest.get("clusters"))
        vocab = (
            load_vocabulary(manifest.get("vocabulary"))
            if manifest.get("vocabulary")
            else {}
        )
        sink: list[SelectedJob] = []
        skipped: list[dict] = []
        tools = self.selection_tools(catalog, clusters, vocab, sink, skipped, manifest)
        await runner.run_agent(
            self.execute_system_prompt(manifest),
            f"Author jobspecs. Goal: {manifest.get('goal', 'run each application well')}",
            tools,
            confirm_fn,
        )
        self._skipped = skipped
        return sink
