import asyncio
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__))))
from fluxq.io import load_manifests
from fluxq.requires import gpu_vendor, is_gpu
from fluxq.select import SelectorTask

VOCAB = {
    "architecture": ["amd64", "arm64"],
    "network": ["efa", "ethernet"],
    "gpu": ["nvidia", "amd"],
    "memory": ["0-64GB", "64-256GB", "256GB+"],
}
REPO = "metric-lammps-gpu"  # catalog is keyed by REPO, not the free-text app name


def _manifest(root, ref, app, arch, capability):
    d = os.path.join(root, *ref.replace(":", "/").split("/"))
    os.makedirs(d, exist_ok=True)
    json.dump(
        {
            "entry": {
                "reproduce": {"reference": ref, "digest": ref + "@x"},
                "artifacts": [
                    {
                        "application": app,
                        "arch": arch,
                        "capability": capability,
                        "needed": [],
                    }
                ],
            }
        },
        open(os.path.join(d, "manifest.json"), "w"),
    )


def test_capability_facts():
    assert gpu_vendor({"accelerator": "cuda"}) == "nvidia"
    assert gpu_vendor({"gpu_libs": ["libamdhip64.so"]}) == "amd"
    assert is_gpu({"accelerator": "cuda"}) and not is_gpu({"accelerator": "none"})
    print("OK capability -> gpu presence + vendor facts")


def test_group_by_repo():
    with tempfile.TemporaryDirectory() as d:
        _manifest(d, f"ghcr.io/cc/{REPO}:a", "LAMMPS", "amd64", {"accelerator": "cuda"})
        _manifest(
            d,
            f"ghcr.io/cc/{REPO}:b",
            "LAMMPS (KOKKOS)",
            "amd64",
            {"accelerator": "cuda"},
        )
        cat = load_manifests(d)
        assert list(cat) == [REPO] and len(cat[REPO]) == 2, cat
    print("OK grouped by repo (two free-text names -> one app, two variants)")


class Author:
    async def converse(self, task):
        return {}

    async def run_agent(self, sp, up, tools, cf):
        t = {x.name: x for x in tools}
        v = json.loads((await t["get_vocabulary"].handler({}))["content"][0]["text"])
        assert v["gpu"] == ["nvidia", "amd"]
        json.loads(
            (await t["get_variants"].handler({"application": REPO}))["content"][0][
                "text"
            ]
        )
        bad = json.loads(
            (
                await t["record_jobspec"].handler(
                    {
                        "application": REPO,
                        "reference": f"ghcr.io/cc/{REPO}:cpu",
                        "command": ["lmp"],
                        "nodes": 1,
                        "needs_gpu": True,
                    }
                )
            )["content"][0]["text"]
        )
        assert "not GPU-capable" in bad["error"], bad
        await t["record_jobspec"].handler(
            {
                "application": REPO,
                "reference": f"ghcr.io/cc/{REPO}:gpu",
                "command": ["lmp", "-in", "in.reaxff"],
                "nodes": 4,
                "needs_gpu": True,
                "network": ["efa", "ethernet"],
                "memory": "64-256GB",
                "reasoning": "cuda; fabric; medium",
            }
        )
        await t["skip_application"].handler(
            {"application": "flux-core", "reason": "infra"}
        )
        return None


def test_end_to_end():
    with tempfile.TemporaryDirectory() as d:
        _manifest(
            d,
            f"ghcr.io/cc/{REPO}:cpu",
            "LAMMPS",
            "amd64",
            {"accelerator": "none", "gpu_libs": []},
        )
        _manifest(
            d,
            f"ghcr.io/cc/{REPO}:gpu",
            "LAMMPS",
            "amd64",
            {"accelerator": "cuda", "gpu_libs": ["libcudart.so.12"]},
        )
        task = SelectorTask()
        jobs = asyncio.run(
            task.execute(
                Author(),
                {
                    "manifests_dir": d,
                    "clusters": [],
                    "vocabulary": VOCAB,
                    "goal": "gpu lammps",
                },
                lambda n, a: True,
            )
        )
        assert len(jobs) == 1 and task._skipped[0]["application"] == "flux-core"
        req = jobs[0].jobspec["requires"]
        assert req["architecture"] == [{"type": "amd64"}]
        assert req["gpu"] == [{"type": "nvidia"}]
        assert req["network"][0]["type"] == "anyof"
        assert req["memory"] == [{"type": "64-256GB"}]
        assert "software" not in req
        print("OK reconciliation: facts stamped, judgment validated, app skipped")


def test_no_launcher_and_ranks_from_tasks_per_node():
    from fluxq.jobspec import build_jobspec
    from fluxq.requires import launcher_in

    assert launcher_in(["mpirun", "-np", "12", "lmp"]) == "mpirun"
    assert launcher_in(["/usr/bin/srun", "lmp"]) == "srun"
    assert launcher_in(["lmp", "-in", "in.lj"]) is None
    js = build_jobspec(name="x", image="img", command=["lmp", "-in", "in.lj"], nodes=3)
    slot = js["resources"][0]
    # slot ABOVE node: one task per whole node
    assert slot["type"] == "slot" and slot["count"] == 3 and slot["label"] == "default"
    node = slot["with"][0]
    assert node["type"] == "node" and node["count"] == 1 and node["exclusive"] is True
    # NO cores anywhere — the cluster is not chosen, so core counts are unknowable
    assert "with" not in node, node
    assert js["tasks"][0]["command"][0] == "lmp", "no launcher in the command"
    # gpu is presence, not a count
    g = build_jobspec(name="x", image="img", command=["a"], nodes=1, needs_gpu=True)
    assert g["resources"][0]["with"][0]["with"] == [{"type": "gpu", "count": 1}]
    print("OK whole-node request; no core/rank guessing; gpu as presence")


if __name__ == "__main__":
    test_capability_facts()
    test_no_launcher_and_ranks_from_tasks_per_node()
    test_group_by_repo()
    test_end_to_end()
    print("\nall selector tests passed")
