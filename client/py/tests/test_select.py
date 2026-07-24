import asyncio
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__))))
from fleetq.io import load_manifests
from fleetq.jobspec import save_jobspecs
from fleetq.requires import is_gpu, network_section, unknown_networks
from fleetq.select import SelectorTask


def _manifest(root, ref, app, needed):
    parts = ref.replace(":", "/").split("/")
    d = os.path.join(root, *parts)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "manifest.json"), "w") as f:
        json.dump({"entry": {"reproduce": {"reference": ref, "digest": ref + "@sha256:x"},
                             "artifacts": [{"application": app, "arch": "amd64", "needed": needed}]}}, f)


def test_network_section_and_validation():
    assert network_section([]) is None                       # portable -> nothing
    assert network_section(["efa"]) == [{"type": "efa"}]      # single
    a = network_section(["efa", "infiniband"])[0]             # several -> anyof
    assert a["type"] == "anyof" and {"type": "efa"} in a["with"] and {"type": "infiniband"} in a["with"]
    assert unknown_networks(["efa", "myrinet"]) == ["myrinet"]
    assert is_gpu(["libcudart.so.12"]) and not is_gpu(["libc.so.6"])
    print("OK network section + validation; gpu detection")


class ChooseLibfabricCPU:
    async def converse(self, task):
        return {}

    async def run_agent(self, system_prompt, user_prompt, tools, confirm_fn):
        t = {x.name: x for x in tools}
        # unknown network must be refused
        bad = json.loads((await t["record_jobspec"].handler(
            {"application": "LAMMPS", "reference": "ghcr.io/cc/metric-lammps-cpu:libfabric-reax",
             "command": ["lmp"], "nodes": 1, "cores_per_node": 8,
             "network": ["myrinet"]}))["content"][0]["text"])
        assert "error" in bad and "unknown network" in bad["error"], bad
        # the libfabric CPU build asks for a fabric (efa OR infiniband), no GPUs
        await t["record_jobspec"].handler(
            {"application": "LAMMPS", "reference": "ghcr.io/cc/metric-lammps-cpu:libfabric-reax",
             "command": ["lmp", "-in", "in.reaxc"], "nodes": 8, "cores_per_node": 32,
             "network": ["efa", "infiniband"],
             "reasoning": "CPU REAX build; libfabric -> needs a fast fabric, efa or infiniband"})
        return None


def test_libfabric_container_requires_a_fabric_not_the_app():
    with tempfile.TemporaryDirectory() as d:
        mdir = os.path.join(d, "manifests")
        _manifest(mdir, "ghcr.io/cc/metric-lammps-cpu:libfabric-reax", "LAMMPS",
                  ["libmpi.so.40", "libc.so.6"])  # note: no libfabric in NEEDED
        clusters = os.path.join(d, "clusters.json")
        with open(clusters, "w") as f:
            json.dump([{"name": "efa-flux", "manager": "flux", "nodes": 16,
                        "capabilities": ["efa"]}], f)
        jobs = asyncio.run(SelectorTask().execute(ChooseLibfabricCPU(),
                    {"manifests_dir": mdir, "clusters": clusters, "goal": "cpu reax"},
                    lambda n, a: True))
        js = jobs[0].jobspec
        req = js["requires"]
        # the useful requirement is present ...
        assert "network" in req and req["network"][0]["type"] == "anyof"
        types = {c["type"] for c in req["network"][0]["with"]}
        assert types == {"efa", "infiniband"}, req
        # ... and the illogical one is gone
        assert "software" not in req, "must NOT require the application (it is in the container)"
        # cpu build -> no gpu in containment
        slot = js["resources"][0]["with"][0]["with"]
        assert all(r["type"] != "gpu" for r in slot)
        print("OK libfabric container requires a fabric (anyof efa/infiniband), not 'lammps'")


if __name__ == "__main__":
    test_network_section_and_validation()
    test_libfabric_container_requires_a_fabric_not_the_app()
    print("\nall fleetq-select tests passed")
