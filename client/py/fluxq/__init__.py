"""fluxq — Python client for fluxq (ships in the fluxq repo, versioned with it).

Subcommands via `python -m fluxq <cmd>` or dedicated entrypoints. Today: `select`
(author jobspecs by choosing the best container per application, clusters in mind).
"""

from .jobspec import SelectedJob, anyof, build_jobspec, containment, save_jobspecs
from .requires import is_gpu, network_section
from .select import SelectorTask

__all__ = ["SelectorTask", "build_jobspec", "containment", "anyof", "SelectedJob",
           "save_jobspecs", "is_gpu", "network_section"]
