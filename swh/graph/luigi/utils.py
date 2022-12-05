# Copyright (C) 2022 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
from typing import Dict


def run_script(script: str, output_path: Path) -> None:
    import os
    import subprocess

    from ..config import check_config

    conf: Dict = {}  # TODO: configurable

    conf = check_config(conf)
    env = {
        **os.environ.copy(),
        "JAVA_TOOL_OPTIONS": conf["java_tool_options"],
        "CLASSPATH": conf["classpath"],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_output_path = Path(f"{output_path}.tmp")

    subprocess.run(
        ["bash", "-c", f"{script.strip()} > {tmp_output_path}"], env=env, check=True
    )

    # Atomically write the output file
    tmp_output_path.replace(output_path)
