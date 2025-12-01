# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from pathlib import Path
from typing import Any, Optional

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import psutil

logger = logging.getLogger(__name__)


def check_config(
    conf: dict[str, Any], base_rust_executable_dir: Optional[Path] = None
) -> dict[str, Any]:
    """Check configuration and propagate defaults.

    Arguments:
        base_rust_executable_dir: path to the directory that contains the local project's
            Rust build artifact, ie. :file:`target/`."""

    conf = conf.copy()
    if "batch_size" not in conf:
        # Use 0.1% of the RAM as a batch size:
        # ~1 billion for big servers, ~10 million for small desktop machines
        conf["batch_size"] = min(int(psutil.virtual_memory().total / 1000), 2**30 - 1)
        logger.debug("batch_size not configured, defaulting to %s", conf["batch_size"])
    if "llp_gammas" not in conf:
        conf["llp_gammas"] = "-1,-2,-3,-4"
        logger.debug("llp_gammas not configured, defaulting to %s", conf["llp_gammas"])
    # rust related config entries
    debug_mode = (
        os.environ.get("PYTEST_VERSION") is not None
        or conf.get("profile") == "debug"
        or (
            "rust_executable_dir" in conf
            and Path(conf["rust_executable_dir"]).name == "debug"
        )
    )
    if "profile" not in conf:
        conf["profile"] = "debug" if debug_mode else "release"
    if "rust_executable_dir" not in conf:
        # look for a target/ directory in the sources root directory

        if base_rust_executable_dir is None:
            # in editable installs, __file__ is a symlink to the original file in
            # the source directory, which is where in the end the rust sources and
            # executable are. So resolve the symlink before looking for the target/
            # directory relative to the actual python file.
            path = Path(__file__).resolve()
            base_rust_executable_dir = path.parent.parent.parent / "target"
        conf["rust_executable_dir"] = str(base_rust_executable_dir / conf["profile"])
    if not conf["rust_executable_dir"].endswith("/"):
        conf["rust_executable_dir"] += "/"

    if "object_types" not in conf:
        conf["object_types"] = "*"

    return conf


def check_config_compress(
    config,
    graph_name,
    in_dir,
    out_dir,
    sensitive_in_dir,
    sensitive_out_dir,
    check_flavor,
):
    """check compression-specific configuration and initialize its execution
    environment.
    """
    conf = check_config(config)

    def _retrieve_value(value, name, default_value=None, is_path=False):
        if value is not None:
            value = str(value) if isinstance(value, Path) else value
        elif name in conf:
            value = conf[name]
        elif default_value is not None:
            value = (
                str(default_value) if isinstance(default_value, Path) else default_value
            )
        else:
            raise ValueError(f"No {name} provided.")
        conf[name] = value
        return Path(value) if is_path else value

    graph_name = _retrieve_value(graph_name, "graph_name")
    in_dir = _retrieve_value(in_dir, "in_dir", is_path=True)
    out_dir = _retrieve_value(out_dir, "out_dir", is_path=True)
    check_flavor = _retrieve_value(check_flavor, "check_flavor")

    out_dir.mkdir(parents=True, exist_ok=True)
    if sensitive_in_dir is not None:
        conf["sensitive_in_dir"] = str(sensitive_in_dir)
    if sensitive_out_dir is not None:
        Path(sensitive_out_dir).mkdir(parents=True, exist_ok=True)
        conf["sensitive_out_dir"] = str(sensitive_out_dir)

    if "tmp_dir" not in conf:
        tmp_dir = out_dir / "tmp"
        conf["tmp_dir"] = str(tmp_dir)
    else:
        tmp_dir = Path(conf["tmp_dir"])
    tmp_dir.mkdir(parents=True, exist_ok=True)

    if check_flavor is None:
        check_flavor = conf.get("check_flavor", "full")
    conf["check_flavor"] = check_flavor

    if conf["check_flavor"] not in [
        "full",
        "history_hosting",
        "staging",
        "example",
        "none",
    ]:
        raise ValueError(
            f"Unsupported check flavor: {check_flavor}."
            "Must be one of full, history_hosting, staging, example or none."
        )

    return conf
