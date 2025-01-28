# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
from pathlib import Path

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import psutil

logger = logging.getLogger(__name__)


def check_config(conf):
    """check configuration and propagate defaults"""
    conf = conf.copy()
    if "batch_size" not in conf:
        # Use 0.1% of the RAM as a batch size:
        # ~1 billion for big servers, ~10 million for small desktop machines
        conf["batch_size"] = min(int(psutil.virtual_memory().total / 1000), 2**30 - 1)
        logger.debug("batch_size not configured, defaulting to %s", conf["batch_size"])
    if "llp_gammas" not in conf:
        conf["llp_gammas"] = "-1,-2,-3,-4,-5,0-0"
        logger.debug("llp_gammas not configured, defaulting to %s", conf["llp_gammas"])
    # rust related config entries
    debug_mode = (
        os.environ.get("PYTEST_VERSION") is not None or conf.get("target") == "debug"
    )
    if debug_mode:
        conf["target"] = "debug"
    if "rust_executable_dir" not in conf:
        # look for a target/ directory in the sources root directory
        profile = "debug" if debug_mode else "release"
        # in editable installs, __file__ is a symlink to the original file in
        # the source directory, which is where in the end the rust sources and
        # executable are. So resolve the symlink before looking for the target/
        # directory relative to the actual python file.
        path = Path(__file__).resolve()
        path = path.parent.parent.parent / "target" / profile
        conf["rust_executable_dir"] = str(path)
    if not conf["rust_executable_dir"].endswith("/"):
        conf["rust_executable_dir"] += "/"

    if "object_types" not in conf:
        conf["object_types"] = "*"

    return conf


def check_config_compress(config, graph_name, in_dir, out_dir, test_flavor):
    """check compression-specific configuration and initialize its execution
    environment.
    """
    conf = check_config(config)

    def _retrieve_value(value, name):
        if value is not None:
            if isinstance(value, Path):
                value = str(value)
            conf[name] = value
        else:
            if name not in conf:
                raise ValueError(f"No {name} provided.")
            else:
                value = conf[name]
        return value

    graph_name = _retrieve_value(graph_name, "graph_name")
    in_dir = Path(_retrieve_value(in_dir, "in_dir"))
    out_dir = Path(_retrieve_value(out_dir, "out_dir"))
    test_flavor = _retrieve_value(test_flavor, "test_flavor")

    out_dir.mkdir(parents=True, exist_ok=True)
    if "tmp_dir" not in conf:
        tmp_dir = out_dir / "tmp"
        conf["tmp_dir"] = str(tmp_dir)
    else:
        tmp_dir = Path(conf["tmp_dir"])
    tmp_dir.mkdir(parents=True, exist_ok=True)

    if test_flavor is None:
        test_flavor = conf.get("test_flavor", "full")
    conf["test_flavor"] = test_flavor

    # NOTE: maybe we should condider using something else
    if conf["test_flavor"] == "full":
        # Parmap's README
        conf["cnt_swhid"] = "swh:1:cnt:43243e2ae91a64e252170cd922718e8c2af323b6"
        # Parmap's root directory
        conf["dir_swhid"] = "swh:1:dir:bc7ddd62cf3d72ffdc365e1bf2dea6eeaa44e185"
        # Parmap's snapshot from November 16, 2024
        conf["snp_swhid"] = "swh:1:snp:8ddca416836fbbc2a7704c69db38739bef6b6cae"
    elif conf["test_flavor"] == "history_hosting":
        # Parmap's root directory
        conf["dir_swhid"] = "swh:1:dir:bc7ddd62cf3d72ffdc365e1bf2dea6eeaa44e185"
        # Parmap's snapshot from November 16, 2024
        conf["snp_swhid"] = "swh:1:snp:8ddca416836fbbc2a7704c69db38739bef6b6cae"
    elif conf["test_flavor"] == "example":
        # Revision in the example dataset
        conf["rev_swhid"] = "swh:1:rev:0000000000000000000000000000000000000009"
        # Directory in the example dataset
        conf["dir_swhid"] = "swh:1:dir:0000000000000000000000000000000000000006"
    elif conf["test_flavor"] == "none":
        pass
    else:
        raise ValueError(
            f"Unsupported test flavor: {test_flavor}."
            "Must be one of full, history_hosting, example or none."
        )

    return conf
