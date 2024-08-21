# Copyright (C) 2019-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from pathlib import Path
import sys

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
    debug_mode = conf.get("debug", "pytest" in sys.modules)
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


def check_config_compress(config, graph_name, in_dir, out_dir):
    """check compression-specific configuration and initialize its execution
    environment.
    """
    conf = check_config(config)

    conf["graph_name"] = graph_name
    conf["in_dir"] = str(in_dir)
    conf["out_dir"] = str(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if "tmp_dir" not in conf:
        tmp_dir = out_dir / "tmp"
        conf["tmp_dir"] = str(tmp_dir)
    else:
        tmp_dir = Path(conf["tmp_dir"])
    tmp_dir.mkdir(parents=True, exist_ok=True)

    return conf
