# Copyright (C) 2021-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
A simple tool to start the swh-graph gRPC server in Rust.
"""

import logging
import os
import shlex
import shutil
import subprocess

import aiohttp.test_utils
import aiohttp.web

from swh.graph.config import check_config

logger = logging.getLogger(__name__)


class ExecutableNotFound(EnvironmentError):
    pass


def build_rust_grpc_server_cmdline(**config):
    logger.debug("Checking configuration and populating default values")
    config = check_config(config)

    port = config.pop("port", None)
    if port is None:
        port = aiohttp.test_utils.unused_port()
        logger.debug("Port not configured, using random port %s", port)

    grpc_path = config["rust_executable_dir"] + "swh-graph-grpc-serve"
    if not os.path.isfile(grpc_path) and config.get("search_system_paths", True):
        grpc_path = shutil.which("swh-graph-grpc-serve")
    if not grpc_path or not os.path.isfile(grpc_path):
        raise ExecutableNotFound("swh-graph-grpc-serve executable not found")

    cmd = [grpc_path]
    if config.get("masked_nodes"):
        cmd.extend(["--masked-nodes", config["masked_nodes"]])
    logger.debug("Configuration: %r", config)
    cmd.extend(["--bind", f"[::]:{port}", str(config["path"])])
    cmd.extend(config.get("extra_options", []))
    print(f"Started GRPC using dataset from {str(config['path'])}")
    return cmd, port


def spawn_rust_grpc_server(**config):
    cmd, port = build_rust_grpc_server_cmdline(**config)
    print(cmd)
    # XXX: shlex.join() is in 3.8
    # logger.info("Starting gRPC server: %s", shlex.join(cmd))
    logger.info("Starting gRPC server: %s", " ".join(shlex.quote(x) for x in cmd))
    env = dict(os.environ)
    if config.get("profile") == "debug":
        env.setdefault("RUST_LOG", "debug,h2=info")  # h2 is very verbose at DEBUG level
    if "statsd_host" in config:
        env["STATSD_HOST"] = config["statsd_host"]
    if "statsd_port" in config:
        env["STATSD_PORT"] = str(config["statsd_port"])
    server = subprocess.Popen(cmd, env=env)
    return server, port


def stop_grpc_server(server: subprocess.Popen, timeout: int = 15):
    server.terminate()
    try:
        server.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("Server did not terminate, sending kill signal...")
        server.kill()
