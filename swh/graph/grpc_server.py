# Copyright (C) 2021-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
A simple tool to start the swh-graph GRPC server in Java.
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


def build_java_grpc_server_cmdline(**config):
    port = config.pop("port", None)
    if port is None:
        port = aiohttp.test_utils.unused_port()
        logger.debug("Port not configured, using random port %s", port)
    logger.debug("Checking configuration and populating default values")
    config = check_config(config)
    if config.get("masked_nodes"):
        raise NotImplementedError("masked_nodes are not supported by the Java backend")
    logger.debug("Configuration: %r", config)
    cmd = [
        "java",
        "--class-path",
        config["classpath"],
        *config["java_tool_options"].split(),
        "org.softwareheritage.graph.rpc.GraphServer",
        "--port",
        str(port),
        str(config["path"]),
    ]
    return cmd, port


def build_rust_grpc_server_cmdline(**config):
    port = config.pop("port", None)
    if port is None:
        port = aiohttp.test_utils.unused_port()
        logger.debug("Port not configured, using random port %s", port)
    debug_mode = config.get("debug", False)

    grpc_path = f"./target/{'debug' if debug_mode else 'release'}/swh-graph-grpc-serve"
    if not os.path.isfile(grpc_path):
        grpc_path = shutil.which("swh-graph-grpc-serve")
    if not grpc_path:
        raise EnvironmentError("swh-graph-grpc-serve executable not found")

    cmd = [grpc_path, "-vvvvv" if debug_mode else "-vv"]

    logger.debug("Checking configuration and populating default values")
    config = check_config(config)
    if config.get("masked_nodes"):
        cmd.extend(["--masked-nodes", config["masked_nodes"]])
    logger.debug("Configuration: %r", config)
    cmd.extend(["--bind", f"[::]:{port}", str(config["path"])])
    print(f"Started GRPC using dataset from {str(config['path'])}")
    return cmd, port


def spawn_java_grpc_server(**config):
    cmd, port = build_java_grpc_server_cmdline(**config)
    print(cmd)
    # XXX: shlex.join() is in 3.8
    # logger.info("Starting gRPC server: %s", shlex.join(cmd))
    logger.info("Starting gRPC server: %s", " ".join(shlex.quote(x) for x in cmd))
    server = subprocess.Popen(cmd)
    return server, port


def spawn_rust_grpc_server(**config):
    cmd, port = build_rust_grpc_server_cmdline(**config)
    print(cmd)
    # XXX: shlex.join() is in 3.8
    # logger.info("Starting gRPC server: %s", shlex.join(cmd))
    logger.info("Starting gRPC server: %s", " ".join(shlex.quote(x) for x in cmd))
    server = subprocess.Popen(cmd)
    return server, port


def stop_java_grpc_server(server: subprocess.Popen, timeout: int = 15):
    server.terminate()
    try:
        server.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        logger.warning("Server did not terminate, sending kill signal...")
        server.kill()
