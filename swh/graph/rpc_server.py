# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
A simple tool to start the swh-graph GRPC server in Java.
"""

import logging
import shlex
import subprocess

import aiohttp.test_utils
import aiohttp.web

from swh.graph.config import check_config


def spawn_java_rpc_server(config, port=None):
    if port is None:
        port = aiohttp.test_utils.unused_port()
    config = check_config(config or {})
    cmd = [
        "java",
        "-cp",
        config["classpath"],
        *config["java_tool_options"].split(),
        "org.softwareheritage.graph.rpc.GraphServer",
        "--port",
        str(port),
        str(config["graph"]["path"]),
    ]
    print(cmd)
    # XXX: shlex.join() is in 3.8
    # logging.info("Starting RPC server: %s", shlex.join(cmd))
    logging.info("Starting RPC server: %s", " ".join(shlex.quote(x) for x in cmd))
    server = subprocess.Popen(cmd)
    return server, port


def stop_java_rpc_server(server: subprocess.Popen, timeout: int = 15):
    server.terminate()
    try:
        server.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        logging.warning("Server did not terminate, sending kill signal...")
        server.kill()
