# Copyright (C) 2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import signal

import pytest

from swh.core.api import TransientRemoteException
from swh.graph.http_client import RemoteGraphClient
from swh.graph.http_naive_client import NaiveClient

from .test_http_client import TEST_ORIGIN_ID


def test_leaves(graph_client, graph_grpc_server_process):
    if isinstance(graph_client, RemoteGraphClient):
        pass
    elif isinstance(graph_client, NaiveClient):
        pytest.skip("test irrelevant for naive graph client")
    else:
        assert False, f"unexpected graph_client class: {graph_client.__class__}"

    list(graph_client.leaves(TEST_ORIGIN_ID))

    server = graph_grpc_server_process
    pid = server.result["pid"]
    os.kill(pid, signal.SIGKILL)
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        pass

    it = graph_client.leaves(TEST_ORIGIN_ID)
    with pytest.raises(TransientRemoteException, match="failed to connect"):
        list(it)
