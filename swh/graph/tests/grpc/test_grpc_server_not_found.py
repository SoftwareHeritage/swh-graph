# Copyright (C) 2024-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.graph.example_dataset import DATASET_DIR
from swh.graph.grpc_server import ExecutableNotFound
from swh.graph.pytest_plugin import GraphServerProcess


def test_grpc_server_not_found():
    config = {
        "graph": {
            "cls": "local_rust",
            "grpc_server": {
                "path": DATASET_DIR / "compressed/example",
                "debug": True,
                "rust_executable_dir": "/path/to/nowhere/",
                "search_system_paths": False,
            },
            "http_rpc_server": {"debug": True},
        }
    }

    server = GraphServerProcess(config)
    assert server
    try:
        server.start()
    finally:
        server.stop()
    assert isinstance(server.result, ExecutableNotFound)
