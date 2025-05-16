# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.graph.example_dataset import DATASET_DIR
from swh.graph.grpc_server import ExecutableNotFound
from swh.graph.pytest_plugin import GraphServerProcess


def test_grpc_server_not_found(mocker):
    config = {
        "graph": {
            "cls": "local_rust",
            "grpc_server": {
                "path": DATASET_DIR / "compressed/example",
                "debug": True,
                "rust_executable_dir": "/path/to/nowhere/",
            },
            "http_rpc_server": {"debug": True},
        }
    }

    mocker.patch("shutil.which", lambda k: None)
    server = GraphServerProcess(config)
    assert server
    try:
        server.start()
    finally:
        server.stop()
    assert isinstance(server.result, ExecutableNotFound)
