# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Spawns a gRPC server on a newly compressed graph, and checks responses
are as expected."""

import difflib
import json
import logging
from typing import Optional, Union

from google.protobuf.json_format import MessageToDict

from .webgraph import check_config_compress

logger = logging.getLogger(__name__)


def run_e2e_test(
    graph_name: str,
    in_dir: Optional[str],
    out_dir: Optional[str],
    test_flavor: Optional[str],
    profile: str = "release",
):
    """Empirically test the graph compression correctness.

    Check for a specific SWHID in the compressed graph and do
    simple traversal requests to ensure the compression went
    well. This is a best effort procedure, as it is (generally)
    not possible to check for every single item in the graph.

    Args:
        graph_name: graph base name, relative to in_dir
        in_dir: input directory, where the uncompressed graph can be found
        out_dir: output directory, where the compressed graph will be stored
        test_flavor: which flavor of tests to run
        profile: Which Rust executables to use

    Raises:
        Exception: GRPC server unexpectedly stopped
    """
    import socket
    import time

    import grpc

    import swh.graph.grpc.swhgraph_pb2 as swhgraph
    import swh.graph.grpc.swhgraph_pb2_grpc as swhgraph_grpc
    from swh.graph.grpc_server import spawn_rust_grpc_server, stop_grpc_server

    conf = check_config_compress(
        {"profile": profile}, graph_name, in_dir, out_dir, test_flavor
    )

    graph_name = conf["graph_name"]
    in_dir = conf["in_dir"]
    out_dir = conf["out_dir"]
    test_flavor = conf["test_flavor"]

    if test_flavor == "none":
        logger.info("End to end tests skipped.")
        return

    if "graph_path" not in conf:
        conf["graph_path"] = f"{out_dir}/{graph_name}"

    server, port = spawn_rust_grpc_server(**conf, path=conf["graph_path"])

    # wait for the server to accept connections
    while True:
        try:
            socket.create_connection(("localhost", port))
        except Exception:
            time.sleep(0.1)
            server.poll()
            if server.returncode is not None:
                raise Exception("GRPC server unexpectedly stopped.")
        else:
            break

    # This dictionary is a copy of a few directories retrieved from the graph. It
    # includes a few well known projects, such as `parmap`, `apt`, or `vim`. The
    # items have all been directly copied over in the file after having been queried
    # from the 2024-12-06 graph.
    # They should not change in the future, as all traversals start from fixed snapshots.
    # For each project, the `cnt` field corresponds to the project's README, and the
    # `dir` field is the root directory.
    test_values: dict[
        str, dict[str, dict[str, Union[str, int, object, list[dict[str, str]]]]]
    ] = {
        "parmap": {
            "cnt": {
                "swhid": "swh:1:cnt:43243e2ae91a64e252170cd922718e8c2af323b6",
                "cnt": {"length": "7225", "isSkipped": False},
                "numSuccessors": "0",
            },
            "dir": {
                "swhid": "swh:1:dir:bc7ddd62cf3d72ffdc365e1bf2dea6eeaa44e185",
                "successor": [
                    {
                        "swhid": "swh:1:cnt:1236c69684fe78ffa93fe0c712b28e7db2e1dec0",
                        "label": [{"name": "TElDRU5TRQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:0164c78f68e74c1950bb15bb65f26af85409be1b",
                        "label": [{"name": "QVVUSE9SUw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:1e653ecad9bd593e5125d9fc0a67caf5e68684ea",
                        "label": [{"name": "TWFrZWZpbGU=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:8d9ac6745d9e09a76bee9254569c07296420dd33",
                        "label": [{"name": "LmdpdGlnbm9yZQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:c4282f28f185d7ff8f437610786210b45da884e3",
                        "label": [{"name": "Y29uZmln", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:43243e2ae91a64e252170cd922718e8c2af323b6",
                        "label": [{"name": "UkVBRE1FLm1k", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:6b04cd329678804c29f57c396dc80673f51e6122",
                        "label": [{"name": "Q0hBTkdFUw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:8f52ba1f4b26e8ad183b3211ee1a3ae1fed1835e",
                        "label": [{"name": "c3Jj", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:7fdf9b6bd0d25f2cab20387eda552ffeb4569a75",
                        "label": [{"name": "cGFybWFwLmJpYg==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:f7b51b7345fb8c3a5fa9f0afebb56f4d2315c83a",
                        "label": [{"name": "dGVzdHM=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:d3efdba145b4d10c89185976162cb703b21752aa",
                        "label": [{"name": "ZXhhbXBsZQ==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:7d71795857dc26d8013bbc28c5a82772ebb0b598",
                        "label": [{"name": "cGFybWFwLm9wYW0=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:353bd74b1b29c30b9b47843ddc8dc0180b37a465",
                        "label": [{"name": "ZHVuZS1wcm9qZWN0", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:7104fab006bbeb57dc12f72d158fe4f2c63a9b27",
                        "label": [
                            {"name": "Y29kZW1ldGEuanNvbg==", "permission": 33188}
                        ],
                    },
                ],
                "numSuccessors": "14",
            },
            "rev": {
                "swhid": "swh:1:rev:ecd3744ed558da4ea2bf9eb87b80b8949f417126",
                "successor": [
                    {"swhid": "swh:1:rev:963608763589e03de38e744d359884d491e65460"},
                    {"swhid": "swh:1:rev:a9ce4341ea0e36dd2336fc868e99b577b3ed0b51"},
                    {"swhid": "swh:1:dir:bc7ddd62cf3d72ffdc365e1bf2dea6eeaa44e185"},
                ],
                "rev": {
                    "author": "54119287",
                    "authorDate": "1731583490",
                    "authorDateOffset": 60,
                    "committer": "72133999",
                    "committerDate": "1731583490",
                    "committerDateOffset": 60,
                    "message": "TWVyZ2UgcHVsbCByZXF1ZXN0ICMxMTUgZnJvbSBhbmxhbWJlcnQvY29kZW1ldGEtZm94LW9yY2lkLXVybHMKCmNvZGVtZXRhLmpzb246IEZpeCBPUkNJRCBVUkxzIGZvciBhdXRob3Jz",  # noqa: B950
                },
                "numSuccessors": "3",
            },
            "snp": {"swhid": "swh:1:snp:8ddca416836fbbc2a7704c69db38739bef6b6cae"},
        },
        "apt": {
            "cnt": {
                "swhid": "swh:1:cnt:b2a0fdb2bed782d674e8799e285e68bd180dd656",
                "cnt": {"length": "10700", "isSkipped": False},
                "numSuccessors": "0",
            },
            "dir": {
                "swhid": "swh:1:dir:ab89bb001c32fba7ae7350155cb5f3d039c6f7ab",
                "successor": [
                    {
                        "swhid": "swh:1:cnt:d159169d1050894d3ea3b98e1c965c4058208fe1",
                        "label": [{"name": "Q09QWUlORy5HUEw=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:40f85d4a096ee06fd1096092f837f424cc7f7042",
                        "label": [{"name": "dGVzdA==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:eb23b7c21df055f9863a9556861304710e34a2e4",
                        "label": [{"name": "ZnRwYXJjaGl2ZQ==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:21b874fa77fda1b0b17a56381908acd065b70001",
                        "label": [{"name": "Q09QWUlORw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:336e401e7b3754ca678d2747db56216f911890a6",
                        "label": [{"name": "ZHNlbGVjdA==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:da6e6d8c274e1e5bd9634fe81f1715580dd5fd6c",
                        "label": [
                            {
                                "name": "Z2l0LWNsYW5nLWZvcm1hdC5zaA==",
                                "permission": 33261,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:fe9628fd07281a7b47c59d5f376105df48712642",
                        "label": [
                            {"name": "cHJlcGFyZS1yZWxlYXNl", "permission": 33261}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:fb713039f3b513bf2a121f0675e894c65e8c6f97",
                        "label": [{"name": "dmVuZG9y", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:9f63061d148dea34f14b68e422b0bf6f5d2478ca",
                        "label": [{"name": "Y21kbGluZQ==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:fb85a73184f07ac5b42be77a3ee89fedda8155e4",
                        "label": [
                            {"name": "LmdpdGxhYi1jaS55bWw=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:8b202bc1e531269de73d51aaaf3054fc1cd12c06",
                        "label": [{"name": "YXB0LXByaXZhdGU=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:b8eff035d7adf2b7e53b3349a7edaccf171a3ce8",
                        "label": [{"name": "Q01ha2U=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:4357ce686d75a0e42c965bee77dac683138cf9b1",
                        "label": [{"name": "YXB0LXBrZw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:3a2677ffef7018ef899a265fd83ac7fc0452cb45",
                        "label": [
                            {"name": "Q01ha2VMaXN0cy50eHQ=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:d5ee100ab8b16066a4932c6510022b2ad6e9ddf7",
                        "label": [{"name": "ZGViaWFu", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:3de81b1e6839921a50595680a69bd35a78d8f8b7",
                        "label": [{"name": "bWV0aG9kcw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:4f95ea257c58a88f0fe019b7af00ba00b31cabc5",
                        "label": [{"name": "cG8=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:2fbb3a159982ca77809a63c3987d0e934098a914",
                        "label": [{"name": "ZG9j", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:cce31e651dddbd043c0dba03438093cff40d663b",
                        "label": [{"name": "QVVUSE9SUw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:c2989f89134d75fe150b543c6bd33886b0cf8fa8",
                        "label": [
                            {"name": "LmNsYW5nLWZvcm1hdA==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:e7d2bbf5415b24123bf932555463bae0983012df",
                        "label": [
                            {"name": "bWlycm9yLWZhaWx1cmUucHk=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:aeab4a372153e6c47e682ae4e7cea2dcf86754a1",
                        "label": [{"name": "LmdpdGlnbm9yZQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:8f1b6f2e39296fc9b89db7cb09b6de5b7d235661",
                        "label": [{"name": "Lm1haWxtYXA=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:b2a0fdb2bed782d674e8799e285e68bd180dd656",
                        "label": [{"name": "UkVBRE1FLm1k", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:d902fb47a15534156dcbf05a56f1806d627c6dc1",
                        "label": [{"name": "YWJpY2hlY2s=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:c05b7c202ed5d66abcfd4f2cd2d1d3102d6f7b2a",
                        "label": [{"name": "RG9ja2VyZmlsZQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:f76b0839784f91f6e10eccd6a89a21fe02176e18",
                        "label": [
                            {"name": "c2hpcHBhYmxlLnltbA==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:e45d79e66758a45f210e95204ddf50d4ebc4d914",
                        "label": [{"name": "Y29tcGxldGlvbnM=", "permission": 16384}],
                    },
                ],
                "numSuccessors": "28",
            },
            "rev": {
                "swhid": "swh:1:rev:a0a76c2e20c1ddefd76a4a539a9350b96d66006e",
                "successor": [
                    {"swhid": "swh:1:rev:e62162d010fc7d6374067964ced3ac227b0440b2"},
                    {"swhid": "swh:1:dir:ab89bb001c32fba7ae7350155cb5f3d039c6f7ab"},
                ],
                "rev": {
                    "author": "78286833",
                    "authorDate": "1711815195",
                    "authorDateOffset": 0,
                    "committer": "78286833",
                    "committerDate": "1711817672",
                    "committerDateOffset": 0,
                    "message": "Rml4IGFuZCB1bmZ1enp5IHByZXZpb3VzIFZDRy9HcmFwaHZpeiBVUkkgY2hhbmdlCgpUaGUgR3JhcGh2aXogY2hhbmdlIGFkYXB0ZWQgb25seSB0aGUgbXNnc3RycyBpbiB0aGUgcG8gZmlsZXMsIHdoaWNoCmZ1enppZWQgYWxsIHRyYW5zbGF0aW9ucyB0aGUgbW9tZW50IHRoZSBtc2dpZHMgd2VyZSB1cGRhdGVkIGJ5IHRoZQptZXJnZSB3aGlsZSBmb3JnZXR0aW5nIGZyLnBvIGFuZCBwbC5wbyBhbmQgaW4gdGhlIFZDRyBjaGFuZ2UgdGhlCm1zZ3N0ciBpbiB0aGUgamEucG8gd2FzIG1pc3NlZC4KClRoaXMgY29tbWl0ICJqdXN0IiB1bmZ1enppZXMgdGhlIHN0cmluZ3MgKGFuZCBmaXhlcyB0aGUgbWlzc2VkIG9uZXMpCnNvIHRoZSB0cmFuc2xhdG9ycyBkb24ndCBoYXZlIHRvIGFzIGludGVuZGVkIGluIHRoZSBjb21taXRzLgoKUmVmZXJlbmNlczogYWU1YzI5MWIwYzc2YjIyYzkzOTY3OWNjZjM1ZTNlYjk0MTMxZTU4NgogZWRmMTAyNWU2OTFlOTRjZGRlMzA1YmMyZDE1NDg0ZmM3ZWU2ZGIxYwo=",  # noqa: B950
                },
                "numSuccessors": "2",
            },
            "snp": {"swhid": "swh:1:snp:4bf4f77adb7659ed2efcbc07d03bba61ee5b9b9b"},
        },
        "coreutils": {
            "cnt": {
                "swhid": "swh:1:cnt:81d292d0719f439334150b3effd673c666a201ed",
                "cnt": {"length": "6649", "isSkipped": False},
                "numSuccessors": "0",
            },
            "dir": {
                "swhid": "swh:1:dir:0dfb60e64c8fc8f416282e3c7deb163834916d43",
                "successor": [
                    {
                        "swhid": "swh:1:dir:d0db55abe96d886906f1b944fbb7ea846aa4296e",
                        "label": [{"name": "dGVzdHM=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:867d4ff204108f3f94f1f74b83adb9ee1b01d12f",
                        "label": [{"name": "ZG9j", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:de77eeb11434ed6e1f8237531a87c7c44be1c451",
                        "label": [{"name": "bTQ=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:6251a2f68681ca468e7e78bcc9ca7cfd2d70664e",
                        "label": [{"name": "TkVXUw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:f34b557bb2144ffb9f8419aa0975f2f828d93673",
                        "label": [{"name": "Z2w=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:17f9d9c693c0867b5402b77fc18b942173c7a393",
                        "label": [{"name": "VEhBTktTLmlu", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:7868f8d17c6e48982251369e971bb137f6a3dd96",
                        "label": [{"name": "cG8=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:62f7fd160b007bd67b308c99dbd6c1b257c5487b",
                        "label": [{"name": "VE9ETw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:3514c6da42efe79c676700c0a7940f4e53663900",
                        "label": [
                            {"name": "LnZnLXN1cHByZXNzaW9ucw==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:81d292d0719f439334150b3effd673c666a201ed",
                        "label": [{"name": "UkVBRE1F", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:2512723590f8439f3cd357386968bc2e776ca74c",
                        "label": [{"name": "aW5pdC5jZmc=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:8a49f569c924d122eedba668d69d3c15a789f0c0",
                        "label": [{"name": "SEFDS0lORw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:0592dbbc83d03fda9631adc82e9802090b094f5a",
                        "label": [{"name": "LmdpdGh1Yg==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:5fbea73a8bba09b957fb19461f00e9947c1a5b94",
                        "label": [
                            {"name": "UkVBRE1FLXZhbGdyaW5k", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:00dd0c5deef3a7be4c72fd873b81eff86d2441b8",
                        "label": [{"name": "bGli", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:dd6b48a36815940ae2ab8a31a894fb41472fc9a0",
                        "label": [
                            {"name": "UkVBRE1FLWhhY2tpbmc=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:d9195952fa5a1005bf36802aa0e432ffc9dd07ec",
                        "label": [{"name": "c2NyaXB0cw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:d4a50fcdae7cc5e14f4ed372dd52d4dfad670b2c",
                        "label": [{"name": "LmdpdGlnbm9yZQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:f2557ce0d1e1889196a9fa3975a943906002795b",
                        "label": [{"name": "Y29uZmlndXJlLmFj", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:511bdd844cf548d1081763fde11c275096010623",
                        "label": [{"name": "YnVpbGQtYXV4", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:3fd546599594be955f36a6992efd218f3649ee7a",
                        "label": [{"name": "TWFrZWZpbGUuYW0=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:d78c8c306487a1015280528adabca091507ab54b",
                        "label": [{"name": "Ym9vdHN0cmFw", "permission": 33261}],
                    },
                    {
                        "swhid": "swh:1:rev:8f292d0931b1e1f7c5a063bd400ff9dabb3897ff",
                        "label": [{"name": "Z251bGli", "permission": 57344}],
                    },
                    {
                        "swhid": "swh:1:cnt:cbe5b0ca43c97d3547fe000c8f5dc460d9c5b81c",
                        "label": [
                            {"name": "Ym9vdHN0cmFwLmNvbmY=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:647b6600edcd9803fefa9190d0c10e5f95c63f56",
                        "label": [{"name": "Y2ZnLm1r", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:9993f552737a95651e6fb08ea74c4020514cb400",
                        "label": [{"name": "LmdpdG1vZHVsZXM=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:9ae4b12af907e40063aff73ab683e6926f4aec13",
                        "label": [
                            {"name": "UkVBRE1FLXByZXJlcQ==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:bbc03495194b6e2c043ec44ca00779c3dc0f3c7b",
                        "label": [
                            {"name": "UkVBRE1FLWluc3RhbGw=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:d5c12aa971728f7677fac58afb940a4bd5fbb1fa",
                        "label": [{"name": "QVVUSE9SUw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:5fe656bfb22facaeda15875ba1793450ad8b0715",
                        "label": [{"name": "c3Jj", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:305325b2eab7d42bed000d06470c183177ed8e87",
                        "label": [{"name": "Z251bGliLXRlc3Rz", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:61049f2c3bacad3688ff821d5db741182e22e4df",
                        "label": [{"name": "bWFu", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:592f36ef3a918f5e1a189156a0671e73fc408fdd",
                        "label": [
                            {"name": "LnByZXYtdmVyc2lvbg==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:081b2d45e493fcee4f2cb1bdecbd575afedfbb54",
                        "label": [
                            {
                                "name": "UkVBRE1FLXBhY2thZ2UtcmVuYW1lZC10by1jb3JldXRpbHM=",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:45f4d9906402e3ee5a932787a781d86c17e18bcb",
                        "label": [
                            {
                                "name": "LngtdXBkYXRlLWNvcHlyaWdodA==",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:e03db91a330a4453fcd7e7daf3437c6bbd27d015",
                        "label": [
                            {"name": "UkVBRE1FLXJlbGVhc2U=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:dc04853c3a10b76a9474fbcea444d304352b4488",
                        "label": [{"name": "Lm1haWxtYXA=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:afa24bfd8b2c75947b109317bd5ce15bda02e63a",
                        "label": [
                            {"name": "ZGlzdC1jaGVjay5taw==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:f288702d2fa16d3cdf0035b15a9fcbc552cd88e7",
                        "label": [{"name": "Q09QWUlORw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:f1c11b3a56f61b425affd674de53dd09dc567d6e",
                        "label": [{"name": "dGhhbmtzLWdlbg==", "permission": 33261}],
                    },
                    {
                        "swhid": "swh:1:cnt:25e7a99be017f663c80e18887c0f4591328b4afa",
                        "label": [{"name": "VEhBTktTdHQuaW4=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:c3b2926c78c939d94358cc63d051a70d38cfea5d",
                        "label": [
                            {"name": "LmdpdGF0dHJpYnV0ZXM=", "permission": 33188}
                        ],
                    },
                ],
                "numSuccessors": "42",
            },
            "rev": {
                "swhid": "swh:1:rev:1ae98dbda7322427e8226356fd110d2553f5fac9",
                "successor": [
                    {"swhid": "swh:1:rev:83ec7a706a9f931e418ec6a084587b8995ca5388"},
                    {"swhid": "swh:1:dir:0dfb60e64c8fc8f416282e3c7deb163834916d43"},
                ],
                "rev": {
                    "author": "71999780",
                    "authorDate": "1722312996",
                    "authorDateOffset": -420,
                    "committer": "52538937",
                    "committerDate": "1722326039",
                    "committerDateOffset": 60,
                    "message": "bWFpbnQ6IHJlbW92ZSB1bm5lY2Vzc2FyeSBpbnR0b3N0ciB1c2FnZSBpbiBwcmludGYKCiogc3JjL2Nrc3VtLmMgKG91dHB1dF9jcmMpOiBVc2UgJyVqdScgaW5zdGVhZCBvZiB1bWF4dG9zdHIuCiogc3JjL3NocmVkLmMgKGRvcGFzcyk6IExpa2V3aXNlLgoqIHNyYy9jc3BsaXQuYyAoaGFuZGxlX2xpbmVfZXJyb3IsIHJlZ2V4cF9lcnJvciwgY2xvc2Vfb3V0cHV0X2ZpbGUpCihwYXJzZV9wYXR0ZXJucyk6IFVzZSAnJWpkJyBpbnN0ZWFkIG9mIG9mZnRvc3RyLgoqIHNyYy90YWlsLmMgKHhsc2Vlayk6IExpa2V3aXNlLgoqIHNyYy9oZWFkLmMgKGVsc2Vlayk6IExpa2V3aXNlLgoqIHNyYy9ncm91cC1saXN0LmMgKGdpZHRvc3RyX3B0cik6IFJlbW92ZSBmdW5jdGlvbi4KKGdpZHRvc3RyKTogUmVtb3ZlIG1hY3JvLgoocHJpbnRfZ3JvdXApOiBVc2UgJyVqdScgaW5zdGVhZCBvZiB1bWF4dG9zdHIuCiogc3JjL2lkLmMgKGdpZHRvc3RyX3B0ciwgdWlkdG9zdHJfcHRyKTogUmVtb3ZlIGZ1bmN0aW9ucy4KKGdpZHRvc3RyLCB1aWR0b3N0cik6IFJlbW92ZSBtYWNyb3MuCihwcmludF91c2VyLCBwcmludF9mdWxsX2luZm8pOiBVc2UgJyVqdScgaW5zdGVhZCBvZiB1bWF4dG9zdHIuCiogc3JjL3NvcnQuYyAoc3BlY2lmeV9ubWVyZ2UpOiBVc2UgJyV1JyBpbnN0ZWFkIG9mIHVpbnR0b3N0ci4K",  # noqa: B950
                },
                "numSuccessors": "2",
            },
            "snp": {"swhid": "swh:1:snp:d5ce84405f7d061377512be24870fa001032063f"},
        },
        "pip": {
            "cnt": {
                "swhid": "swh:1:cnt:479ddfd7ba175c2db0b0f13898990656c8bff2c8",
                "cnt": {"length": "2375", "isSkipped": False},
                "numSuccessors": "0",
            },
            "dir": {
                "swhid": "swh:1:dir:c027aeec1e933aa3ef3d8d51227591c72d5ded33",
                "successor": [
                    {
                        "swhid": "swh:1:cnt:8ccefbc6e59f0bc761589c7994c15c7922ce5127",
                        "label": [{"name": "QVVUSE9SUy50eHQ=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:9f7b3c018c074313a78471abe1f3a80b60319d7f",
                        "label": [{"name": "LmdpdGh1Yg==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:3a0319c2356b8337ed335c3ea36c489e9272fa4b",
                        "label": [
                            {
                                "name": "LnByZS1jb21taXQtY29uZmlnLnlhbWw=",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:96a4cb0107e774ff045f15634b8f6edf9649fbe1",
                        "label": [{"name": "dG9vbHM=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:f9de633517a68fbd7dcfffaaf723a3194f59a22a",
                        "label": [{"name": "dGVzdHM=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:711e6bc8485d2f76a44373b90c6cfa48f9f24794",
                        "label": [
                            {"name": "cHlwcm9qZWN0LnRvbWw=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:5ebf7141b1d7a13a097a3588fb276be7affe5ed7",
                        "label": [{"name": "TkVXUy5yc3Q=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:6401bb537983a328f98689afaa2b0ac68ea929ae",
                        "label": [{"name": "ZG9jcw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:2051362769c4d356a0d9e9445d92f0b0dfc4c133",
                        "label": [{"name": "bm94ZmlsZS5weQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:814da8265cabfaf7e95100faba9761253f402b81",
                        "label": [{"name": "TUFOSUZFU1QuaW4=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:479ddfd7ba175c2db0b0f13898990656c8bff2c8",
                        "label": [{"name": "UkVBRE1FLnJzdA==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:3eea23d8575573d0b29cfed5dc3f7e3859deb296",
                        "label": [{"name": "c3Jj", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:3093f76c8ce2f35cbd55c91a0c6dcc1c2086ecc2",
                        "label": [{"name": "bmV3cw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:875dba24ed3f151a2a17c30e325623d78e0792db",
                        "label": [{"name": "Lm1haWxtYXA=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:f09b08660e73a7854476c035f2abe0897c5aee97",
                        "label": [
                            {
                                "name": "LmdpdC1ibGFtZS1pZ25vcmUtcmV2cw==",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:c0d2bba55e930af76612d474cc2882de07633916",
                        "label": [
                            {"name": "LnJlYWR0aGVkb2NzLnltbA==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:e75a1c0de68c40f2182d3624158460219a229de1",
                        "label": [{"name": "U0VDVVJJVFkubWQ=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:6a0fc6943c16498aa56cdb5bfd0612460a68c46b",
                        "label": [
                            {"name": "LmdpdGF0dHJpYnV0ZXM=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:79b8ab84b06d6f15a23a1d83d2bc4245aaadd70f",
                        "label": [{"name": "LmdpdGlnbm9yZQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:d0c072fbf96914da51ef441c619cc28f010d5940",
                        "label": [
                            {
                                "name": "LnJlYWR0aGVkb2NzLWN1c3RvbS1yZWRpcmVjdHMueW1s",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:8e7b65eaf628360e6f32f4140fcdd7ec7c2b7077",
                        "label": [{"name": "TElDRU5TRS50eHQ=", "permission": 33188}],
                    },
                ],
                "numSuccessors": "21",
            },
            "rev": {
                "swhid": "swh:1:rev:fe0925b3c00bf8956a0d33408df692ac364217d4",
                "successor": [
                    {"swhid": "swh:1:dir:c027aeec1e933aa3ef3d8d51227591c72d5ded33"},
                    {"swhid": "swh:1:rev:099ae9703a2e15a7f90b82fa251c51be0f88d602"},
                    {"swhid": "swh:1:rev:420435903ff2fc694d6950a47b896427ecaed78f"},
                ],
                "rev": {
                    "author": "13162035",
                    "authorDate": "1731171039",
                    "authorDateOffset": 480,
                    "committer": "72133999",
                    "committerDate": "1731171039",
                    "committerDateOffset": 480,
                    "message": "TWVyZ2UgcHVsbCByZXF1ZXN0ICMxMzA3MyBmcm9tIG1nb3JueS9yaWNoLXBpcGUtaGFuZGxpbmcKCg==",  # noqa: B950
                },
                "numSuccessors": "3",
            },
            "snp": {"swhid": "swh:1:snp:9120fcbbc23b126711fdcac75b44a9234387c09b"},
        },
        "vim": {
            "cnt": {
                "swhid": "swh:1:cnt:cfee68e8a2767b6e252923a6f934115fbb13eb44",
                "cnt": {"length": "6914", "isSkipped": False},
                "numSuccessors": "0",
            },
            "dir": {
                "swhid": "swh:1:dir:d7b6b8be2f7cedd5a5c12a0df7f9ef6456a70441",
                "successor": [
                    {
                        "swhid": "swh:1:cnt:778b8293faa59236d1f7f0046c3ab8dfee269f54",
                        "label": [{"name": "dmltdHV0b3IuY29t", "permission": 33261}],
                    },
                    {
                        "swhid": "swh:1:cnt:9216dbe8dbb4f532fe9fa632fbfc49f3ee98fbe1",
                        "label": [
                            {"name": "LmdpdGF0dHJpYnV0ZXM=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:d9d99c655ede93f13e313361ed0c3955c93a6c40",
                        "label": [{"name": "Y29uZmlndXJl", "permission": 33261}],
                    },
                    {
                        "swhid": "swh:1:cnt:a586af40e2a6fffba0d666833df281e2aaacad12",
                        "label": [
                            {"name": "LmVkaXRvcmNvbmZpZw==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:6fc84ef10b2543b16495b11a22f6683cd21b7e32",
                        "label": [
                            {"name": "LmNsYW5nLWZvcm1hdA==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:abb3a6ba93bc83c4648209f9b2917499fedadfee",
                        "label": [{"name": "bGFuZw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:cb635a2f59f66c44dc446533fc9c40857e47dda6",
                        "label": [
                            {
                                "name": "LmdpdC1ibGFtZS1pZ25vcmUtcmV2cw==",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:c5b1b3910c348b69f54ad88c7678c2f5dbacb656",
                        "label": [{"name": "RmlsZWxpc3Q=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:7441fa7d75af5621ab7c6e8f2e070ecf335f6341",
                        "label": [{"name": "c3Jj", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:06cecb4a8aa7455ea5cc7003ae470c3809824d4e",
                        "label": [{"name": "cGl4bWFwcw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:1ed4a821600faf0c4586ce9dffb28ef2be1cb4a6",
                        "label": [{"name": "UkVBRE1FZGly", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:048ced9db6a6d82f46b8d973200fcb4c9421a4dd",
                        "label": [{"name": "LmdpdGlnbm9yZQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:5c9754e2ec8fe077488488fcebb9eb12877a8e67",
                        "label": [{"name": "LmhnaWdub3Jl", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:fddf33c930d58d77b67c75e591da5167d237b687",
                        "label": [{"name": "TWFrZWZpbGU=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:06c4aff198ccdbcd3ce462594352753581b0fb16",
                        "label": [{"name": "bnNpcw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:6695ee397f5b85f06385d5fceac35d73a6671beb",
                        "label": [{"name": "dmltdHV0b3IuYmF0", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:e81ed84d7561154ab6e4e9e155fee8d16c20dc86",
                        "label": [{"name": "LmNpcnJ1cy55bWw=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:6c5c05b0130f70cbd86f1c174b42c349c8ca6f0f",
                        "label": [{"name": "TElDRU5TRQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:35da1092107cc4217baf7f54a687db15d5e25ef2",
                        "label": [
                            {"name": "LmFwcHZleW9yLnltbA==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:ef957779895280b16a987063c554325a1a473003",
                        "label": [{"name": "LmNvZGVjb3YueW1s", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:c9ce73a2d1ffaa3f116b104cfb12b38a4d3d95eb",
                        "label": [
                            {"name": "UkVBRE1FX1ZJTTkubWQ=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:960c9109603a0ddccb2a8388a8a8b8e6ea39d111",
                        "label": [
                            {"name": "dW5pbnN0YWxsLnR4dA==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:ee4d69ddf8110f3b0bbb1433efa640609fc5acfb",
                        "label": [{"name": "dG9vbHM=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:7d1e0166c9c19379b3dfcfbe79a7628aa68e2cd2",
                        "label": [{"name": "U0VDVVJJVFkubWQ=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:99bfcfa2c8c782121baacf12734d0925169da127",
                        "label": [
                            {"name": "Q09OVFJJQlVUSU5HLm1k", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:be909280c8cf2ba5812f79599fb671ecd4662b9c",
                        "label": [
                            {"name": "UkVBRE1FLnJ1eC50eHQ=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:e3c58955679a2e55e42691150e84a9df682e8252",
                        "label": [{"name": "Y2k=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:cfee68e8a2767b6e252923a6f934115fbb13eb44",
                        "label": [{"name": "UkVBRE1FLm1k", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:d90ebbc1fae66f43bc4f62922e068d518f2f9a6d",
                        "label": [{"name": "UkVBRE1FLnR4dA==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:4a10974550b45f4c99824ddce6446212860737f2",
                        "label": [{"name": "LmdpdGh1Yg==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:9de4bee64384ba8aff615bfad88fe61d8ea8fabb",
                        "label": [{"name": "cnVudGltZQ==", "permission": 16384}],
                    },
                ],
                "numSuccessors": "31",
            },
            "rev": {
                "swhid": "swh:1:rev:a01148d2cb2f8d2820a5b95474d11db0d1802360",
                "successor": [
                    {"swhid": "swh:1:dir:d7b6b8be2f7cedd5a5c12a0df7f9ef6456a70441"},
                    {"swhid": "swh:1:rev:d7745acbd8fe1e4feb356a6dc7fc185eeab17d67"},
                ],
                "rev": {
                    "author": "4757105",
                    "authorDate": "1732367998",
                    "authorDateOffset": 60,
                    "committer": "4722660",
                    "committerDate": "1732367998",
                    "committerDateOffset": 60,
                    "message": "cnVudGltZShkb2MpOiBFeHBhbmQgZG9jcyBvbiA6ISB2cy4gOnRlcm0KCmZpeGVzOiAjMTYwNzEKY2xvc2VzOiAjMTYwODkKClNpZ25lZC1vZmYtYnk6IG1hdHZleXQgPG1hdHRoZXd0YXJhc292QHlhbmRleC5ydT4KU2lnbmVkLW9mZi1ieTogQ2hyaXN0aWFuIEJyYWJhbmR0IDxjYkAyNTZiaXQub3JnPgo=",  # noqa: B950
                },
                "numSuccessors": "2",
            },
            "snp": {"swhid": "swh:1:snp:da91e8e97edb59f9e97ee9388e419f77b018d856"},
        },
        "swh-graph": {
            "cnt": {
                "swhid": "swh:1:cnt:d7abf98f65283673a669eeba114f545934153f5d",
                "cnt": {"length": "1216", "isSkipped": False},
                "numSuccessors": "0",
            },
            "dir": {
                "swhid": "swh:1:dir:1618172adfdc2baef53ff0c27ebbd33899c455b0",
                "successor": [
                    {
                        "swhid": "swh:1:cnt:94a9ed024d3859793618152ea559a168bbcbb5e2",
                        "label": [{"name": "TElDRU5TRQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:c329086f6eacdb10daf84199c6c84f9ef6eca553",
                        "label": [{"name": "QVVUSE9SUw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:ec16ee17aebb01aaa7a41afe002bed9397798810",
                        "label": [
                            {
                                "name": "LmdpdC1ibGFtZS1pZ25vcmUtcmV2cw==",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:4d665e8f4f2011777a311571234c6799dc70e6dc",
                        "label": [{"name": "Y29uZnRlc3QucHk=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:40fb2fbffe23f81f42b871cb9c52190f9c84ae32",
                        "label": [{"name": "ZG9ja2Vy", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:dir:b8fa5f3d9e738b3919288177d545ceb731b4979e",
                        "label": [{"name": "cmVwb3J0cw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:0db890fdde68d478e5ba043f0e542291e8b0b54b",
                        "label": [{"name": "Q2FyZ28ubG9jaw==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:5e2536575335d59c4ac72043b55829538f6808c2",
                        "label": [
                            {
                                "name": "LmNvcGllci1hbnN3ZXJzLnltbA==",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:63fd1d0de44bb7608e931c749375db72cdba8a2d",
                        "label": [{"name": "c2V0dXAuY2Zn", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:982caf51e401acbaa8148c2037666cb8a0cea3c1",
                        "label": [{"name": "dG94LmluaQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:84f00683b1a2aabcdedfbbb037c60e2cad7b896f",
                        "label": [
                            {"name": "cHlwcm9qZWN0LnRvbWw=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:d05318c30b051223c74df4efbc9a834b80170692",
                        "label": [
                            {"name": "TWFrZWZpbGUubG9jYWw=", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:c017966a0e7e3e27093f6f877f487dc5cfeab769",
                        "label": [{"name": "cHl0ZXN0LmluaQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:da1ff9757b6ee4ec9a2c301429fdb3db20f3e8c9",
                        "label": [{"name": "cHlvMw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:6f99f9cce04d231c27aff30b29348ff0fb5e55d0",
                        "label": [{"name": "Q2FyZ28udG9tbA==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:b0fcc18eabf59b8a210601ead1b13608e673c68f",
                        "label": [{"name": "dG9vbHM=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:de363f304f4d77d635657649307d0bbab0d6b003",
                        "label": [
                            {
                                "name": "cmVxdWlyZW1lbnRzLXRlc3QudHh0",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:c2518229482a670e9e42e32addbaa1866a979c2e",
                        "label": [{"name": "Q09OVFJJQlVUT1JT", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:32e4c4b9383bdd6de264c7adb6c940a4735cd45f",
                        "label": [{"name": "Lm1haWxtYXA=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:76223786dcaf6d9cce703e224c0c895c3f87a0cd",
                        "label": [{"name": "bXlweS5pbmk=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:c702dcbf5b6d17160c6c62f78b2d1ba1ce8c9945",
                        "label": [
                            {
                                "name": "cmVxdWlyZW1lbnRzLXN3aC10ZXN0LnR4dA==",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:35a070b2739a7091a888a3fa6d05f2f1ec622c9e",
                        "label": [
                            {"name": "cmVxdWlyZW1lbnRzLnR4dA==", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:8ff7d1d0488f445654a7903b0eab52ca8876771b",
                        "label": [{"name": "cHJvdG8=", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:347a1be25a18cf817291e8c21050bf05321813a8",
                        "label": [
                            {
                                "name": "cmVxdWlyZW1lbnRzLXN3aC50eHQ=",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:fc7439c4c42752076a103d24769894c64f85a1d3",
                        "label": [{"name": "ZG9jcw==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:5764e8839103150489b184184aaeb890e12744c7",
                        "label": [
                            {
                                "name": "LnByZS1jb21taXQtY29uZmlnLnlhbWw=",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:2b0e640212e420a1fdc44e68d9da10694eb50f10",
                        "label": [{"name": "c3do", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:6e2dee929af68f2187dbdadc16fe2028fcfe9dfc",
                        "label": [
                            {"name": "Q09ERV9PRl9DT05EVUNULm1k", "permission": 33188}
                        ],
                    },
                    {
                        "swhid": "swh:1:cnt:61c58b5d0f97f579e4cb97f6b937932fcb5ebcaa",
                        "label": [
                            {
                                "name": "cmVxdWlyZW1lbnRzLWx1aWdpLnR4dA==",
                                "permission": 33188,
                            }
                        ],
                    },
                    {
                        "swhid": "swh:1:dir:a48a042899bf075849016c41e84c52388cd7495d",
                        "label": [{"name": "cnVzdA==", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:d7abf98f65283673a669eeba114f545934153f5d",
                        "label": [{"name": "UkVBRE1FLnJzdA==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:dir:ee848a09897c5c40985e6257cd8dce79d5a1ca5b",
                        "label": [{"name": "LmNhcmdv", "permission": 16384}],
                    },
                    {
                        "swhid": "swh:1:cnt:5876d2dcbe8e493b9a7a060a0a1cc9e1bbc76535",
                        "label": [{"name": "LmdpdGlnbm9yZQ==", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:b07918e2110a818adba7db7bd93b2d1f0c95f9df",
                        "label": [{"name": "TWFrZWZpbGU=", "permission": 33188}],
                    },
                    {
                        "swhid": "swh:1:cnt:13beda51bc0e4ab318d33fd9e79fcb8b3f5bb540",
                        "label": [
                            {
                                "name": "cmVxdWlyZW1lbnRzLXN3aC1sdWlnaS50eHQ=",
                                "permission": 33188,
                            }
                        ],
                    },
                ],
                "numSuccessors": "35",
            },
            "rev": {
                "swhid": "swh:1:rev:985dcf705e03fde55285ca8aaff2488f43e9a55f",
                "successor": [
                    {"swhid": "swh:1:dir:1618172adfdc2baef53ff0c27ebbd33899c455b0"},
                    {"swhid": "swh:1:rev:ae81781d7856208349655c434dd788977a99c707"},
                ],
                "rev": {
                    "author": "35558135",
                    "authorDate": "1725351142",
                    "authorDateOffset": 120,
                    "committer": "76456249",
                    "committerDate": "1725358688",
                    "committerDateOffset": 0,
                    "message": "UmVwbGFjZSBtYWR2aXNlIHdpdGggTW1hcEZsYWdzCgpUaGVzZSBmbGFncyB3ZXJlIGFkZGVkIGluIG1tYXBfcnMgdjAuNi4xIGFuZCBzcGFyZSB1cyBzb21lIGJvaWxlcnBsYXRlCg==",  # noqa: B950
                },
                "numSuccessors": "2",
            },
            "snp": {"swhid": "swh:1:snp:d34d87373bb367ba310002693cb7c4c139c3b882"},
        },
    }

    if test_flavor == "full":
        traversal_start = "snp"
        status_table = {
            "parmap": {"cnt": False, "dir": False, "rev": False},
            "apt": {"cnt": False, "dir": False, "rev": False},
            "coreutils": {"cnt": False, "dir": False, "rev": False},
            "pip": {"cnt": False, "dir": False, "rev": False},
            "vim": {"cnt": False, "dir": False, "rev": False},
            "swh-graph": {"cnt": False, "dir": False, "rev": False},
        }
        level_keys = ["rev", "dir", "cnt"]
    elif test_flavor == "history_hosting":
        traversal_start = "snp"
        status_table = {
            "parmap": {"rev": False},
            "apt": {"rev": False},
            "coreutils": {"rev": False},
            "pip": {"rev": False},
            "vim": {"rev": False},
            "swh-graph": {"rev": False},
        }
        level_keys = ["rev"]
    else:  # test_flavor == "example"
        traversal_start = "rev"
        test_values = {
            "example": {
                "dir": {
                    "swhid": "swh:1:dir:0000000000000000000000000000000000000006",
                    "successor": [
                        {
                            "swhid": "swh:1:cnt:0000000000000000000000000000000000000005",
                            "label": [{"name": "bWFpbi5weQ==", "permission": 33188}],
                        },
                        {
                            "swhid": "swh:1:cnt:0000000000000000000000000000000000000004",
                            "label": [
                                {"name": "UkVBRE1FLnJzdA==", "permission": 33188}
                            ],
                        },
                    ],
                    "numSuccessors": "2",
                },
                "rev": {"swhid": "swh:1:rev:0000000000000000000000000000000000000009"},
            }
        }
        status_table = {"example": {"dir": False}}
        level_keys = ["dir"]

    try:
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            stub = swhgraph_grpc.TraversalServiceStub(channel)
            for key, val in test_values.items():
                response = stub.Traverse(
                    swhgraph.TraversalRequest(src=[val[traversal_start]["swhid"]])  # type: ignore
                )
                for elt in response:
                    for l_key in level_keys:
                        if elt.swhid == val[l_key]["swhid"]:
                            elt_dict = MessageToDict(elt)
                            if "successor" in elt_dict:
                                # ensure same ordering in successor lists for comparison
                                for node_dict in (elt_dict, val[l_key]):
                                    assert isinstance(node_dict["successor"], list)
                                    node_dict["successor"] = list(
                                        sorted(
                                            node_dict["successor"],
                                            key=lambda succ: succ["swhid"],
                                        )
                                    )
                            if elt_dict == val[l_key]:
                                status_table[key][l_key] = True
                            else:
                                diff = difflib.context_diff(
                                    json.dumps(
                                        elt_dict, indent=2, sort_keys=True
                                    ).splitlines(True),
                                    json.dumps(
                                        val[l_key], indent=2, sort_keys=True
                                    ).splitlines(True),
                                )
                                logger.error(f"{l_key} mismatch: {''.join(diff)}")
    finally:
        stop_grpc_server(server)

    is_ok = True
    for project, results in status_table.items():
        for level, res in results.items():
            if not res:
                logger.error("Something went wrong with the compression.")
                logger.error(f"{level} for project {project} is incorrect")
                is_ok = False
    if is_ok:
        logger.info("Compression seems to have gone well.")
