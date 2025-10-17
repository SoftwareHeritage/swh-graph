# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Spawns a gRPC server on a newly compressed graph, and checks responses
are as expected."""

from base64 import b64decode, b64encode
from importlib.metadata import version
import json
import logging
from pathlib import Path
import socket
import time
from typing import Literal

import attr
from google.protobuf.field_mask_pb2 import FieldMask
import grpc

from swh.graph.config import check_config_compress
import swh.graph.grpc.swhgraph_pb2 as swhgraph
import swh.graph.grpc.swhgraph_pb2_grpc as swhgraph_grpc
from swh.graph.grpc_server import spawn_rust_grpc_server, stop_grpc_server
from swh.graph.shell import Rust, Sink
from swh.model.swhids import CoreSWHID, QualifiedSWHID

e2e_logger = logging.getLogger(__name__)


def run_e2e_check(
    graph_name: str,
    in_dir: str | None,
    out_dir: str | None,
    sensitive_in_dir: str | None,
    sensitive_out_dir: str | None,
    check_flavor: Literal["full", "history_hosting", "staging", "example", "none"],
    profile: Literal["release", "debug"] = "release",
    logger: logging.Logger | None = None,
):
    """Empirically check the graph compression correctness.

    Check for specific SWHIDs in the compressed graph and do simple traversal
    requests to ensure the compression went well.

    Args:
        graph_name: Graph base name, relative to in_dir.
        in_dir: Input directory, where the uncompressed graph can be found.
        out_dir: Output directory, where the compressed graph can be found.
        sensitive_in_dir: Sensitive input directory, where the uncompressed
            sensitive graph can be found.
        sensitive_out_dir: Sensitive output directory, where the compressed
            sensitive graph can be found.
        check_flavor: Which flavor of checks to run.

            - "full": for a dataset made from the entire archive
            - "history_hosting": for a history and hosting dataset
            - "staging": for a dataset made from staging
            - "example": for the example dataset shipped with `swh-graph`
            - "none": for preventing to run the checks altogether

        profile: Which Rust executables to use.

    Raises:
        Exception: GRPC server unexpectedly stopped.
    """
    if logger is None:
        logger = e2e_logger

    conf = check_config_compress(
        {"profile": profile},
        graph_name,
        in_dir,
        out_dir,
        sensitive_in_dir,
        sensitive_out_dir,
        check_flavor,
    )

    graph_name = conf["graph_name"]
    in_dir = conf["in_dir"]
    out_dir = conf["out_dir"]
    check_flavor = conf["check_flavor"]

    meta_path = Path(out_dir) / "meta"
    meta_path.mkdir(exist_ok=True, parents=True)
    meta_data: dict[str, str | bool] = {}

    meta_data["version"] = version("swh.graph")
    meta_data["flavor"] = check_flavor

    if check_flavor == "none":
        logger.info("End-to-end checks skipped.")
        with open(meta_path / "e2e-check.json", "w") as fd:
            json.dump(meta_data, fd, indent=4)
        return

    if "graph_path" not in conf:
        conf["graph_path"] = f"{out_dir}/{graph_name}"

    server, port = spawn_rust_grpc_server(**conf, path=conf["graph_path"])

    # wait for the server to accept connections
    while True:
        try:
            socket.create_connection(("localhost", port))
        except ConnectionRefusedError:
            time.sleep(0.1)
            server.poll()
            if server.returncode is not None:
                raise Exception("GRPC server unexpectedly stopped.")
        else:
            break

    # This dictionary is a copy of a few items retrieved from the graph.
    # It includes a few well known projects, such as `parmap`, `apt`, or `vim`.
    # They should not change in the future, as all traversals start from fixed snapshots.
    # The keys of this dictionary are the origins of the aforementioned projects.
    # For each origin the there is a dictionary where the keys are the SWHIDs of snapshots
    # of said origin.
    # For each snapshot, there is a list of SWHIDs that should be found in this snapshot.
    # The revisions all have the same author, but not necessarily the same committer.
    # The directory is the root directory of the project at the latest revision found in its
    # corresponding snapshot. The fact that it is the root directory is not checked here.
    # The content is the README of the project at the latest revision found in its
    # corresponding snapshot. The fact that it's a README is not checked here.
    projects: dict[str, dict[str, list[str]]] = {
        "https://github.com/rdicosmo/parmap": {
            "swh:1:snp:01b2cc89f4c423f1bda4757edd86ae4013b919b0": [
                "swh:1:rev:963608763589e03de38e744d359884d491e65460",
                "swh:1:rev:db44dc9cf7a6af7b56d8ebda8c75be3375c89282",
                "swh:1:rev:1846939e731b0f5552e2cdf3b0667042cb3d5b30",
                "swh:1:rev:ba9a61336ca4ad78012566e506eebf343ef1ac1f",
                "swh:1:rev:36089bc12f3560faffeeb66d3f38594978561c70",
                "swh:1:dir:2dc0f462d191524530f5612d2935851505af41dd",
                "swh:1:cnt:43243e2ae91a64e252170cd922718e8c2af323b6",
            ],
        },
        "https://github.com/pallets/flask": {
            "swh:1:snp:e8a88d27c5f999868004a3186dc753f6b64f52fa": [
                "swh:1:rev:4fe0aebab79a092615f5f86a24b91bac07fb2ef2",
                "swh:1:rev:e3535f99717bdf3882c0b48bff3e3eb244454e34",
                "swh:1:rev:f567ab90681235f38bdeaeda004d89e85b8627d9",
                "swh:1:rev:7621b3d96ac2d53846c4df211bc05c5e13420028",
                "swh:1:rev:e165f3aef4dbede5d7e3f71c336c89a268753ae9",
                "swh:1:dir:a098f5ea0c6dc036256d936d6da119d28dcf8e86",
                "swh:1:cnt:df4d41cbd3779eb7f865f3b357546acdbc59fa00",
            ],
        },
        "https://github.com/home-assistant/core": {
            "swh:1:snp:4a4962aaf6939bcc2a50f5d3f8fef1fb8ae482db": [
                "swh:1:rev:fa8284d360e64037aa06b1081bdb0f655d9d7be8",
                "swh:1:rev:9b3f92e265937a18858fef2f7b26a6e3fed8998b",
                "swh:1:rev:8b46c8bf206e860a36351de84ec505e65adc7bfa",
                "swh:1:rev:77d83bffee3c767b0014a3c4296e18002efc1549",
                "swh:1:rev:6df77ef94ba90ba6b3c2c9530be0ba4f61c43558",
                "swh:1:dir:b711bde9af241d46fdf7fc63924eecef717a16ec",
                "swh:1:cnt:85c632f7eb1d1861911c735077699c29840ff728",
            ]
        },
        "https://github.com/neovim/neovim": {
            "swh:1:snp:296e07d432c81a8c34d4418f10848af0ed8cb4b8": [
                "swh:1:rev:d82168e41c21a3a107e52c139abc90dbe41f2010",
                "swh:1:rev:046340303076c3680cf767f01c6748d9dd5c8fa5",
                "swh:1:rev:7db9992445a69df16eb12d349d49ab8968e9a505",
                "swh:1:rev:64c2c7c40da4bafb6f74076b7ffbffb262246c7a",
                "swh:1:rev:788bc12a6f4c5a4627cbc75a2f539bfc622384a2",
                "swh:1:dir:234c61bf6e96643c59a33586039c82b492ca86af",
                "swh:1:cnt:8feb7918886d54844bf9234a314f28675b9a76fc",
            ]
        },
        "https://gitlab.softwareheritage.org/swh/devel/swh-graph": {
            "swh:1:snp:d34d87373bb367ba310002693cb7c4c139c3b882": [
                "swh:1:rev:985dcf705e03fde55285ca8aaff2488f43e9a55f",
                "swh:1:rev:af36dc6f7a6d69fbcceccf7f6ef8cea626e06d55",
                "swh:1:rev:b777c9d0d7405cd25f0cf3f60bb21ec38d6eeb58",
                "swh:1:rev:a894a2b14e892fe3f3c126a0ceb4299020f7877d",
                "swh:1:rev:5109f7788c4a59443dd5beeef010dded3f3a1b70",
                "swh:1:dir:1618172adfdc2baef53ff0c27ebbd33899c455b0",
                "swh:1:cnt:d7abf98f65283673a669eeba114f545934153f5d",
            ],
        },
    }

    # This is a dictionary holding the full names of the authors of the revisions included
    # in the `projects` dictionary.
    fullnames: dict[str, str] = {
        "https://github.com/rdicosmo/parmap": (
            "Um9iZXJ0byBEaSBDb3NtbyA8cm9iZXJ0b0BkaWNvc21vLm9yZz4="
        ),
        "https://github.com/pallets/flask": (
            "ZGVwZW5kYWJvdFtib3RdIDw0OTY5OTMzMytkZXBlbmRhYm90W2JvdF1AdXNlcnMubm9yZXBseS5naXRodWIuY29tPg=="
        ),
        "https://github.com/home-assistant/core": (
            "ZGVwZW5kYWJvdFtib3RdIDw0OTY5OTMzMytkZXBlbmRhYm90W2JvdF1AdXNlcnMubm9yZXBseS5naXRodWIuY29tPg=="
        ),
        "https://github.com/neovim/neovim": (
            "ZGVwZW5kYWJvdFtib3RdIDw0OTY5OTMzMytkZXBlbmRhYm90W2JvdF1AdXNlcnMubm9yZXBseS5naXRodWIuY29tPg=="
        ),
        "https://gitlab.softwareheritage.org/swh/devel/swh-graph": (
            "VmFsZW50aW4gTG9yZW50eiA8dmxvcmVudHpAc29mdHdhcmVoZXJpdGFnZS5vcmc+"
        ),
    }

    errors: list[QualifiedSWHID | tuple[str, int]] = []

    # This is a compressed graph of only the “history and hosting” layer (origins,
    # snapshots, releases, revisions) and the root directory (or rarely content) of every
    # revision/release; but most directories and contents are excluded.
    if check_flavor == "history_hosting":
        for project in projects.values():
            for swhids in project.values():
                for swhid in swhids:
                    if swhid.startswith("swh:1:dir:") or swhid.startswith("swh:1:cnt:"):
                        swhids.remove(swhid)
    # Before October 2025, only `parmap` and `apt` were in staging, so other origins need to
    # be removed for the checks to succeed.
    elif check_flavor == "staging":
        for origin in [
            "https://github.com/pallets/flask",
            "https://github.com/home-assistant/core",
            "https://github.com/neovim/neovim",
            "https://gitlab.softwareheritage.org/swh/devel/swh-graph",
        ]:
            projects.pop(origin)
            fullnames.pop(origin)
    # This is the example graph (`swh/graph/example_dataset/`). It is
    # mostly used for CI and testing purposes.
    elif check_flavor == "example":
        projects = {
            "https://example.com/swh/graph": {
                "swh:1:snp:0000000000000000000000000000000000000022": [
                    "swh:1:rev:0000000000000000000000000000000000000009",
                    "swh:1:dir:0000000000000000000000000000000000000006",
                    "swh:1:cnt:0000000000000000000000000000000000000004",
                ],
            }
        }
        fullnames = {"https://example.com/swh/graph": b64encode(b"bar").decode()}

    # All authors of listed revisions should be the same, but we don't know their IDs
    # so we can only check whether they all have the same ID or not.
    authors: dict[str, int | None] = {}

    try:
        with grpc.insecure_channel(f"localhost:{port}") as channel:
            stub = swhgraph_grpc.TraversalServiceStub(channel)
            for origin, project in projects.items():
                for snp_swhid, swhids in project.items():
                    response = stub.Traverse(
                        swhgraph.TraversalRequest(
                            src=[snp_swhid],
                            mask=FieldMask(paths=["swhid", "rev.author"]),
                        )
                    )
                    for elt in response:
                        if elt.swhid in swhids:
                            swhids.remove(elt.swhid)
                            if elt.swhid.startswith("swh:1:rev:"):
                                # Add the author ID of the revision's author as the value
                                # of `authors[origin]` if there isn't one already, otherwise
                                # checks they are the same.
                                if (
                                    authors.setdefault(origin, elt.rev.author)
                                    != elt.rev.author
                                ):
                                    full_swhid = attr.evolve(
                                        QualifiedSWHID.from_string(elt.swhid),
                                        origin=origin,
                                        visit=CoreSWHID.from_string(snp_swhid),
                                    )
                                    logger.error(f"Author ID for {full_swhid} is wrong")
                                    errors.append(full_swhid)
    finally:
        stop_grpc_server(server)

    check_authors = (
        sensitive_out_dir is not None
        and Path(f"{sensitive_out_dir}/{graph_name}.persons").exists()
        and Path(f"{sensitive_out_dir}/{graph_name}.persons.ef").exists()
    )

    meta_data["authors_checked"] = check_authors

    # Check if the author IDs previously checked match their full names. This is triggered
    # only when the sensitive files containing said full names are present on disk.
    if check_authors:
        for origin, author in authors.items():
            if author is None:
                return
            fullname = (
                (
                    Rust(
                        "swh-graph-person-id-to-name",
                        f"{author}",
                        f"{sensitive_out_dir}/{graph_name}",
                        conf=conf,
                    )
                    > Sink()
                )
                .run()
                .stdout
            )
            if fullname.removesuffix(b"\n") != b64decode(fullnames[origin]):
                logger.error(
                    f"Full name for author ID {author} for project {origin} is incorrect: "
                    f"expected {b64decode(fullnames[origin]).decode()}, "
                    f"got {fullname.decode(errors='replace')}"
                )
                errors.append((origin, author))
    else:
        logger.warn("End-to-end checks for full names skipped")

    with open(meta_path / "e2e-check.json", "w") as fd:
        json.dump(meta_data, fd, indent=4)

    for origin, project in projects.items():
        for snp_swhid, swhids in project.items():
            for swhid in swhids:
                full_swhid = attr.evolve(
                    QualifiedSWHID.from_string(swhid),
                    origin=origin,
                    visit=CoreSWHID.from_string(snp_swhid),
                )
                logger.error(f"{full_swhid} has not been found during the traversal")
                errors.append(full_swhid)

    if errors:
        error_msg = "\n".join([str(error) for error in errors])
        raise Exception(
            "End-to-end checks for compression failed. "
            "The following errors have been detected:\n"
            f"{error_msg}\n"
            "See above logs for more details"
        )
    logger.info("Compression seems to have gone well")
