# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
from pathlib import Path
from typing import Dict, List

import luigi
import requests

from swh.export.luigi import (
    AthenaDatabaseTarget,
    CreateAthena,
    ObjectType,
    S3PathParameter,
    merge_lists,
)

from .compressed_graph import _tables_for_object_types


class SelectTopGithubOrigins(luigi.Task):
    """Writes a list of origins selected from popular Github repositories"""

    local_export_path = luigi.PathParameter()
    num_origins = luigi.IntParameter(default=1)
    query = luigi.Parameter(
        default="language:python",
        description="Search query to use to filter Github repositories",
    )

    def output(self) -> luigi.LocalTarget:
        """Text file with a list of origin URLs"""
        return luigi.LocalTarget(self.local_export_path / "origins.txt")

    def run(self) -> None:
        """Sends a query to the Github API to get a list of origins"""
        import math

        urls: List[str] = []
        for i in range(0, math.ceil(self.num_origins / 100)):
            resp = requests.get(
                "https://api.github.com/search/repositories",
                params=dict(  # type: ignore[arg-type]
                    page=i,
                    s="starts",
                    order="desc",
                    q=self.query,
                    per_page=min(100, self.num_origins - len(urls)),
                ),
            )
            urls.extend(repo["html_url"] for repo in resp.json()["items"])

        with self.output().open("wt") as fd:
            for url in urls:
                fd.write(f"{url}\n")


class SubdatasetOriginsFromFile(luigi.Task):
    """Reads a list of origins from a local file, computed externally to Luigi."""

    local_export_path = luigi.PathParameter()
    path = luigi.Parameter(
        default="",
        description="What file to read origins from. "
        "Defaults to local_export_path / origins.txt",
    )

    def output(self) -> luigi.LocalTarget:
        """Text file with a list of origin URLs"""
        return luigi.LocalTarget(self.path or self.local_export_path / "origins.txt")

    def run(self) -> None:
        """Does nothing"""
        pass


class ListSwhidsForSubdataset(luigi.Task):
    """Lists all SWHIDs reachable from a set of origins"""

    select_task = luigi.ChoiceParameter(
        choices=["SelectTopGithubOrigins", "SubdatasetOriginsFromFile"],
        default="SelectTopGithubOrigins",
        description="Which algorithm to use to generate the list of origins",
    )
    local_export_path = luigi.PathParameter()
    grpc_api = luigi.Parameter()

    def requires(self) -> luigi.Task:
        """Returns an instance of ``self.select_task``"""
        return globals()[self.select_task](local_export_path=self.local_export_path)

    def output(self) -> luigi.LocalTarget:
        """Text file with a list of SWHIDs"""
        return luigi.LocalTarget(self.local_export_path / "swhids.txt")

    def run(self) -> None:
        """Builds the list"""
        import hashlib

        from google.protobuf.field_mask_pb2 import FieldMask
        import grpc
        import tqdm

        import swh.graph.grpc.swhgraph_pb2 as swhgraph
        import swh.graph.grpc.swhgraph_pb2_grpc as swhgraph_grpc

        with self.input().open("r") as fd:
            origin_urls = [line.strip() for line in fd]

        origin_swhids = [
            f"swh:1:ori:{hashlib.sha1(url.encode()).hexdigest()}" for url in origin_urls
        ]

        with grpc.insecure_channel(self.grpc_api) as channel:
            stub = swhgraph_grpc.TraversalServiceStub(channel)

            known_origin_swhids = []
            for origin_swhid in origin_swhids:
                try:
                    stub.GetNode(swhgraph.GetNodeRequest(swhid=origin_swhid))
                except grpc.RpcError as e:
                    if e.code() != grpc.StatusCode.NOT_FOUND:
                        raise
                else:
                    known_origin_swhids.append(origin_swhid)

            unknown_origins = set(origin_swhids) - set(known_origin_swhids)
            print(f"Filtered out {len(unknown_origins)} unknown origins")

            request = swhgraph.TraversalRequest(
                src=known_origin_swhids,
                mask=FieldMask(paths=["swhid"]),
            )

            with self.output().open("wt") as fd:
                for item in tqdm.tqdm(stub.Traverse(request)):
                    fd.write(f"{item.swhid}\n")


class CreateSubdatasetOnAthena(luigi.Task):
    """Generates an ORC export from an existing ORC export, filtering out SWHIDs
    not in the given list."""

    local_export_path: Path = luigi.PathParameter()
    s3_parent_export_path: str = S3PathParameter(  # type: ignore[assignment]
        description="s3:// URL to the existing complete export",
    )
    s3_export_path: str = S3PathParameter(  # type: ignore[assignment]
        description="s3:// URL to the export to produce",
    )
    s3_athena_output_location = S3PathParameter()
    athena_db_name = luigi.Parameter()
    athena_parent_db_name = luigi.Parameter()
    object_types = luigi.EnumListParameter(
        enum=ObjectType, default=list(ObjectType), batch_method=merge_lists
    )

    def requires(self) -> Dict[str, luigi.Task]:
        """Returns an instance of :class:`ListSwhidsForSubdataset` and one of
        :class:`CreateAthena`"""
        return {
            "swhids": ListSwhidsForSubdataset(local_export_path=self.local_export_path),
            "export": CreateAthena(
                athena_db_name=self.athena_parent_db_name,
                s3_export_path=self.s3_parent_export_path,
            ),
        }

    def output(self) -> Dict[str, luigi.Target]:
        """Returns the S3 location and Athena database for the subdataset"""
        from swh.export.athena import TABLES

        return {
            "orc": self._meta(),
            "athena": AthenaDatabaseTarget(self.athena_db_name, set(TABLES)),
        }

    def _meta(self):
        import luigi.contrib.s3

        return luigi.contrib.s3.S3Target(f"{self.s3_export_path}/meta/export.json")

    def run(self) -> None:
        """Runs a query on Athena, producing files on S3"""
        import datetime
        from importlib.metadata import version
        import json
        import socket

        from swh.export.athena import generate_subdataset

        start_date = datetime.datetime.now(tz=datetime.timezone.utc)
        generate_subdataset(
            self.athena_parent_db_name,
            self.athena_db_name,
            self.s3_export_path.rstrip("/") + "/orc",
            self.input()["swhids"].path,
            self.s3_export_path.rstrip("/") + "/queries",
        )
        end_date = datetime.datetime.now(tz=datetime.timezone.utc)

        with luigi.contrib.s3.S3Target(
            f"{self.s3_parent_export_path}/meta/export.json"
        ).open("r") as fd:
            parent_meta = json.load(fd)

        meta = {
            "flavor": "subdataset",
            "export_start": start_date.isoformat(),
            "export_end": end_date.isoformat(),
            "object_types": _tables_for_object_types(self.object_types),
            "parent": parent_meta,
            "hostname": socket.getfqdn(),
            "tool": {
                "name": "swh.export",
                "version": version("swh.export"),
            },
        }

        with self._meta().open("w") as fd:
            json.dump(meta, fd, indent=4)
