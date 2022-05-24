# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
A proxy HTTP server for swh-graph, talking to the Java code via py4j, and using
FIFO as a transport to stream integers between the two languages.
"""

import json
import os
from typing import Optional

import aiohttp.test_utils
import aiohttp.web
from google.protobuf import json_format
import grpc

from swh.core.api.asynchronous import RPCServerApp
from swh.core.config import read as config_read
from swh.graph.rpc.swhgraph_pb2 import (
    CheckSwhidRequest,
    NodeFields,
    NodeFilter,
    StatsRequest,
    TraversalRequest,
)
from swh.graph.rpc.swhgraph_pb2_grpc import TraversalServiceStub
from swh.graph.rpc_server import spawn_java_rpc_server
from swh.model.swhids import EXTENDED_SWHID_TYPES

try:
    from contextlib import asynccontextmanager
except ImportError:
    # Compatibility with 3.6 backport
    from async_generator import asynccontextmanager  # type: ignore


# maximum number of retries for random walks
RANDOM_RETRIES = 10  # TODO make this configurable via rpc-serve configuration


class GraphServerApp(RPCServerApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_startup.append(self._start)
        self.on_shutdown.append(self._stop)

    @staticmethod
    async def _start(app):
        app["channel"] = grpc.aio.insecure_channel(app["rpc_url"])
        await app["channel"].__aenter__()
        app["rpc_client"] = TraversalServiceStub(app["channel"])
        await app["rpc_client"].Stats(StatsRequest(), wait_for_ready=True)

    @staticmethod
    async def _stop(app):
        await app["channel"].__aexit__(None, None, None)
        if app.get("local_server"):
            app["local_server"].terminate()


async def index(request):
    return aiohttp.web.Response(
        content_type="text/html",
        body="""<html>
<head><title>Software Heritage graph server</title></head>
<body>
<p>You have reached the <a href="https://www.softwareheritage.org/">
Software Heritage</a> graph API server.</p>

<p>See its
<a href="https://docs.softwareheritage.org/devel/swh-graph/api.html">API
documentation</a> for more information.</p>
</body>
</html>""",
    )


class GraphView(aiohttp.web.View):
    """Base class for views working on the graph, with utility functions"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rpc_client: TraversalServiceStub = self.request.app["rpc_client"]

    def get_direction(self):
        """Validate HTTP query parameter `direction`"""
        s = self.request.query.get("direction", "forward")
        if s not in ("forward", "backward"):
            raise aiohttp.web.HTTPBadRequest(text=f"invalid direction: {s}")
        return s.upper()

    def get_edges(self):
        """Validate HTTP query parameter `edges`, i.e., edge restrictions"""
        s = self.request.query.get("edges", "*")
        if any(
            [
                node_type != "*" and node_type not in EXTENDED_SWHID_TYPES
                for edge in s.split(":")
                for node_type in edge.split(",", maxsplit=1)
            ]
        ):
            raise aiohttp.web.HTTPBadRequest(text=f"invalid edge restriction: {s}")
        return s

    def get_return_types(self):
        """Validate HTTP query parameter 'return types', i.e,
        a set of types which we will filter the query results with"""
        s = self.request.query.get("return_types", "*")
        if any(
            node_type != "*" and node_type not in EXTENDED_SWHID_TYPES
            for node_type in s.split(",")
        ):
            raise aiohttp.web.HTTPBadRequest(
                text=f"invalid type for filtering res: {s}"
            )
        # if the user puts a star,
        # then we filter nothing, we don't need the other information
        if "*" in s:
            return "*"
        else:
            return s

    def get_traversal(self):
        """Validate HTTP query parameter `traversal`, i.e., visit order"""
        s = self.request.query.get("traversal", "dfs")
        if s not in ("bfs", "dfs"):
            raise aiohttp.web.HTTPBadRequest(text=f"invalid traversal order: {s}")
        return s

    def get_limit(self):
        """Validate HTTP query parameter `limit`, i.e., number of results"""
        s = self.request.query.get("limit", "0")
        try:
            return int(s)
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(text=f"invalid limit value: {s}")

    def get_max_edges(self):
        """Validate HTTP query parameter 'max_edges', i.e.,
        the limit of the number of edges that can be visited"""
        s = self.request.query.get("max_edges", "0")
        try:
            return int(s)
        except ValueError:
            raise aiohttp.web.HTTPBadRequest(text=f"invalid max_edges value: {s}")

    async def check_swhid(self, swhid):
        """Validate that the given SWHID exists in the graph"""
        r = await self.rpc_client.CheckSwhid(CheckSwhidRequest(swhid=swhid))
        if not r.exists:
            raise aiohttp.web.HTTPBadRequest(text=str(r.details))


class StreamingGraphView(GraphView):
    """Base class for views streaming their response line by line."""

    content_type = "text/plain"

    @asynccontextmanager
    async def response_streamer(self, *args, **kwargs):
        """Context manager to prepare then close a StreamResponse"""
        response = aiohttp.web.StreamResponse(*args, **kwargs)
        response.content_type = self.content_type
        await response.prepare(self.request)
        yield response
        await response.write_eof()

    async def get(self):
        await self.prepare_response()
        async with self.response_streamer() as self.response_stream:
            self._buf = []
            try:
                await self.stream_response()
            finally:
                await self._flush_buffer()
            return self.response_stream

    async def prepare_response(self):
        """This can be overridden with some setup to be run before the response
        actually starts streaming.
        """
        pass

    async def stream_response(self):
        """Override this to perform the response streaming. Implementations of
        this should await self.stream_line(line) to write each line.
        """
        raise NotImplementedError

    async def stream_line(self, line):
        """Write a line in the response stream."""
        self._buf.append(line)
        if len(self._buf) > 100:
            await self._flush_buffer()

    async def _flush_buffer(self):
        await self.response_stream.write("\n".join(self._buf).encode() + b"\n")
        self._buf = []


class StatsView(GraphView):
    """View showing some statistics on the graph"""

    async def get(self):
        res = await self.rpc_client.Stats(StatsRequest())
        stats = json_format.MessageToDict(
            res, including_default_value_fields=True, preserving_proto_field_name=True
        )
        # Int64 fields are serialized as strings by default.
        for descriptor in res.DESCRIPTOR.fields:
            if descriptor.type == descriptor.TYPE_INT64:
                try:
                    stats[descriptor.name] = int(stats[descriptor.name])
                except KeyError:
                    pass
        json_body = json.dumps(stats, indent=4, sort_keys=True)
        return aiohttp.web.Response(body=json_body, content_type="application/json")


class SimpleTraversalView(StreamingGraphView):
    """Base class for views of simple traversals"""

    async def prepare_response(self):
        src = self.request.match_info["src"]
        self.traversal_request = TraversalRequest(
            src=[src],
            edges=self.get_edges(),
            direction=self.get_direction(),
            return_nodes=NodeFilter(types=self.get_return_types()),
            return_fields=NodeFields(),
        )
        if self.get_max_edges():
            self.traversal_request.max_edges = self.get_max_edges()
        await self.check_swhid(src)
        self.configure_request()

    def configure_request(self):
        pass

    async def stream_response(self):
        async for node in self.rpc_client.Traverse(self.traversal_request):
            await self.stream_line(node.swhid)


class LeavesView(SimpleTraversalView):
    def configure_request(self):
        self.traversal_request.return_nodes.max_traversal_successors = 0


class NeighborsView(SimpleTraversalView):
    def configure_request(self):
        self.traversal_request.min_depth = 1
        self.traversal_request.max_depth = 1


class VisitNodesView(SimpleTraversalView):
    pass


class VisitEdgesView(SimpleTraversalView):
    def configure_request(self):
        self.traversal_request.return_fields.successor = True

    async def stream_response(self):
        async for node in self.rpc_client.Traverse(self.traversal_request):
            for succ in node.successor:
                await self.stream_line(node.swhid + " " + succ.swhid)


class CountView(GraphView):
    """Base class for counting views."""

    count_type: Optional[str] = None

    async def get(self):
        src = self.request.match_info["src"]
        self.traversal_request = TraversalRequest(
            src=[src],
            edges=self.get_edges(),
            direction=self.get_direction(),
            return_nodes=NodeFilter(types=self.get_return_types()),
            return_fields=NodeFields(),
        )
        if self.get_max_edges():
            self.traversal_request.max_edges = self.get_max_edges()
        self.configure_request()
        res = await self.rpc_client.CountNodes(self.traversal_request)
        return aiohttp.web.Response(
            body=str(res.count), content_type="application/json"
        )

    def configure_request(self):
        pass


class CountNeighborsView(CountView):
    def configure_request(self):
        self.traversal_request.min_depth = 1
        self.traversal_request.max_depth = 1


class CountLeavesView(CountView):
    def configure_request(self):
        self.traversal_request.return_nodes.max_traversal_successors = 0


class CountVisitNodesView(CountView):
    pass


def make_app(config=None, rpc_url=None, **kwargs):
    app = GraphServerApp(**kwargs)

    if rpc_url is None:
        app["local_server"], port = spawn_java_rpc_server(config)
        rpc_url = f"localhost:{port}"

    app.add_routes(
        [
            aiohttp.web.get("/", index),
            aiohttp.web.get("/graph", index),
            aiohttp.web.view("/graph/stats", StatsView),
            aiohttp.web.view("/graph/leaves/{src}", LeavesView),
            aiohttp.web.view("/graph/neighbors/{src}", NeighborsView),
            aiohttp.web.view("/graph/visit/nodes/{src}", VisitNodesView),
            aiohttp.web.view("/graph/visit/edges/{src}", VisitEdgesView),
            aiohttp.web.view("/graph/neighbors/count/{src}", CountNeighborsView),
            aiohttp.web.view("/graph/leaves/count/{src}", CountLeavesView),
            aiohttp.web.view("/graph/visit/nodes/count/{src}", CountVisitNodesView),
        ]
    )

    app["rpc_url"] = rpc_url
    return app


def make_app_from_configfile():
    """Load configuration and then build application to run"""
    config_file = os.environ.get("SWH_CONFIG_FILENAME")
    config = config_read(config_file)
    return make_app(config=config)
