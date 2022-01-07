# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
A proxy HTTP server for swh-graph, talking to the Java code via py4j, and using
FIFO as a transport to stream integers between the two languages.
"""

import asyncio
from collections import deque
import os
from typing import Optional

import aiohttp.web

from swh.core.api.asynchronous import RPCServerApp
from swh.core.config import read as config_read
from swh.graph.backend import Backend
from swh.model.swhids import EXTENDED_SWHID_TYPES

try:
    from contextlib import asynccontextmanager
except ImportError:
    # Compatibility with 3.6 backport
    from async_generator import asynccontextmanager  # type: ignore


# maximum number of retries for random walks
RANDOM_RETRIES = 5  # TODO make this configurable via rpc-serve configuration


class GraphServerApp(RPCServerApp):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.on_startup.append(self._start_gateway)
        self.on_shutdown.append(self._stop_gateway)

    @staticmethod
    async def _start_gateway(app):
        # Equivalent to entering `with app["backend"]:`
        app["backend"].start_gateway()

    @staticmethod
    async def _stop_gateway(app):
        # Equivalent to exiting `with app["backend"]:` with no error
        app["backend"].stop_gateway()


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
        self.backend = self.request.app["backend"]

    def get_direction(self):
        """Validate HTTP query parameter `direction`"""
        s = self.request.query.get("direction", "forward")
        if s not in ("forward", "backward"):
            raise aiohttp.web.HTTPBadRequest(text=f"invalid direction: {s}")
        return s

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

    def check_swhid(self, swhid):
        """Validate that the given SWHID exists in the graph"""
        try:
            self.backend.check_swhid(swhid)
        except (NameError, ValueError) as e:
            raise aiohttp.web.HTTPBadRequest(text=str(e))


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
        stats = self.backend.stats()
        return aiohttp.web.Response(body=stats, content_type="application/json")


class SimpleTraversalView(StreamingGraphView):
    """Base class for views of simple traversals"""

    simple_traversal_type: Optional[str] = None

    async def prepare_response(self):
        self.src = self.request.match_info["src"]
        self.edges = self.get_edges()
        self.direction = self.get_direction()
        self.max_edges = self.get_max_edges()
        self.return_types = self.get_return_types()
        self.check_swhid(self.src)

    async def stream_response(self):
        async for res_line in self.backend.traversal(
            self.simple_traversal_type,
            self.direction,
            self.edges,
            self.src,
            self.max_edges,
            self.return_types,
        ):
            await self.stream_line(res_line)


class LeavesView(SimpleTraversalView):
    simple_traversal_type = "leaves"


class NeighborsView(SimpleTraversalView):
    simple_traversal_type = "neighbors"


class VisitNodesView(SimpleTraversalView):
    simple_traversal_type = "visit_nodes"


class VisitEdgesView(SimpleTraversalView):
    simple_traversal_type = "visit_edges"


class WalkView(StreamingGraphView):
    async def prepare_response(self):
        self.src = self.request.match_info["src"]
        self.dst = self.request.match_info["dst"]

        self.edges = self.get_edges()
        self.direction = self.get_direction()
        self.algo = self.get_traversal()
        self.limit = self.get_limit()
        self.max_edges = self.get_max_edges()
        self.return_types = self.get_return_types()

        self.check_swhid(self.src)
        if self.dst not in EXTENDED_SWHID_TYPES:
            self.check_swhid(self.dst)

    async def get_walk_iterator(self):
        return self.backend.traversal(
            "walk",
            self.direction,
            self.edges,
            self.algo,
            self.src,
            self.dst,
            self.max_edges,
            self.return_types,
        )

    async def stream_response(self):
        it = self.get_walk_iterator()
        if self.limit < 0:
            queue = deque(maxlen=-self.limit)
            async for res_swhid in it:
                queue.append(res_swhid)
            while queue:
                await self.stream_line(queue.popleft())
        else:
            count = 0
            async for res_swhid in it:
                if self.limit == 0 or count < self.limit:
                    await self.stream_line(res_swhid)
                    count += 1
                else:
                    break


class RandomWalkView(WalkView):
    def get_walk_iterator(self):
        return self.backend.traversal(
            "random_walk",
            self.direction,
            self.edges,
            RANDOM_RETRIES,
            self.src,
            self.dst,
            self.max_edges,
            self.return_types,
        )


class CountView(GraphView):
    """Base class for counting views."""

    count_type: Optional[str] = None

    async def get(self):
        self.src = self.request.match_info["src"]
        self.check_swhid(self.src)

        self.edges = self.get_edges()
        self.direction = self.get_direction()
        self.max_edges = self.get_max_edges()

        loop = asyncio.get_event_loop()
        cnt = await loop.run_in_executor(
            None,
            self.backend.count,
            self.count_type,
            self.direction,
            self.edges,
            self.src,
            self.max_edges,
        )
        return aiohttp.web.Response(body=str(cnt), content_type="application/json")


class CountNeighborsView(CountView):
    count_type = "neighbors"


class CountLeavesView(CountView):
    count_type = "leaves"


class CountVisitNodesView(CountView):
    count_type = "visit_nodes"


def make_app(config=None, backend=None, **kwargs):
    if (config is None) == (backend is None):
        raise ValueError("make_app() expects exactly one of 'config' or 'backend'")
    if backend is None:
        backend = Backend(graph_path=config["graph"]["path"], config=config["graph"])
    app = GraphServerApp(**kwargs)
    app.add_routes(
        [
            aiohttp.web.get("/", index),
            aiohttp.web.get("/graph", index),
            aiohttp.web.view("/graph/stats", StatsView),
            aiohttp.web.view("/graph/leaves/{src}", LeavesView),
            aiohttp.web.view("/graph/neighbors/{src}", NeighborsView),
            aiohttp.web.view("/graph/visit/nodes/{src}", VisitNodesView),
            aiohttp.web.view("/graph/visit/edges/{src}", VisitEdgesView),
            # temporarily disabled in wait of a proper fix for T1969
            # aiohttp.web.view("/graph/walk/{src}/{dst}", WalkView)
            aiohttp.web.view("/graph/randomwalk/{src}/{dst}", RandomWalkView),
            aiohttp.web.view("/graph/neighbors/count/{src}", CountNeighborsView),
            aiohttp.web.view("/graph/leaves/count/{src}", CountLeavesView),
            aiohttp.web.view("/graph/visit/nodes/count/{src}", CountVisitNodesView),
        ]
    )

    app["backend"] = backend
    return app


def make_app_from_configfile():
    """Load configuration and then build application to run

    """
    config_file = os.environ.get("SWH_CONFIG_FILENAME")
    config = config_read(config_file)
    return make_app(config=config)
