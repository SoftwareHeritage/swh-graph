# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import contextlib
import functools
from swh.graph.backend import Backend
from swh.graph.dot import dot_to_svg, graph_dot, KIND_TO_SHAPE


BASE_URL = "https://archive.softwareheritage.org/browse"
KIND_TO_URL_FRAGMENT = {
    "ori": "/origin/{}",
    "snp": "/snapshot/{}",
    "rel": "/release/{}",
    "rev": "/revision/{}",
    "dir": "/directory/{}",
    "cnt": "/content/sha1_git:{}/",
}


def call_async_gen(generator, *args, **kwargs):
    loop = asyncio.get_event_loop()
    it = generator(*args, **kwargs).__aiter__()
    while True:
        try:
            res = loop.run_until_complete(it.__anext__())
            yield res
        except StopAsyncIteration:
            break


class Neighbors:
    """Neighbor iterator with custom O(1) length method"""

    def __init__(self, graph, iterator, length_func):
        self.graph = graph
        self.iterator = iterator
        self.length_func = length_func

    def __iter__(self):
        return self

    def __next__(self):
        succ = self.iterator.nextLong()
        if succ == -1:
            raise StopIteration
        return GraphNode(self.graph, succ)

    def __len__(self):
        return self.length_func()


class GraphNode:
    """Node in the SWH graph"""

    def __init__(self, graph, node_id):
        self.graph = graph
        self.id = node_id

    def children(self):
        return Neighbors(
            self.graph,
            self.graph.java_graph.successors(self.id),
            lambda: self.graph.java_graph.outdegree(self.id),
        )

    def parents(self):
        return Neighbors(
            self.graph,
            self.graph.java_graph.predecessors(self.id),
            lambda: self.graph.java_graph.indegree(self.id),
        )

    def simple_traversal(self, ttype, direction="forward", edges="*"):
        for node in call_async_gen(
            self.graph.backend.simple_traversal, ttype, direction, edges, self.id
        ):
            yield self.graph[node]

    def leaves(self, *args, **kwargs):
        yield from self.simple_traversal("leaves", *args, **kwargs)

    def visit_nodes(self, *args, **kwargs):
        yield from self.simple_traversal("visit_nodes", *args, **kwargs)

    def visit_edges(self, direction="forward", edges="*"):
        for src, dst in call_async_gen(
            self.graph.backend.visit_edges, direction, edges, self.id
        ):
            yield (self.graph[src], self.graph[dst])

    def visit_paths(self, direction="forward", edges="*"):
        for path in call_async_gen(
            self.graph.backend.visit_paths, direction, edges, self.id
        ):
            yield [self.graph[node] for node in path]

    def walk(self, dst, direction="forward", edges="*", traversal="dfs"):
        for node in call_async_gen(
            self.graph.backend.walk, direction, edges, traversal, self.id, dst
        ):
            yield self.graph[node]

    def _count(self, ttype, direction="forward", edges="*"):
        return self.graph.backend.count(ttype, direction, edges, self.id)

    count_leaves = functools.partialmethod(_count, ttype="leaves")
    count_neighbors = functools.partialmethod(_count, ttype="neighbors")
    count_visit_nodes = functools.partialmethod(_count, ttype="visit_nodes")

    @property
    def pid(self):
        return self.graph.node2pid[self.id]

    @property
    def kind(self):
        return self.pid.split(":")[2]

    def __str__(self):
        return self.pid

    def __repr__(self):
        return "<{}>".format(self.pid)

    def dot_fragment(self):
        swh, version, kind, hash = self.pid.split(":")
        label = "{}:{}..{}".format(kind, hash[0:2], hash[-2:])
        url = BASE_URL + KIND_TO_URL_FRAGMENT[kind].format(hash)
        shape = KIND_TO_SHAPE[kind]
        return '{} [label="{}", href="{}", target="_blank", shape="{}"];'.format(
            self.id, label, url, shape
        )

    def _repr_svg_(self):
        nodes = [self, *list(self.children()), *list(self.parents())]
        dot = graph_dot(nodes)
        svg = dot_to_svg(dot)
        return svg


class Graph:
    def __init__(self, backend, node2pid, pid2node):
        self.backend = backend
        self.java_graph = backend.entry.get_graph()
        self.node2pid = node2pid
        self.pid2node = pid2node

    def stats(self):
        return self.backend.stats()

    @property
    def path(self):
        return self.java_graph.getPath()

    def __len__(self):
        return self.java_graph.getNbNodes()

    def __getitem__(self, node_id):
        if isinstance(node_id, int):
            self.node2pid[node_id]  # check existence
            return GraphNode(self, node_id)
        elif isinstance(node_id, str):
            node_id = self.pid2node[node_id]
            return GraphNode(self, node_id)

    def __iter__(self):
        for pid, pos in self.backend.pid2node:
            yield self[pid]

    def iter_prefix(self, prefix):
        for pid, pos in self.backend.pid2node.iter_prefix(prefix):
            yield self[pid]

    def iter_type(self, pid_type):
        for pid, pos in self.backend.pid2node.iter_type(pid_type):
            yield self[pid]


@contextlib.contextmanager
def load(graph_path):
    with Backend(graph_path) as backend:
        yield Graph(backend, backend.node2pid, backend.pid2node)
