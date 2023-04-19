# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import inspect
import itertools
import re
import statistics
from typing import (
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from swh.model.swhids import CoreSWHID, ExtendedSWHID, ValidationError

from .http_client import GraphArgumentException

_NODE_TYPES = "ori|snp|rel|rev|dir|cnt"
NODES_RE = re.compile(rf"(\*|{_NODE_TYPES})")
EDGES_RE = re.compile(rf"(\*|{_NODE_TYPES}):(\*|{_NODE_TYPES})")


T = TypeVar("T", bound=Callable)
SWHIDlike = Union[CoreSWHID, ExtendedSWHID, str]


def check_arguments(f: T) -> T:
    """Decorator for generic argument checking for methods of NaiveClient.
    Checks ``src`` is a valid and known SWHID, and ``edges`` has the right format."""
    signature = inspect.signature(f)

    @functools.wraps(f)
    def newf(*args, **kwargs):
        __tracebackhide__ = True  # for pytest
        try:
            bound_args = signature.bind(*args, **kwargs)
        except TypeError as e:
            # rethrow the exception from here so pytest doesn't flood the terminal
            # with signature.bind's call stack.
            raise TypeError(*e.args) from None
        self = bound_args.arguments["self"]

        src = bound_args.arguments.get("src")
        if src:
            self._check_swhid(src)

        edges = bound_args.arguments.get("edges")
        if edges:
            if edges != "*" and not EDGES_RE.match(edges):
                raise GraphArgumentException(f"invalid edge restriction: {edges}")

        return_types = bound_args.arguments.get("return_types")
        if return_types:
            if not NODES_RE.match(return_types):
                raise GraphArgumentException(
                    f"invalid return_types restriction: {return_types}"
                )

        return f(*args, **kwargs)

    return newf  # type: ignore


def filter_node_types(node_types: str, nodes: Iterable[str]) -> Iterator[str]:
    if node_types == "*":
        yield from nodes
    else:
        prefixes = tuple(f"swh:1:{type_}:" for type_ in node_types.split(","))
        for node in nodes:
            if node.startswith(prefixes):
                yield node


class NaiveClient:
    """An alternative implementation of the graph server, written in
    pure-python and meant for simulating it in other components' test cases;
    constructed from a list of nodes and (directed) edges, both represented as
    SWHIDs.

    It is NOT meant to be efficient in any way; only to be a very simple
    implementation that provides the same behavior.

    >>> nodes = [
    ...     "swh:1:rev:1111111111111111111111111111111111111111",
    ...     "swh:1:rev:2222222222222222222222222222222222222222",
    ...     "swh:1:rev:3333333333333333333333333333333333333333",
    ... ]
    >>> edges = [
    ...     (
    ...         "swh:1:rev:1111111111111111111111111111111111111111",
    ...         "swh:1:rev:2222222222222222222222222222222222222222",
    ...     ),
    ...     (
    ...         "swh:1:rev:2222222222222222222222222222222222222222",
    ...         "swh:1:rev:3333333333333333333333333333333333333333",
    ...     ),
    ... ]
    >>> c = NaiveClient(nodes=nodes, edges=edges)
    >>> list(c.leaves("swh:1:rev:1111111111111111111111111111111111111111"))
    ['swh:1:rev:3333333333333333333333333333333333333333']
    """

    def __init__(
        self, *, nodes: List[SWHIDlike], edges: List[Tuple[SWHIDlike, SWHIDlike]]
    ):
        self.graph = Graph(nodes, edges)

    def _check_swhid(self, swhid):
        try:
            ExtendedSWHID.from_string(swhid)
        except ValidationError as e:
            raise GraphArgumentException(*e.args) from None
        if swhid not in self.graph.nodes:
            raise GraphArgumentException(f"SWHID not found: {swhid}")

    def stats(self) -> Dict:
        return {
            "num_nodes": len(self.graph.nodes),
            "num_edges": sum(map(len, self.graph.forward_edges.values())),
            "compression_ratio": 1.0,
            "bits_per_edge": 100.0,
            "bits_per_node": 100.0,
            "avg_locality": 0.0,
            "indegree_min": min(map(len, self.graph.backward_edges.values())),
            "indegree_max": max(map(len, self.graph.backward_edges.values())),
            "indegree_avg": statistics.mean(
                map(len, self.graph.backward_edges.values())
            ),
            "outdegree_min": min(map(len, self.graph.forward_edges.values())),
            "outdegree_max": max(map(len, self.graph.forward_edges.values())),
            "outdegree_avg": statistics.mean(
                map(len, self.graph.forward_edges.values())
            ),
            "export_started_at": 1669888200,
            "export_ended_at": 1669899600,
        }

    @check_arguments
    def leaves(
        self,
        src: str,
        edges: str = "*",
        direction: str = "forward",
        max_edges: int = 0,
        return_types: str = "*",
        max_matching_nodes: int = 0,
    ) -> Iterator[str]:
        # TODO: max_edges
        leaves = filter_node_types(
            return_types,
            [
                node
                for node in self.graph.get_subgraph(src, edges, direction)
                if not self.graph.get_filtered_neighbors(node, edges, direction)
            ],
        )

        if max_matching_nodes > 0:
            leaves = itertools.islice(leaves, max_matching_nodes)

        return leaves

    @check_arguments
    def neighbors(
        self,
        src: str,
        edges: str = "*",
        direction: str = "forward",
        max_edges: int = 0,
        return_types: str = "*",
    ) -> Iterator[str]:
        # TODO: max_edges
        yield from filter_node_types(
            return_types, self.graph.get_filtered_neighbors(src, edges, direction)
        )

    @check_arguments
    def visit_nodes(
        self,
        src: str,
        edges: str = "*",
        direction: str = "forward",
        max_edges: int = 0,
        return_types: str = "*",
        max_matching_nodes: int = 0,
    ) -> Iterator[str]:
        # TODO: max_edges
        res = filter_node_types(
            return_types, self.graph.get_subgraph(src, edges, direction)
        )
        if max_matching_nodes > 0:
            res = itertools.islice(res, max_matching_nodes)
        return res

    @check_arguments
    def visit_edges(
        self, src: str, edges: str = "*", direction: str = "forward", max_edges: int = 0
    ) -> Iterator[Tuple[str, str]]:
        if max_edges == 0:
            max_edges = None  # type: ignore
        else:
            max_edges -= 1
        yield from list(self.graph.iter_edges_dfs(direction, edges, src))[:max_edges]

    @check_arguments
    def visit_paths(
        self, src: str, edges: str = "*", direction: str = "forward", max_edges: int = 0
    ) -> Iterator[List[str]]:
        # TODO: max_edges
        for path in self.graph.iter_paths_dfs(direction, edges, src):
            if path[-1] in self.leaves(src, edges, direction):
                yield list(path)

    @check_arguments
    def walk(
        self,
        src: str,
        dst: str,
        edges: str = "*",
        traversal: str = "dfs",
        direction: str = "forward",
        limit: Optional[int] = None,
    ) -> Iterator[str]:
        # TODO: implement algo="bfs"
        # TODO: limit
        match_path: Callable[[str], bool]
        if ":" in dst:
            match_path = dst.__eq__
            self._check_swhid(dst)
        else:
            match_path = lambda node: node.startswith(f"swh:1:{dst}:")  # noqa
        for path in self.graph.iter_paths_dfs(direction, edges, src):
            if match_path(path[-1]):
                if not limit:
                    # 0 or None
                    yield from path
                elif limit > 0:
                    yield from path[0:limit]
                else:
                    yield from path[limit:]

    @check_arguments
    def random_walk(
        self,
        src: str,
        dst: str,
        edges: str = "*",
        direction: str = "forward",
        limit: Optional[int] = None,
    ):
        # TODO: limit
        yield from self.walk(src, dst, edges, "dfs", direction, limit)

    @check_arguments
    def count_leaves(
        self,
        src: str,
        edges: str = "*",
        direction: str = "forward",
        max_matching_nodes: int = 0,
    ) -> int:
        return len(
            list(
                self.leaves(
                    src, edges, direction, max_matching_nodes=max_matching_nodes
                )
            )
        )

    @check_arguments
    def count_neighbors(
        self, src: str, edges: str = "*", direction: str = "forward"
    ) -> int:
        return len(self.graph.get_filtered_neighbors(src, edges, direction))

    @check_arguments
    def count_visit_nodes(
        self,
        src: str,
        edges: str = "*",
        direction: str = "forward",
        max_matching_nodes: int = 0,
    ) -> int:
        res = len(self.graph.get_subgraph(src, edges, direction))
        if max_matching_nodes > 0:
            res = min(max_matching_nodes, res)
        return res


class Graph:
    def __init__(
        self, nodes: List[SWHIDlike], edges: List[Tuple[SWHIDlike, SWHIDlike]]
    ):
        self.nodes = [str(node) for node in nodes]
        self.forward_edges: Dict[str, List[str]] = {}
        self.backward_edges: Dict[str, List[str]] = {}
        for node in nodes:
            self.forward_edges[str(node)] = []
            self.backward_edges[str(node)] = []
        for (src, dst) in edges:
            self.forward_edges[str(src)].append(str(dst))
            self.backward_edges[str(dst)].append(str(src))

    def get_filtered_neighbors(
        self,
        src: str,
        edges_fmt: str,
        direction: str,
    ) -> Set[str]:
        if direction == "forward":
            edges = self.forward_edges
        elif direction == "backward":
            edges = self.backward_edges
        else:
            raise GraphArgumentException(f"invalid direction: {direction}")

        neighbors = edges.get(src, [])

        if edges_fmt == "*":
            return set(neighbors)
        else:
            filtered_neighbors: Set[str] = set()
            for edges_fmt_item in edges_fmt.split(","):
                (src_fmt, dst_fmt) = edges_fmt_item.split(":")
                if src_fmt != "*" and not src.startswith(f"swh:1:{src_fmt}:"):
                    continue
                if dst_fmt == "*":
                    filtered_neighbors.update(neighbors)
                else:
                    prefix = f"swh:1:{dst_fmt}:"
                    filtered_neighbors.update(
                        n for n in neighbors if n.startswith(prefix)
                    )
            return filtered_neighbors

    def get_subgraph(self, src: str, edges_fmt: str, direction: str) -> Set[str]:
        seen = set()
        to_visit = {src}
        while to_visit:
            node = to_visit.pop()
            seen.add(node)
            neighbors = set(self.get_filtered_neighbors(node, edges_fmt, direction))
            new_nodes = neighbors - seen
            to_visit.update(new_nodes)

        return seen

    def iter_paths_dfs(
        self, direction: str, edges_fmt: str, src: str
    ) -> Iterator[Tuple[str, ...]]:
        for (path, node) in DfsSubgraphIterator(self, direction, edges_fmt, src):
            yield path + (node,)

    def iter_edges_dfs(
        self, direction: str, edges_fmt: str, src: str
    ) -> Iterator[Tuple[str, str]]:
        for (path, node) in DfsSubgraphIterator(self, direction, edges_fmt, src):
            if len(path) > 0:
                yield (path[-1], node)


class SubgraphIterator(Iterator[Tuple[Tuple[str, ...], str]]):
    def __init__(self, graph: Graph, direction: str, edges_fmt: str, src: str):
        self.graph = graph
        self.direction = direction
        self.edges_fmt = edges_fmt
        self.seen: Set[str] = set()
        self.src = src

    def more_work(self) -> bool:
        raise NotImplementedError()

    def pop(self) -> Tuple[Tuple[str, ...], str]:
        raise NotImplementedError()

    def push(self, new_path: Tuple[str, ...], neighbor: str) -> None:
        raise NotImplementedError()

    def __next__(self) -> Tuple[Tuple[str, ...], str]:
        # Stores (path, next_node)
        if not self.more_work():
            raise StopIteration()

        (path, node) = self.pop()

        new_path = path + (node,)

        if node not in self.seen:
            neighbors = self.graph.get_filtered_neighbors(
                node, self.edges_fmt, self.direction
            )

            # We want to visit the first neighbor first, and to_visit is a stack;
            # so we need to reversed() the list of neighbors to get it on top
            # of the stack.
            for neighbor in reversed(list(neighbors)):
                self.push(new_path, neighbor)

        self.seen.add(node)
        return (path, node)


class DfsSubgraphIterator(SubgraphIterator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.to_visit: List[Tuple[Tuple[str, ...], str]] = [((), self.src)]

    def more_work(self) -> bool:
        return bool(self.to_visit)

    def pop(self) -> Tuple[Tuple[str, ...], str]:
        return self.to_visit.pop()

    def push(self, new_path: Tuple[str, ...], neighbor: str) -> None:
        self.to_visit.append((new_path, neighbor))
