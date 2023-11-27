# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging

from swh.core.api import RPCClient

logger = logging.getLogger(__name__)


class GraphAPIError(Exception):
    """Graph API Error"""

    def __str__(self):
        return """An unexpected error occurred
               in the Graph backend: {}""".format(
            self.args
        )


class GraphArgumentException(Exception):
    def __init__(self, *args, response=None):
        super().__init__(*args)
        self.response = response


class RemoteGraphClient(RPCClient):
    """Client to the Software Heritage Graph."""

    def __init__(self, url, timeout=None):
        super().__init__(api_exception=GraphAPIError, url=url, timeout=timeout)
        try:
            stats = self.stats()
        except GraphArgumentException as e:
            if e.response.status_code == 404:
                raise ValueError(
                    "URL is incorrect (got 404 while trying to retrieve stats)"
                ) from None
            raise
        if "num_nodes" not in stats:
            raise ValueError("stats returned unexpected results (no `num_nodes` entry)")
        if "export_started_at" in stats:
            from datetime import datetime, timezone

            logger.debug(
                "Graph export started at %s (%d nodes)",
                datetime.fromtimestamp(
                    int(stats["export_started_at"]), tz=timezone.utc
                ).isoformat(),
                stats["num_nodes"],
            )

    def raw_verb_lines(self, verb, endpoint, **kwargs):
        response = self.raw_verb(verb, endpoint, stream=True, **kwargs)
        self.raise_for_status(response)
        for line in response.iter_lines():
            content = line.decode().lstrip("\n")
            if content:
                yield content

    def get_lines(self, endpoint, **kwargs):
        yield from self.raw_verb_lines("get", endpoint, **kwargs)

    def raise_for_status(self, response) -> None:
        if response.status_code // 100 == 4:
            raise GraphArgumentException(
                response.content.decode("ascii"), response=response
            )
        super().raise_for_status(response)

    # Web API endpoints

    def stats(self):
        return self._get("stats")

    def leaves(
        self,
        src,
        edges="*",
        direction="forward",
        max_edges=0,
        return_types="*",
        max_matching_nodes=0,
    ):
        return self.get_lines(
            "leaves/{}".format(src),
            params={
                "edges": edges,
                "direction": direction,
                "max_edges": max_edges,
                "return_types": return_types,
                "max_matching_nodes": max_matching_nodes,
            },
        )

    def neighbors(
        self, src, edges="*", direction="forward", max_edges=0, return_types="*"
    ):
        return self.get_lines(
            "neighbors/{}".format(src),
            params={
                "edges": edges,
                "direction": direction,
                "max_edges": max_edges,
                "return_types": return_types,
            },
        )

    def visit_nodes(
        self,
        src,
        edges="*",
        direction="forward",
        max_edges=0,
        return_types="*",
        max_matching_nodes=0,
    ):
        return self.get_lines(
            "visit/nodes/{}".format(src),
            params={
                "edges": edges,
                "direction": direction,
                "max_edges": max_edges,
                "return_types": return_types,
                "max_matching_nodes": max_matching_nodes,
            },
        )

    def visit_edges(self, src, edges="*", direction="forward", max_edges=0):
        for edge in self.get_lines(
            "visit/edges/{}".format(src),
            params={"edges": edges, "direction": direction, "max_edges": max_edges},
        ):
            yield tuple(edge.split())

    def visit_paths(self, src, edges="*", direction="forward", max_edges=0):
        def decode_path_wrapper(it):
            for e in it:
                yield json.loads(e)

        return decode_path_wrapper(
            self.get_lines(
                "visit/paths/{}".format(src),
                params={"edges": edges, "direction": direction, "max_edges": max_edges},
            )
        )

    def walk(
        self, src, dst, edges="*", traversal="dfs", direction="forward", limit=None
    ):
        endpoint = "walk/{}/{}"
        return self.get_lines(
            endpoint.format(src, dst),
            params={
                "edges": edges,
                "traversal": traversal,
                "direction": direction,
                "limit": limit,
            },
        )

    def random_walk(
        self, src, dst, edges="*", direction="forward", limit=None, return_types="*"
    ):
        endpoint = "randomwalk/{}/{}"
        return self.get_lines(
            endpoint.format(src, dst),
            params={
                "edges": edges,
                "direction": direction,
                "limit": limit,
                "return_types": return_types,
            },
        )

    def count_leaves(self, src, edges="*", direction="forward", max_matching_nodes=0):
        return self._get(
            "leaves/count/{}".format(src),
            params={
                "edges": edges,
                "direction": direction,
                "max_matching_nodes": max_matching_nodes,
            },
        )

    def count_neighbors(self, src, edges="*", direction="forward"):
        return self._get(
            "neighbors/count/{}".format(src),
            params={"edges": edges, "direction": direction},
        )

    def count_visit_nodes(
        self, src, edges="*", direction="forward", max_matching_nodes=0
    ):
        return self._get(
            "visit/nodes/count/{}".format(src),
            params={
                "edges": edges,
                "direction": direction,
                "max_matching_nodes": max_matching_nodes,
            },
        )
