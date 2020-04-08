# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import lru_cache
import subprocess
import collections


KIND_TO_SHAPE = {
    "ori": "egg",
    "snp": "doubleoctagon",
    "rel": "octagon",
    "rev": "diamond",
    "dir": "folder",
    "cnt": "oval",
}


@lru_cache()
def dot_to_svg(dot):
    try:
        p = subprocess.run(
            ["dot", "-Tsvg"],
            input=dot,
            universal_newlines=True,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(e.stderr) from e
    return p.stdout


def graph_dot(nodes):
    ids = {n.id for n in nodes}

    by_kind = collections.defaultdict(list)
    for n in nodes:
        by_kind[n.kind].append(n)

    forward_edges = [
        (node.id, child.id)
        for node in nodes
        for child in node.children()
        if child.id in ids
    ]
    backward_edges = [
        (parent.id, node.id)
        for node in nodes
        for parent in node.parents()
        if parent.id in ids
    ]
    edges = set(forward_edges + backward_edges)
    edges_fmt = "\n".join("{} -> {};".format(a, b) for a, b in edges)
    nodes_fmt = "\n".join(node.dot_fragment() for node in nodes)

    s = """digraph G {{
    ranksep=1;
    nodesep=0.5;

    {nodes}
    {edges}

    }}""".format(
        nodes=nodes_fmt, edges=edges_fmt
    )
    return s
