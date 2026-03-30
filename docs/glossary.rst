.. _swh-graph-glossary:

swh-graph glossary
==================

Graphs
------

backward graph
    Transposed version of the "normal" graph that goes opposite to |swh|'s Merkle DAG: contents point to directories,
    directories point to their parent directories and to revisions, revisions point to their child revisions and to snapshots,
    and snapshots to origins.

forward graph
    The "normal" graph that follows |swh|'s Merkle DAG: origins point to snapshots, snapshots to revisions and releases,
    revisions to their parent revisions and to directories, directories to contents.

full graph
    A graph with tens of billions of nodes and about a trillion arcs, with all nodes in the |swh| archive.

history-and-hosting graph
    An independently compressed subset of the full graph, that only contains origins, snapshots, releases, revisions;
    and the few directory and content, but does not have their properties (content length, or whether they are skipped).

    Node ids in the history-and-hosting graph are contiguous

    Not to be confused with the "history-and-hosting layer", which is a subset of the full graph with mostly the same nodes.

symmetric graph
    Union of a forward and a corresponding backward graph.
    Rarely used in practice, as it breaks the (very useful) property that |swh| graphs are DAGs.

Labels and properties
---------------------

label
    Extra information on some arcs. In the forward graph, ``ori->snp`` arcs have labels that each contain
    a visit date and a visit date; ``snp->rev`` and ``snp->rel`` arcs have labels that each contain a branch name identifier;
    and ``dir->rev``, ``dir->dir``, and ``dir->cnt`` have labels that each contain a file name identifier and a permission.

property
    Extra information on some nodes. Origins have a URL, releases and revisions have many (message, name, author identifier, ...), and contents have a length.

Subgraphs
---------

.. image:: images/graph_layers.svg


filesystem layer
    A subset of the full graph, that only contains directory and content nodes.

    It is the complement of the "history-and-hosting layer".

history-and-hosting layer
    A subset of the full graph, that only contains origins, snapshots, releases, and revisions nodes.

    It is the complement of the "filesystem layer".

    Not to be confused with the "history-and-hosting graph", which is a subset of the full graph with mostly the same nodes.

Relations between nodes
-----------------------

.. image:: images/node_relations.svg

ancestor
    A node ``A`` is an ancestor of ``N_0`` in a graph ``G`` if there is a chain of nodes ``N_1``, ..., ``N_n``
    such that for all i, ``N_i`` is a predecessor of ``N_{i-1}`` in ``G``.
    In other words, ancestors are the transitive closure of predecessors.
    In the forward graph, all non-orphan nodes have at least one origin as ancestor.

descendant
    A node ``D`` is a descendant of ``N_0`` in a graph ``G`` if there is a chain of nodes ``N_1``, ..., ``N_n``
    such that for all i, ``N_i`` is a successor of ``N_{i-1}`` in ``G``.
    In other words, descendents are the transitive closure of successors.
    In the forward graph, all non-empty nodes have at least one content as descendant.

predecessor
    A node ``P`` is a predecessor of node ``N`` in a graph ``G`` if ``G`` contains the arc ``P -> N``.
    In a forward graph, origins are successors of snapshots, revisions are predecessors of their **parent** revisions,
    directories are predecessor of the subdirectories and contents they contain.

successor
    A node ``S`` is a successor of node ``N`` in a graph ``G`` if ``G`` contains the arc ``N -> S``.
    In a forward graph, snapshots are successors of origins, revisions are successors of their **child** revisions,
    directories are successors of their parent directory, and contents are successors of directories.

"Special" nodes
---------------

empty content
    The content which contains zero bytes. Its SWHID is ``e69de29bb2d1d6434b8b29ae775ad8c2e48c5391``.

empty directory
    The directory which does not have any successor in the forward graph
    (and does not have any predecessor in the backward graph).
    Its SWHID is ``swh:1:dir:4b825dc642cb6eb9a060e54bf8d69288fbee4904``

merge
    A revision with more than one parent revision (ie. more than one successor of type revision, in the forward graph).
    Also called an "octopus merge" when there are more than two.

root directory of a revision
    The directory that is the successor of a revision in the forward graph. It is guaranteed to be unique.

root revision
    A revision with no parent revision (ie. no successor of type revision, in the forward graph).
    ``rev1`` in the images above.
