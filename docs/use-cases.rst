=========
Use cases
=========


This document lists use cases and benchmark scenarios for the Software Heritage
graph service.


Conventions
===========

- **Node identification**: in the following, nodes are always identified by
  their :ref:`SWHIDs <persistent-identifiers>`.


Use cases
=========


Browsing
--------

The following use cases require traversing the *forward graph*.

- **ls**: given a directory node, list (non recursively) all linked nodes of
  type directory and content

  Implementation::

    /graph/neighbors/:DIR_ID?edges=dir:cnt,dir:dir

- **ls -R**: given a directory node, recursively list all linked nodes of type
  directory and content

  Implementation::

    /graph/visit/paths/:DIR_ID?edges=dir:cnt,dir:dir

- **git log**: given a revision node, recursively list all linked nodes of type
  revision

  Implementation::

    /graph/visit/nodes/:REV_ID?edges=rev:rev


Vault
-----

The following use cases require traversing the *forward graph*.

- **tarball** (same as *ls -R* above)

- **git bundle**: given a node, recursively list all linked nodes of any kind

  Implementation::

     /graph/visit/nodes/:NODE_ID?edges=*


Provenance
----------

The following use cases require traversing the *backward (transposed)
graph*.

- **commit provenance**: given a content or directory node, return *a* commit
  whose directory (recursively) contains it

  Implementation::

    /graph/walk/:NODE_ID/rev?direction=backward&edges=dir:dir,cnt:dir,dir:rev

- **complete commit provenance**: given a content or directory node, return
  *all* commits whose directory (recursively) contains it

  Implementation::

    /graph/leaves/:NODE_ID?direction=backward&edges=dir:dir,cnt:dir,dir:rev

- **origin provenance**: given a content, directory, or commit node, return
  *an* origin that has at least one snapshot that (recursively) contains it

  Implementation::

    /graph/walk/:NODE_ID/ori?direction=backward&edges=*

- **complete origin provenance**: given a content, directory, or commit node,
  return *all* origins that have at least one snapshot that (recursively)
  contains it

  Implementation::

    /graph/leaves/:NODE_ID?direction=backward&edges=*

- *SLOC tracking*: left as future work


Provenance statistics
~~~~~~~~~~~~~~~~~~~~~

The following use cases require traversing the *backward (transposed)
graph*.

- **content popularity across commits**: for each content, count the number of
  commits (or *commit popularity*) that link to a directory that (recursively)
  includes it. Plot the distribution of content popularity across commits.

  Implementation: apply *complete commit provenance* to each content node,
  count the returned commits, aggregate.

- **commit popularity across origins**: for each commit, count the number of
  origins (or *origin popularity*) that have a snapshot that (recursively)
  includes it. Plot the distribution of commit popularity across origins.

  Implementation: apply *complete origin provenance* to each commit node, count
  the returned origins, aggregate.

- *SLOC popularity across contents*: left as future work

The following use cases require traversing the *forward graph*.

- **revision size** (as n. of contents) distribution: for each revision, count
  the number of contents that are (recursively) reachable from it. Plot the
  distribution of revision sizes.

- **origin size** (as n. of revisions) distribution: for each origin, count the
  number of revisions that are (recursively) reachable from it. Plot the
  distribution of origin sizes.


Benchmarks
==========

Notes on how to benchmark graph access:

- separate pure-graph timings from timings related to use additional mappings
  (e.g., node types), no matter if the mappings are in-memory or on-disk

- separate in-memory timings from on-disk timings; in particular, separate the
  timing of translating node identifiers between internal integers and SWHIDs

- for each use case that requires a node as input, we will randomize the choice
  of the input node and repeat the experiment a suitable number of times; where
  possible we will aggregate results computing basic statistics (average,
  standard deviation), as well as normalize results w.r.t. the “size” of the
  chosen node (e.g., number of nodes/path length in the resulting visit)


Basic benchmarks
----------------

- **Edge traversal**: given a node, retrieve the first node in its adjacency
  list.

  For reference: Apostolico, Drovandi in *Graph Compression by BFS* report
  times to retrieve the adjacency list of a node (and/or test if an edge exists
  between two nodes) in the 2-3 us range, for the largest graph in their
  experiments (22 M nodes, 600 M edges).


Each use case is a benchmark
----------------------------

In addition to abstract benchmark, we will use each use case above as a
scenario-based benchmark.
