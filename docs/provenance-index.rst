.. _provenance-index:

================
Provenance index
================

:ref:`swh-graph <swh-graph>` can be used to generate the "Provenance Index", which is
a set of Parquet tables that allow efficiently computing the set of revisions any given
content (ie. file) is in.

Primer on Parquet
=================

`Parquet <https://parquet.apache.org/>`_ is a column-oriented file format that allows
efficient batch requests. Column-oriented means that instead of storing rows sequentially,
rows are grouped by batches of about 1M rows (by default), and a Parquet file stores all
values of column 1 of these rows, followed by all values of column 2 of these rows, etc.
This allows more efficient compression, and avoids decoding columns we are not interested in.

While it does not provide row-level indexes like "regular databases" (aka. OLTP)
like PostgreSQL, it provides two mechanism to filter out row groups before decoding them:

- statistics, which store the maximum and minimum value of a column within a row group
  or a `page <https://parquet.apache.org/docs/file-format/pageindex/>`__ (1MB), useful
  for sorted/sequential data
- `bloom filters <https://en.wikipedia.org/wiki/Bloom_filter>`_, which allow
  probabilistically filtering out most row groups when looking for a specific set of
  values, useful for high-cardinality non-sequential data.

Frontier directories
====================

The naive implementation of the provenance index would be to store, for each content,
the set of all revisions and releases that contain it, possibly sorted by date.

However, this would take unrealistic amounts of spaces, so we use an intermediate layer
to keep the combinatorial explosion under control.
This layer is a set of directories called "frontier directories", which are computed
through a heuristic aimed at finding directories that are both in many revisions
and contain many contents.

Currently, the heuristic is defined as: a directory ``D`` is frontier for a
revision/release ``R`` if and only iff it follows all three of these conditions:

- there is at least one content ``C`` directly in ``D``
- for all content ``C`` in ``D``, the date of first occurrence of ``C`` predates
  the authorship date of ``R``
- ``D`` is not the root directory of ``R``

Tables
======

Node id - SWHID map
-------------------

The :file:`nodes` table allows mapping the ids (space-efficient but unstable)
used by other tables to SWHIDs (stable but 21-bytes long) contains three columns:

- ``id``, a 64-bits unsigned integer (with `DELTA_BINARY_PACKED`_ encoding)
- ``type``, a string among ``cnt``, ``dir``, ``rev``, or ``rel``
- ``sha1_git``, a 20-bytes binary string

``type`` and ``sha1_git`` together represent a :ref:`SWHID <persistent-identifiers>` (v1)

Within each file of the table, rows are ordered in such a way that consecutive rows
have ``id`` in a mostly monotonic order, allowing fast access (50ms) thanks to the page
index.

The ``sha1_git`` column has Bloom filters, allowing decent access performance (600ms,
as opposed to 20s without).


content-in-directory
--------------------

For each content, this table stores the list of directories that contain it. Columns:

- ``cnt``, a 64-bits id of the content
- ``dir``, a 64-bits id of the directory
- ``path``, a byte string of the filesystem path from the directory to the content
  (with `DELTA_BYTE_ARRAY`_ encoding; TODO: add/replace with zstd compression?)

Within each file of the table, rows are ordered in such a way that consecutive rows
have ``dir`` in a mostly monotonic order.

directory-in-revision
---------------------

For each directory that is frontier for **any** revision, this table stores the list
of **all** revisions the directory is in. Columns:

- ``dir``, a 64-bits id of the directory
- ``dir_max_author_date``, the date of the newest content in the directory
- ``revrel``, a 64-bits id of the revision
- ``revrel_author_date``, the authorship date of the revision
- ``path``, a byte string of the filesystem path from the revision to the directory
  (with `DELTA_BYTE_ARRAY`_ encoding; TODO: add/replace with zstd compression?)

Within each file of the table, rows are ordered in such a way that consecutive rows
have ``revrel`` in a mostly monotonic order.

content-in-revision
-------------------

Contains the list of revisions that each content is in, that would not be covered
by the other two tables (ie. the content is contained from a revision, without
any path going through a frontier directory).

Columns:

- ``cnt``, a 64-bits id of the directory
- ``revrel``, a 64-bits id of the revision
- ``revrel_author_date``, the authorship date of the revision
- ``path``, a byte string of the filesystem path from the revision to the directory
  (with `DELTA_BYTE_ARRAY`_ encoding; TODO: add/replace with zstd compression?)

Within each file of the table, rows are ordered in such a way that consecutive rows
have ``revrel`` in a mostly monotonic order.

To simplify index generation, this table contains some ``(content, revision)``
relationships where the ``content`` is reachable from the ``revision`` through
a frontier directory. This happens when there are multiple paths from the ``revision``
to the ``content`` and only some of them go through a frontier directory.

.. _DELTA_BINARY_PACKED: https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-encoding-delta_binary_packed--5
.. _DELTA_BYTE_ARRAY: https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-strings-delta_byte_array--7
