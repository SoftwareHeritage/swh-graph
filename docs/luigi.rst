.. _swh-graph-luigi:

Luigi workflows
===============

.. highlight:: bash

The preferred way to create and transfer swh-graph data is through
`Luigi <https://luigi.readthedocs.io/>`_ tasks/workflows rather than a regular CLI.
Those include all common operations in swh-graph, except running the gRPC server (
:program:`swh graph grpc-serve`).

Using Luigi allows automatically building missing prerequisites datasets in order
to build a new dataset, which is common when working with swh-graph data.

The :program:`swh graph luigi` CLI wraps Luigi's CLI to simplify
passing common parameters to tasks.
Command lines usually look like this::

    swh graph luigi <common_parameters> <task_name> -- <direct_task_parameters>

where:

* ``<common_parameters>`` are the parameters exposed by :program:`swh graph luigi`.
  See the :ref:`CLI documentation <swh-graph-cli>` for the list of common parameters
  The most importants are:

  * ``--dataset-name`` is the name of the dataset to work on, which is the date of
    the export, optionally with a suffix (eg. ``2022-12-07`` or
    ``2022-12-07-history-hosting``)
  * ``--base-directory`` which is the root directory for all datasets. It contains
    a subdirectory named after ``--dataset-name``, which contains the data the workflow
    will work with.
  * ``--graph-base-directory``, the location of the compressed graph. To obtain
    reasonable performance, it should be a tmpfs setup as described in the
    :ref:`swh-graph-memory` documentation
  * ``--s3-prefix``, which is usually ``s3://softwareheritage/graph/``
  * ``--athena-prefix`` which is a prefix for tables created in Amazon Athena.
    Actual table names will be this prefix, followed by ``_`` and the dataset name
    (with dashes stripped)
* ``<task_name>`` is a CamelCase (usually) imperative-form name of the last task
  to run (tasks that it depends on will automatically run first)
* ``<direct_task_parameters>`` are parameters directly passed to Luigi.
  In short, there are ``--scheduler-url`` which takes an URL to the `Luigi scheduler
  <https://luigi.readthedocs.io/en/stable/central_scheduler.html>`_
  (or ``--local-scheduler`` if you do not want it), ``--log-level {INFO,DEBUG,...}``
  parameters to the main task (``--kebab-cased-parameter``), and parameters to other tasks
  (``--TaskName-kebab-cased-parameter``).
  See the `Luigi CLI documentation <https://luigi.readthedocs.io/en/stable/running_luigi.html>`_ for details.


.. note::

   Progress report for most tasks is currently displayed only to the standard output,
   and not to the luigi scheduler dashboard.


.. _swh-graph-luigi-graph-export:

Graph export
------------

This section describes tasks which export a graph from the archive to ORC (and/or CSV)
files. This is referred to as the "graph export", not to be confused with the "compressed
graph" (even though both are compressed).

There are three important tasks to deal with the graph export:

* :ref:`swh-graph-luigi-ExportGraph` does the export itself
* :ref:`swh-graph-luigi-RunExportAll` is a pseudo-task which depends on
  :ref:`swh-graph-luigi-ExportGraph`, :ref:`swh-graph-luigi-CreateAthena`,
  and :ref:`swh-graph-luigi-UploadExportToS3`.
* :ref:`swh-graph-luigi-LocalExport` which is a pseudo-task that any other task should
  depend on.
  It can be configured by users to either trigger a new export or download it from S3.

In details:

.. _swh-graph-luigi-ExportGraph:

ExportGraph
^^^^^^^^^^^

Implemented by :class:`swh.dataset.luigi.ExportGraph`.

This consumes from the :ref:`journal <swh-journal>`, and to write a bunch of ORC
(and/or edges CSV) files which contain all data in the |swh| archive.

Example invocation::

    swh graph luigi \
        --base-directory /poolswh/softwareheritage/vlorentz/ \
        --dataset-name 2022-12-07 \
        ExportGraph \
        -- \
        --scheduler-url http://localhost:50092/ \
        --ExportGraph-config ~/luigid/graph.prod.yml \
        --ExportGraph-processes 96

or, equivalently::

    swh graph luigi \
        --base-directory /poolswh/softwareheritage/vlorentz/ \
        --dataset-name 2022-12-07 \
        ExportGraph \
        -- \
        --scheduler-url http://localhost:50092/ \
        --config ~/luigid/graph.prod.yml \
        --processes 96


:file:`~/luigid/graph.prod.yml` must contain at least a :ref:`journal <cli-config-journal>`
block.

.. _swh-graph-luigi-UploadExportToS3:

UploadExportToS3
^^^^^^^^^^^^^^^^

Implemented by :class:`swh.dataset.luigi.UploadExportToS3`.

.. _swh-graph-luigi-DownloadExportFromS3:

DownloadExportFromS3
^^^^^^^^^^^^^^^^^^^^

Implemented by :class:`swh.dataset.luigi.DownloadExportFromS3`.


.. _swh-graph-luigi-CreateAthena:

CreateAthena
^^^^^^^^^^^^

Implemented by :class:`swh.dataset.luigi.CreateAthena`.

Depends on :ref:`swh-graph-luigi-UploadExportToS3` and creates Amazon Athena tables
for the ORC dataset.


.. _swh-graph-luigi-LocalExport:

LocalExport
^^^^^^^^^^^

Implemented by :class:`swh.graph.dataset.LocalExport`.

This is a pseudo-task used as a dependency by other tasks which need a graph,
but do not care whether it should be generated locally or downloading if missing.

It is configured through either ``--LocalExport-export-task-type DownloadExportFromS3``
(the default) or ``--LocalExport-export-task-type ExportGraph`` (to locally compress a new
graph from scratch).

.. _swh-graph-luigi-RunExportAll:

RunExportAll
^^^^^^^^^^^^

Implemented by :class:`swh.dataset.luigi.RunExportCompressUpload`.

This is a pseudo-task which depends on :ref:`swh-graph-luigi-ExportGraph`,
:ref:`swh-graph-luigi-CreateAthena`,
and :ref:`swh-graph-luigi-UploadExportToS3`.

.. _swh-graph-luigi-compressed-graph:

Compressed graph
----------------

There are three important tasks to deal with the compressed graph:

* :ref:`swh-graph-luigi-CompressGraph` does the compression itself (and depends on a graph export)
* :ref:`swh-graph-luigi-RunExportCompressUpload` is a pseudo-task which depends on
  :ref:`swh-graph-luigi-LocalExport` (so, indirectly :ref:`swh-graph-luigi-ExportGraph`),
  :ref:`swh-graph-luigi-CreateAthena`, :ref:`swh-graph-luigi-CompressGraph`,
  and :ref:`swh-graph-luigi-UploadGraphToS3`.
* :ref:`swh-graph-luigi-LocalGraph` which is a pseudo-task that any other task should
  depend on.
  It can be configured by users to either compress a new graph or download it from S3.

In details:

.. _swh-graph-luigi-CompressGraph:

CompressGraph
^^^^^^^^^^^^^

Implemented by :class:`swh.graph.luigi.compressed_graph.CompressGraph`.
It depends on all leaf tasks
of the compression pipeline, which don't need to be called correctly.

An example call is::

    swh graph luigi \
        --base-directory /poolswh/softwareheritage/vlorentz/ \
        --s3-prefix s3://softwareheritage/graph/ \
        --athena-prefix swh \
        --dataset-name 2022-12-07 \
        CompressGraph \
        -- \
        --scheduler-url http://localhost:50092/ \
        --RunExportAll-s3-athena-output-location s3://softwareheritage/tmp/athena/import_of_2022-12-07/ \
        --ExportGraph-config ~/luigid/graph.prod.yml \
        --ExportGraph-processes 96

Note the final parameters: they are passed to dependent tasks, not directly to
``CompressGraph``.

.. _swh-graph-luigi-UploadGraphToS3:

UploadGraphToS3
^^^^^^^^^^^^^^^^

Implemented by :class:`swh.graph.luigi.compressed_graph.UploadGraphToS3`.

.. _swh-graph-luigi-DownloadGraphFromS3:

DownloadGraphFromS3
^^^^^^^^^^^^^^^^^^^^

Implemented by :class:`swh.graph.luigi.compressed_graph.DownloadGraphFromS3`.

Example call::

    swh graph luigi \
        --base-directory /poolswh/softwareheritage/vlorentz/ \
        --dataset-name 2022-12-07 \
        --s3-prefix s3://softwareheritage/graph/ \
        -- \
        --scheduler-url http://localhost:50092/ \
        --log-level INFO
        DownloadGraphFromS3


.. _swh-graph-luigi-RunExportCompressUpload:

RunExportCompressUpload
^^^^^^^^^^^^^^^^^^^^^^^

Implemented by :class:`swh.graph.luigi.RunExportCompressUpload`.

This is a pseudo-task which depends on :ref:`swh-graph-luigi-ExportGraph`,
:ref:`swh-graph-luigi-CreateAthena`, :ref:`swh-graph-luigi-CompressGraph`,
and :ref:`swh-graph-luigi-UploadGraphToS3`.

An example call is::

    swh graph luigi \
        --base-directory /poolswh/softwareheritage/vlorentz/ \
        --s3-prefix s3://softwareheritage/graph/ \
        --athena-prefix swh \
        --dataset-name 2022-12-07 \
        RunExportCompressUpload \
        -- \
        --scheduler-url http://localhost:50092/ \
        --RunExportAll-s3-athena-output-location s3://softwareheritage/tmp/athena/import_of_2022-12-07/ \
        --ExportGraph-config ~/luigid/graph.prod.yml \
        --ExportGraph-processes 96 \

Or, for a partial subgraph (not the ``--export-name`` is unchanged, because it
uses the same export but produces a different compressed graph)::

    swh graph luigi \
        --base-directory /poolswh/softwareheritage/vlorentz/ \
        --s3-prefix s3://softwareheritage/graph/ \
        --athena-prefix swh \
        --dataset-name 2022-12-07-history-hosting \
        --export-name 2022-12-07 \
        RunExportCompressUpload \
        -- \
        --scheduler-url http://localhost:50092/ \
        --RunExportAll-s3-athena-output-location s3://softwareheritage/tmp/athena/import_of_2022-12-07-history-hosting/ \
        --ExportGraph-config ~/luigid/graph.prod.yml \
        --ExportGraph-processes 96 \
        --CompressGraph-object-types ori,snp,rel,rev


.. _swh-graph-luigi-LocalGraph:

LocalGraph
^^^^^^^^^^

Implemented by :class:`swh.graph.luigi.LocalGraph`.

This is a pseudo-task used as a dependency by other tasks which need a graph,
but do not care whether it should be generated locally or downloading if missing.

It is configured through either ``--LocalGraph-compression-task-type DownloadExportFromS3``
(the default) or ``--LocalGraph-compression-task-type CompressGraph`` (to locally compress a new
graph from scratch).


Blobs datasets
--------------

:mod:`swh.graph.luigi.blobs_datasets` contains tasks to extract a subset of blobs
from the archive, usually based on their names.
It is normally triggered through :ref:`swh-graph-luigi-RunBlobDataset`.
See the module's documentation for details on other tasks.

.. _swh-graph-luigi-RunBlobDataset:

RunBlobDataset
^^^^^^^^^^^^^^

Runs all tasks to select, download, and analyze a blob dataset.

Example call, to generate `the license dataset
<https://annex.softwareheritage.org/public/dataset/license-blobs/2022-12-07/>`_::

    swh graph luigi \
        --graph-base-directory /dev/shm/swh-graph/2022-12-07/ \
        --base-directory /poolswh/softwareheritage/vlorentz/ \
        --previous-dataset-name 2022-04-25 \
        --dataset-name 2022-12-07 \
        --s3-prefix s3://softwareheritage/derived_datasets/ \
        --athena-prefix swh \
        --s3-athena-output-location s3://softwareheritage/tmp/athena \
        --grpc-api localhost:50093 \
        -- \
        --scheduler-url http://localhost:50092/ \
        --log-level INFO \
        RunBlobDataset \
        --blob-filter license \
        --DownloadBlobs-download-url 'https://softwareheritage.s3.amazonaws.com/content/{sha1}' \
        --DownloadBlobs-decompression-algo gzip


In particular, note the optional ``--previous-dataset-name`` parameter, which
reuses a previous version of the blob dataset to speed-up tasks by running incrementally.


File names
----------

.. attention:

   This section is incomplete, see :mod:`swh.graph.luigi.file_names` as documentation


Provenance
----------

.. attention:

   This section is incomplete, see :mod:`swh.graph.luigi.file_names` as documentation


Origin contributors
-------------------

.. attention:

   This section is incomplete, see :mod:`swh.graph.luigi.origin_contributors` as documentation


.. _swh-graph-luigi-RunOriginContributors:

RunOriginContributors
^^^^^^^^^^^^^^^^^^^^^

Example call::

    swh graph luigi \
        --graph-base-directory /dev/shm/swh-graph/2022-12-07/ \
        --base-directory /poolswh/softwareheritage/vlorentz/ \
        --base-sensitive-directory /poolswh/softwareheritage/vlorentz/sensitive_datasets \
        --athena-prefix swh \
        --dataset-name 2022-12-07 \
        RunOriginContributors \
        -- \
        --scheduler-url http://localhost:50092/

Topology
--------

.. attention:

   This section is incomplete, see :mod:`swh.graph.luigi.topology` as documentation
