.. _swh-graph-testing:

Test graphs
===========

In order to support writing unit-tests, swh-graph provides three mechanisms,
detailed in the sections below

Example dataset
---------------

Tests written in Python can use :const:`swh.graph.example_dataset.DATASET_DIR` to
get the path to the example dataset available locally.

They may also use fixtures from :mod:`swh.graph.pytest_plugin` such as
:func:`graph_grpc_server_started <swh.graph.pytest_plugin.graph_grpc_server_started>`
to get the port of a temporary gRPC server process serving that graph,
:func:`graph_grpc_stub <swh.graph.pytest_plugin.graph_grpc_stub>` to connect to that
server as a gRPC client, or :func:`graph_client <swh.graph.pytest_plugin.graph_client>`
to get an HTTP client instance.

The example dataset/graph is static, and meant only to test common cases because any
change to it breaks tests in swh-graph and dependent packages.
To test more edge cases, you need to use one of the two methods below to generate
your own graphs.

In-memory VecGraph
------------------

Tests written in Rust can use the
`GraphBuilder <https://docs.rs/swh-graph/latest/swh_graph/graph_builder/struct.GraphBuilder.html>`_
to define a graph with a high-level interface. The ``GraphBuilder`` returns a graph
that implements the same traits as the real one.

JSON-serialized graph
---------------------

An in-memory ``VecGraph`` can be serialized to JSON (or any format `serde <https://serde.rs/>`_
supports) using ``swh_graph::serde::serialize_with_labels_and_maps``. For example, in JSON:

.. code-block:: rust

    let file = std::fs::File::create(output_path)
        .with_context(|| format!("Could not create {}", output_path.display()))?;

    let mut serializer = serde_json::Serializer::new(BufWriter::new(file));

    swh_graph::serde::serialize_with_labels_and_maps(&mut serializer, &graph)
        .with_context(|| format!("Could not serialize to {}", output_path.display()))?;


These serialized graphs can then be loaded as a ``VecGraph`` again, using
``swh_graph::serde::deserialize_with_labels_and_maps``. For example, in JSON:

.. code-block:: rust

    let file = std::fs::File::open(&graph_path).with_context(|| {
        format!("Could not open {}", graph_path.display())
    })?;

    let mut deserializer =
        serde_json::Deserializer::from_reader(BufReader::new(file));

    let graph = swh_graph::serde::deserialize_with_labels_and_maps(&mut deserializer)
        .map_err(|e| anyhow!("Could not read JSON graph: {e}"))?;

In particular, the gRPC server executable (``swh-graph-grpc-serve``) can read files
serialized to JSON this way, using the ``--graph-format json`` option.

The JSON serialization, especially of properties, is not meant to be easily human-editable.
Therefore, it is recommended to keep the Rust code used to generate it checked-in in your
source code, and generate the JSON files at the beginning of your test suite (eg. as
a session-scoped pytest fixture, for tests written in Python).
