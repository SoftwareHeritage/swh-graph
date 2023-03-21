Graph service - Java backend
============================

Server side Java RPC API.

Build
-----

```console
$ mvn compile assembly:single
```

Start RPC API
-------------

```console
$ java -cp target/swh-graph-*.jar \
    org.softwareheritage.graph.rpc.GraphServer \
    <compressed_graph_path>
```

Default port is 50091 (use the `--port` option to change port number).

Tests
-----

Unit tests rely on test data that are already available in the Git repository
(under `swh/graph/example_dataset/`). You generally only need to run them
using Maven:

```console
$ mvn test
```

See the documentation of the `swh.graph.example_dataset` module if you
need to regenerate the example dataset files.
