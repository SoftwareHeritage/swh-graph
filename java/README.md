Graph service - Java backend
============================

Server side Java RPC API.

Build
-----

```bash
$ mvn compile assembly:single
```

Start RPC API
-------------

```bash
$ java -cp target/swh-graph-*.jar \
    org.softwareheritage.graph.rpc.GraphServer \
    <compressed_graph_path>
```

Default port is 50091 (use the `--port` option to change port number).

Tests
-----

Unit tests rely on test data that are already available in the Git repository
(under `src/swh/graph/tests/dataset/`). You generally only need to run them
using Maven:

```bash
$ mvn test
```

In case you want to regenerate the test data:

```bash
# Graph compression
$ cd src/swh/graph/tests/dataset
$ ./generate_graph.sh
$ cd ../../../..

$ mvn compile assembly:single
# Dump mapping files
$ java -cp target/swh-graph-*.jar \
    org.softwareheritage.graph.compress.NodeMapBuilder \
    src/swh/graph/tests/dataset/example.nodes.csv.gz \
    src/swh/graph/tests/dataset/output/example
```
