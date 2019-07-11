Graph service - Server side
===========================

Server side Java REST API.

Build
-----

```bash
$ mvn compile assembly:single
```

Start REST API
--------------

```bash
$ java -cp target/swh-graph-1.0-jar-with-dependencies.jar   \
    org.softwareheritage.graph.App                          \
    <compressed_graph_path>
```

Default port is 5009.

Tests
-----

Unit tests rely on test data that are already available in the Git repository
(under `src/test/dataset/`). You generally only need to run them using Maven:

```bash
$ mvn test
```

In case you want to regenerate the test data:

```bash
# Graph compression
$ cd src/test/dataset
$ ./generate_graph.sh
$ cd ../../../

$ mvn compile assembly:single
# Dump mapping files
$ java -cp target/swh-graph-1.0-jar-with-dependencies.jar   \
    org.softwareheritage.graph.backend.Setup                \
    src/test/dataset/example.nodes.csv.gz                   \
    src/test/dataset/output/example
```
