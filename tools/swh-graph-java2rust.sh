#!/bin/bash
set -e

if [ -z "$1" -o -z "$2" ] ; then
    echo "Usage: $0 SWH_GRAPH_SRC_DIR COMPRESSED_GRAPH_DIR"
    echo "Example: swh-graph-java2rust.sh ~/src/swh-graph ~/graph/lastest/compressed"
    exit 2
fi

SOURCE_DIR="$1"
GRAPH_DIR="$2"

# Build the Java implementation
make -C $SOURCE_DIR java

# Convert the GOV minimal-perfect-hash function from `.mph` to `.cmph`
java -classpath ${SOURCE_DIR}/swh/graph/swh-graph.jar \
     org.softwareheritage.graph.utils.Mph2Cmph \
     ${GRAPH_DIR}/graph.mph \
     ${GRAPH_DIR}/graph.cmph

# Move to (Rust) source dir, for "cargo run"
cd $SOURCE_DIR

# Generate Elias-Fano-encoded offsets (`.ef` files) of the graph
cargo run --release --bin swh-graph-index ef -- ${GRAPH_DIR}/graph
cargo run --release --bin swh-graph-index ef -- ${GRAPH_DIR}/graph-transposed

# Ditto, this time for the labelled graph
cargo run --release --bin swh-graph-index labels-ef -- \
      ${GRAPH_DIR}/graph-labelled $((1+ $(cat ${GRAPH_DIR}/graph.nodes.count.txt)))
cargo run --release --bin swh-graph-index labels-ef -- \
      ${GRAPH_DIR}/graph-transposed-labelled $((1+ $(cat $GRAPH_DIR/graph.nodes.count.txt)))

# Generate `node2type.bin` from `node2type.map` (the format of the latter is Java-specific)
cargo run --release --bin swh-graph-node2type -- ${GRAPH_DIR}/graph

# Convert the Java-specific `.property.content.is_skipped.bin` to a plain `.property.content.is_skipped.bits`:
java -classpath ${SOURCE_DIR}/java/target/swh-graph-*.jar \
     ${SOURCE_DIR}/java/src/main/java/org/softwareheritage/graph/utils/Bitvec2Bits.java \
     ${GRAPH_DIR}/graph.property.content.is_skipped.bin \
     ${GRAPH_DIR}/graph.property.content.is_skipped.bits
