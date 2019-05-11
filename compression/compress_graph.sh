#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Expected two arguments: <input graph> <output dir>"
    exit -1
fi

INPUT_GRAPH=$1
OUTPUT_DIR=$2
DATASET=$(basename $INPUT_GRAPH)
COMPR_GRAPH="$OUTPUT_DIR/$DATASET"

java_cmd () {
    /usr/bin/time -v java                                                   \
        -Xmx1024G -server -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G \
        -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA        \
        -XX:+UseTLAB -XX:+ResizeTLAB                                        \
        -cp /app/'*' $*
}

llp_ordering () {
    # Create a symmetrized version of the graph
    # (output: .{graph,offsets,properties})
    java_cmd it.unimi.dsi.big.webgraph.Transform symmetrizeOffline \
        $COMPR_GRAPH-bv $COMPR_GRAPH-bv-sym
    java_cmd it.unimi.dsi.big.webgraph.BVGraph --list $COMPR_GRAPH-bv-sym

    # Find a better permutation through Layered LPA
    # WARNING: no 64-bit version of LLP
    java_cmd it.unimi.dsi.law.graph.LayeredLabelPropagation \
        --longs $COMPR_GRAPH-bv-sym $COMPR_GRAPH.order
}

bfs_ordering () {
    java_cmd it.unimi.dsi.law.graph.BFS $COMPR_GRAPH-bv $COMPR_GRAPH.order
}

mkdir -p $OUTPUT_DIR

# Build a function (MPH) that maps node names to node numbers in lexicographic
# order (output: .mph)
java_cmd it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction \
    --zipped $COMPR_GRAPH.mph $INPUT_GRAPH.nodes.csv.gz

# Build the graph in BVGraph format (output: .{graph,offsets,properties})
java_cmd it.unimi.dsi.big.webgraph.ScatteredArcsASCIIGraph  \
    --function $COMPR_GRAPH.mph                         \
    --zipped $COMPR_GRAPH-bv < $INPUT_GRAPH.edges.csv.gz
# Build the offset big-list file to load the graph faster (output: .obl)
java_cmd it.unimi.dsi.big.webgraph.BVGraph --list $COMPR_GRAPH-bv

# Find a better permutation
bfs_ordering

# Permute the graph accordingly
java_cmd it.unimi.dsi.big.webgraph.Transform mapOffline \
    $COMPR_GRAPH-bv $COMPR_GRAPH $COMPR_GRAPH.order
java_cmd it.unimi.dsi.big.webgraph.BVGraph --list $COMPR_GRAPH

# Compute graph statistics (output: .{indegree,outdegree,stats})
java_cmd it.unimi.dsi.big.webgraph.Stats $COMPR_GRAPH
