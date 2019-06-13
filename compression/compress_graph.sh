#!/bin/bash

usage() {
    echo "Usage: --input <graph path> --output <out dir> --lib <graph lib path>"
    echo "  options:"
    echo "    -t, --tmp <temporary dir> (default to /tmp/)"
    exit 1
}

graph_path=""
out_dir=""
lib_path=""
tmp_dir="/tmp/"
while (( "$#" )); do
    case "$1" in
        -i|--input) shift; graph_path=$1;;
        -o|--output) shift; out_dir=$1;;
        -l|--lib) shift; lib_path=$1;;
        -t|--tmp) shift; tmp_dir=$1;;
        *) usage;;
    esac
    shift
done

if [[ -z $graph_path || -z $out_dir || -z $lib_path ]]; then
    usage
fi

dataset=$(basename $graph_path)
compr_graph_path="$out_dir/$dataset"

mkdir -p $out_dir
mkdir -p $tmp_dir

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
        $compr_graph_path-bv $compr_graph_path-bv-sym
    java_cmd it.unimi.dsi.big.webgraph.BVGraph --list $compr_graph_path-bv-sym

    # Find a better permutation through Layered LPA
    # WARNING: no 64-bit version of LLP
    java_cmd it.unimi.dsi.law.graph.LayeredLabelPropagation \
        --longs $compr_graph_path-bv-sym $compr_graph_path.order
}

bfs_ordering () {
    java_cmd it.unimi.dsi.law.graph.BFSBig \
        $compr_graph_path-bv $compr_graph_path.order
}

# Build a function (MPH) that maps node names to node numbers in lexicographic
# order (output: .mph)
java_cmd it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction   \
    --zipped $compr_graph_path.mph --temp-dir $tmp_dir          \
    $graph_path.nodes.csv.gz

# Build the graph in BVGraph format (output: .{graph,offsets,properties})
java_cmd it.unimi.dsi.big.webgraph.ScatteredArcsASCIIGraph      \
    --function $compr_graph_path.mph --temp-dir $tmp_dir        \
    --zipped $compr_graph_path-bv < $graph_path.edges.csv.gz
# Build the offset big-list file to load the graph faster (output: .obl)
java_cmd it.unimi.dsi.big.webgraph.BVGraph --list $compr_graph_path-bv

# Find a better permutation
bfs_ordering

# Permute the graph accordingly
batch_size=1000000000
java_cmd it.unimi.dsi.big.webgraph.Transform mapOffline \
    $compr_graph_path-bv $compr_graph_path $compr_graph_path.order $batch_size
java_cmd it.unimi.dsi.big.webgraph.BVGraph --list $compr_graph_path

# Compute graph statistics (output: .{indegree,outdegree,stats})
java_cmd it.unimi.dsi.big.webgraph.Stats $compr_graph_path

# Create transposed graph (to allow backward traversal)
java_cmd it.unimi.dsi.big.webgraph.Transform transposeOffline \
    $compr_graph_path $compr_graph_path-transposed $batch_size
java_cmd it.unimi.dsi.big.webgraph.BVGraph --list $compr_graph_path-transposed
