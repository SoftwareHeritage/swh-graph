#!/bin/bash

usage() {
    echo "Usage: --input <graph path> --output <out dir> --lib <graph lib dir>"
    echo "  options:"
    echo "    -t, --tmp <temporary dir> (default to /tmp/)"
    echo "    --stdout  <stdout file> (default to ./stdout)"
    echo "    --stderr  <stderr file> (default to ./stderr)"
    echo "    --batch-size <batch size> (default to 10^6): WebGraph internals"
    exit 1
}

graph_path=""
out_dir=""
lib_dir=""
tmp_dir="/tmp/"
stdout_file="stdout"
stderr_file="stderr"
batch_size=1000000
while (( "$#" )); do
    case "$1" in
        -i|--input) shift; graph_path=$1;;
        -o|--output) shift; out_dir=$1;;
        -l|--lib) shift; lib_dir=$1;;
        -t|--tmp) shift; tmp_dir=$1;;
        --stdout) shift; stdout_file=$1;;
        --stderr) shift; stderr_file=$1;;
        --batch-size) shift; batch_size=$1;;
        *) usage;;
    esac
    shift
done

if [[ -z $graph_path || -z $out_dir || -z $lib_dir ]]; then
    usage
fi

if [[ -f "$stdout_file" || -f "$stderr_file" ]]; then
    echo "Cannot overwrite previous compression stdout/stderr files"
    exit 1
fi

dataset=$(basename $graph_path)
compr_graph_path="$out_dir/$dataset"

mkdir -p $out_dir
mkdir -p $tmp_dir

java_cmd () {
    /usr/bin/time -v java -cp $lib_dir/'*' $*
}

{
    # Build a function (MPH) that maps node names to node numbers in
    # lexicographic order (output: .mph)
    java_cmd it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction       \
        --zipped $compr_graph_path.mph --temp-dir $tmp_dir              \
        $graph_path.nodes.csv.gz                                        &&

    # Build the graph in BVGraph format (output: .{graph,offsets,properties})
    java_cmd it.unimi.dsi.big.webgraph.ScatteredArcsASCIIGraph          \
        --function $compr_graph_path.mph --temp-dir $tmp_dir            \
        --zipped $compr_graph_path-bv < $graph_path.edges.csv.gz        &&
    # Build the offset big-list file to load the graph faster (output: .obl)
    java_cmd it.unimi.dsi.big.webgraph.BVGraph                          \
        --list $compr_graph_path-bv                                     &&

    # Find a better permutation using a BFS traversal order (output: .order)
    java_cmd it.unimi.dsi.law.graph.BFSBig                              \
        $compr_graph_path-bv $compr_graph_path.order                    &&

    # Permute the graph accordingly
    java_cmd it.unimi.dsi.big.webgraph.Transform mapOffline             \
        $compr_graph_path-bv $compr_graph_path                          \
        $compr_graph_path.order $batch_size $tmp_dir                    &&
    java_cmd it.unimi.dsi.big.webgraph.BVGraph                          \
        --list $compr_graph_path                                        &&

    # Compute graph statistics (output: .{indegree,outdegree,stats})
    java_cmd it.unimi.dsi.big.webgraph.Stats $compr_graph_path          &&

    # Create transposed graph (to allow backward traversal)
    java_cmd it.unimi.dsi.big.webgraph.Transform transposeOffline       \
        $compr_graph_path $compr_graph_path-transposed                  \
        $batch_size $tmp_dir                                            &&
    java_cmd it.unimi.dsi.big.webgraph.BVGraph                          \
        --list $compr_graph_path-transposed
} >> $stdout_file 2>> $stderr_file

if [[ $? -eq 0 ]]; then
    echo "Graph compression done."
else
    echo "Graph compression failed: see $stderr_file for more info."
    exit 1
fi
