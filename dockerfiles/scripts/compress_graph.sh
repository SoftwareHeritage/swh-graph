#!/bin/bash

usage() {
    echo "Usage: compress_graph.sh --lib <LIB_DIR> --input <GRAPH_BASEPATH>"
    echo "Options:"
    echo "  -o, --outdir <OUT_DIR>    (Default: GRAPH_DIR/compressed)"
    echo "  -t, --tmp <TMP_DIR>       (Default: OUT_DIR/tmp)"
    echo "  --stdout  <STDOUT_LOG>    (Default: OUT_DIR/stdout)"
    echo "  --stderr  <STDERR_LOG>    (Default: OUT_DIR/stderr)"
    echo "  --batch-size <BATCH_SIZE> (Default: 10^6): WebGraph internals"
    exit 1
}

graph_path=""
out_dir=""
lib_dir=""
stdout_file=""
stderr_file=""
batch_size=1000000
while (( "$#" )); do
    case "$1" in
        -i|--input)   shift; graph_path=$1 ;;
        -o|--outdir)  shift; out_dir=$1 ;;
        -l|--lib)     shift; lib_dir=$1 ;;
        -t|--tmp)     shift; tmp_dir=$1 ;;
        --stdout)     shift; stdout_file=$1 ;;
        --stderr)     shift; stderr_file=$1 ;;
        --batch-size) shift; batch_size=$1 ;;
        *) usage ;;
    esac
    shift
done

if [[ -z "$graph_path" || ! -d "$lib_dir" ]]; then
    usage
fi
if [ -z "$out_dir" ] ; then
    out_dir="$(dirname $graph_path)/compressed"
fi
if [ -z "$tmp_dir" ] ; then
    tmp_dir="${out_dir}/tmp"
fi
if [ -z "$stdout_file" ] ; then
    stdout_file="${out_dir}/stdout"
fi
if [ -z "$stderr_file" ] ; then
    stderr_file="${out_dir}/stderr"
fi

dataset=$(basename $graph_path)
compr_graph_path="${out_dir}/${dataset}"

test -d "$out_dir" || mkdir -p "$out_dir"
test -d "$tmp_dir" || mkdir -p "$tmp_dir"

step_info() {
    echo -e "\n* swh-graph: $1 step... ($2)\n"
}

java_cmd () {
    /usr/bin/time -v java -cp $lib_dir/'*' $*
}

{
    # Build a function (MPH) that maps node names to node numbers in
    # lexicographic order (output: .mph)
    step_info "MPH" "1/6"                                               &&
    java_cmd it.unimi.dsi.sux4j.mph.GOVMinimalPerfectHashFunction       \
        --zipped $compr_graph_path.mph --temp-dir $tmp_dir              \
        $graph_path.nodes.csv.gz                                        &&

    # Build the graph in BVGraph format (output: .{graph,offsets,properties})
    step_info "BV compress" "2/6"                                       &&
    java_cmd it.unimi.dsi.big.webgraph.ScatteredArcsASCIIGraph          \
        --function $compr_graph_path.mph --temp-dir $tmp_dir            \
        --zipped $compr_graph_path-bv < $graph_path.edges.csv.gz        &&
    # Build the offset big-list file to load the graph faster (output: .obl)
    java_cmd it.unimi.dsi.big.webgraph.BVGraph                          \
        --list $compr_graph_path-bv                                     &&

    # Find a better permutation using a BFS traversal order (output: .order)
    step_info "BFS" "3/6"                                               &&
    java_cmd it.unimi.dsi.law.big.graph.BFS                             \
        $compr_graph_path-bv $compr_graph_path.order                    &&

    # Permute the graph accordingly
    step_info "Permute" "4/6"                                           &&
    java_cmd it.unimi.dsi.big.webgraph.Transform mapOffline             \
        $compr_graph_path-bv $compr_graph_path                          \
        $compr_graph_path.order $batch_size $tmp_dir                    &&
    java_cmd it.unimi.dsi.big.webgraph.BVGraph                          \
        --list $compr_graph_path                                        &&

    # Compute graph statistics (output: .{indegree,outdegree,stats})
    step_info "Stats" "5/6"                                             &&
    java_cmd it.unimi.dsi.big.webgraph.Stats $compr_graph_path          &&

    # Create transposed graph (to allow backward traversal)
    step_info "Transpose" "6/6"                                         &&
    java_cmd it.unimi.dsi.big.webgraph.Transform transposeOffline       \
        $compr_graph_path $compr_graph_path-transposed                  \
        $batch_size $tmp_dir                                            &&
    java_cmd it.unimi.dsi.big.webgraph.BVGraph                          \
        --list $compr_graph_path-transposed
} > $stdout_file 2> $stderr_file

if [[ $? -eq 0 ]]; then
    echo "Graph compression done."
else
    echo "Graph compression failed: see $stderr_file for more info."
    exit 1
fi
