#!/bin/bash

die_usage() {
    echo "Usage: docker/run.sh GRAPH_DIR"
    exit 1
}

graph_dir="$1"
if [ -z "$graph_dir" -o ! -d "$graph_dir" ] ; then
    die_usage
fi

docker run -ti \
       --volume "$graph_dir":/srv/softwareheritage/graph/data \
       --publish 127.0.0.1:5009:5009 \
       swh-graph:latest \
       bash
