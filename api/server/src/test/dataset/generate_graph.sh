#!/bin/bash

# Build Docker work environment
toplevel_dir=`git rev-parse --show-toplevel`
mkdir -p dockerfiles
cp -r $toplevel_dir/dockerfiles/ .
docker build --tag swh-graph-test dockerfiles

# Setup input for compression script
tr ' ' '\n' < graph.edges.csv | sort -u > graph.nodes.csv
gzip --force --keep graph.edges.csv
gzip --force --keep graph.nodes.csv

# Setup output
rm -f stderr stdout

docker run                                                  \
    --name swh-graph-test --rm --tty --interactive          \
    --volume $(pwd):/data swh-graph-test:latest             \
    ./scripts/compress_graph.sh                             \
    --input /data/graph --output /data/ --lib /graph-lib/
