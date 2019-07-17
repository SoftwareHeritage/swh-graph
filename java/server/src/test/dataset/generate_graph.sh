#!/bin/bash

# Clean previous run
rm -rf dockerfiles output
mkdir output

# Build Docker work environment
toplevel_dir=`git rev-parse --show-toplevel`
mkdir -p dockerfiles
cp -r $toplevel_dir/dockerfiles/ .
docker build --tag swh-graph-test dockerfiles

# Setup input for compression script
tr ' ' '\n' < example.edges.csv | sort -u > example.nodes.csv
gzip --force --keep example.edges.csv
gzip --force --keep example.nodes.csv

docker run                                                      \
    --user $(id -u):$(id -g)                                    \
    --name swh-graph-test --rm --tty --interactive              \
    --volume $(pwd):/input                                      \
    --volume $(pwd)/output:/output                              \
    swh-graph-test:latest                                       \
    ./scripts/compress_graph.sh                                 \
    --input /input/example --output /output                     \
    --lib /swh/graph-lib --tmp /output/tmp                      \
    --stdout /output/stdout --stderr /output/stderr
