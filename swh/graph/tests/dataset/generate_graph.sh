#!/bin/bash

# Clean previous run
rm -rf docker/ output
mkdir output

# Build Docker work environment
toplevel_dir=`git rev-parse --show-toplevel`
mkdir -p docker
cp -r $toplevel_dir/docker/ .
docker build --tag swh-graph-test docker

# Setup input for compression script
tr ' ' '\n' < example.edges.csv | sort -u > example.nodes.csv
zstd < example.nodes.csv > example.nodes.csv.zst
zstd < example.edges.csv > example.edges.csv.zst

docker run \
    --user $(id -u):$(id -g) \
    --name swh-graph-test --rm --tty --interactive  \
    --volume $(pwd):/input --volume $(pwd)/output:/output \
    swh-graph-test:latest \
    swh graph compress --graph /input/example --outdir /output
