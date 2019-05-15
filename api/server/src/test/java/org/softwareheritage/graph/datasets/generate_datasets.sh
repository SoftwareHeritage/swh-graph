#!/bin/bash

# Build Docker work environment
toplevel_dir=`git rev-parse --show-toplevel`
mkdir -p dockerfiles
cp $toplevel_dir/compression/{compress_graph.sh,Dockerfile} dockerfiles/
docker build --tag swh-graph-test dockerfiles

# Compress each existing dataset
for dataset in  dir_to_dir dir_to_file dir_to_rev origin_to_snapshot \
                release_to_obj rev_to_dir rev_to_rev snapshot_to_obj; do
    if [ -f "$dataset.edges.csv" ] ; then
        # Setup input for compression script
        tr ' ' '\n' < $dataset.edges.csv | sort -u > $dataset.nodes.csv
        gzip --force --keep $dataset.edges.csv
        gzip --force --keep $dataset.nodes.csv

        echo "Compressing $dataset..."
        mkdir -p $dataset
        docker run                                          \
            --name swh-graph-test --rm --tty --interactive  \
            --volume $(pwd):/data swh-graph-test:latest     \
            ./compress_graph.sh /data/$dataset /data/$dataset > /dev/null
    fi
done
