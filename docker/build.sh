#!/bin/bash

DOCKER_DIR="./docker"

if ! [ -d "$DOCKER_DIR" ] ; then
    echo "can't find $DOCKER_DIR dir"
    echo "are you running run/build.sh from swh-graph root directory?"
    exit 1
fi

docker build --tag swh-graph docker/
