#!/bin/bash

if [ -z "$1" -o -z "$2" -o ! -f "$1" ] ; then
    echo "Usage: [SORT_FLAGS=...] pid2int2int2pid.sh IN_PID2INT.bin OUT_INT2PID.bin"
    exit 1
fi

swh graph map dump -t pid2int "$1" \
    | sort -n -k 2 $SORT_FLAGS \
    | cut -f 1 \
    | swh graph map write -t int2pid "$2"
