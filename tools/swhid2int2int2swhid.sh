#!/bin/bash

if [ -z "$1" -o -z "$2" -o ! -f "$1" ] ; then
    echo "Usage: [SORT_FLAGS=...] swhid2int2int2swhid.sh IN_SWHID2INT.bin OUT_INT2SWHID.bin"
    exit 1
fi

swh graph map dump -t swhid2int "$1" \
    | sort -n -k 2 $SORT_FLAGS \
    | cut -f 1 \
    | swh graph map write -t int2swhid "$2"
