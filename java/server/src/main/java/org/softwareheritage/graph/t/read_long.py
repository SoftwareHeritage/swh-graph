#!/usr/bin/python3

import struct
import sys

BUF_SIZE = 64*1024
BIN_FMT = '>q'  # 64 bit integer, big endian


def main(fname):
    with open(fname, 'rb') as f:
        data = f.read(BUF_SIZE)
        while(data):
            for data in struct.iter_unpack(BIN_FMT, data):
                print(data[0])
            data = f.read(BUF_SIZE)


if __name__ == '__main__':
    try:
        main(sys.argv[1])
    except IndexError:
        print('Usage: read_long FILENAME')
        sys.exit(1)
