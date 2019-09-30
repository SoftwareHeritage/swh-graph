#!/usr/bin/env python3

# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert a textual int->PID map (as a simple list of textual PIDs), to the
binary format of :class:`swh.graph.pid.IntToPidMap`.

"""

import sys

from swh.graph.pid import IntToPidMap


def main(fname):
    with open(fname, 'wb') as f:
        for line in sys.stdin:
            pid = line.rstrip()
            IntToPidMap.write_record(f, pid)


if __name__ == '__main__':
    main(sys.argv[1])
