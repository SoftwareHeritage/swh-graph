#!/usr/bin/env python3

# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Convert a textual PID->int map (as a list of space-separated <PID, int>
textual pairs), to the binary format of :class:`swh.graph.pid.PidToIntMap`.

"""

import sys

from swh.graph.pid import PidToIntMap


def main(fname):
    with open(fname, 'wb') as f:
        for line in sys.stdin:
            (pid, int_str) = line.rstrip().split(maxsplit=1)
            PidToIntMap.write_record(f, pid, int(int_str))


if __name__ == '__main__':
    main(sys.argv[1])
