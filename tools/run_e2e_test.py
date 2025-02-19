# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Script to run the end to end compression tests without having to
compress an entire graph.
"""

from swh.graph.e2e_tests import run_e2e_test

run_e2e_test(
    graph_name="example",
    in_dir="swh/graph/example_dataset/orc/",
    out_dir="swh/graph/example_dataset/compressed/",
    test_flavor="example",
    target="debug",
)
