# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
Script to run the end to end compression checks without having to
compress an entire graph.
"""

from swh.graph.e2e_check import run_e2e_check

run_e2e_check(
    graph_name="example",
    in_dir="swh/graph/example_dataset/orc/",
    out_dir="swh/graph/example_dataset/compressed/",
    sensitive_in_dir="swh/graph/example_dataset_sensitive/orc/",
    sensitive_out_dir="swh/graph/example_dataset_sensitive/compressed/",
    check_flavor="example",
    profile="debug",
)
