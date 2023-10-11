# Copyright (C) 2017-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from glob import glob

from setuptools import setup

JAR_PATHS = list(glob("java/target/swh-graph-*.jar"))

# TODO: the use of data_files is now deprecated and there is no real replacement
# for it (by design); we should think of a better way of distributing this jar.
setup(
    data_files=[("share/swh-graph", JAR_PATHS)],
)
