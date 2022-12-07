#!/usr/bin/env python3
# Copyright (C) 2015-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from glob import glob
from io import open
from os import path

from setuptools import find_packages, setup

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()


def parse_requirements(*names):
    requirements = []
    for name in names:
        if name:
            reqf = "requirements-%s.txt" % name
        else:
            reqf = "requirements.txt"

        if not path.exists(reqf):
            return requirements

        with open(reqf) as f:
            for line in f.readlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                requirements.append(line)
    return requirements


JAR_PATHS = list(glob("java/target/swh-graph-*.jar"))

setup(
    name="swh.graph",
    description="Software Heritage graph service",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    python_requires=">=3.7",
    author="Software Heritage developers",
    author_email="swh-devel@inria.fr",
    url="https://forge.softwareheritage.org/diffusion/DGRPH",
    packages=find_packages(),
    install_requires=parse_requirements(None, "swh"),
    tests_require=parse_requirements("test"),
    setup_requires=["setuptools-scm"],
    use_scm_version=True,
    extras_require={
        "testing": parse_requirements("luigi", "test"),
        "luigi": parse_requirements("luigi"),
    },
    include_package_data=True,
    data_files=[("share/swh-graph", JAR_PATHS)],
    entry_points="""
        [console_scripts]
        swh-graph=swh.graph.cli:main
        [swh.cli.subcommands]
        graph=swh.graph.cli
    """,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
    ],
    project_urls={
        "Bug Reports": "https://forge.softwareheritage.org/maniphest",
        "Funding": "https://www.softwareheritage.org/donate",
        "Source": "https://forge.softwareheritage.org/source/swh-graph",
        "Documentation": "https://docs.softwareheritage.org/devel/swh-graph/",
    },
)
