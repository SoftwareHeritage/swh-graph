import subprocess

from setuptools import build_meta as _orig
from setuptools.build_meta import *  # noqa


def build_sdist(sdist_directory, config_settings=None):
    print("Building the JAR file")
    subprocess.check_call(["make", "java"])
    return _orig.build_sdist(sdist_directory, config_settings)
