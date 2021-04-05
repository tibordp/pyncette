#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import io
import itertools
import re
from glob import glob
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import find_packages
from setuptools import setup


def read(*names, **kwargs):
    with io.open(
        join(dirname(__file__), *names), encoding=kwargs.get("encoding", "utf8")
    ) as fh:
        return fh.read()


extras = {
    "redis": ["aioredis>=1.3.1"],
    "prometheus": ["prometheus_client>=0.8.0"],
    "postgres": ["asyncpg>=0.20.1"],
    "dynamodb": ["aioboto3>=8.3.0"],
    "mysql": ["aiomysql>=0.0.21", "cryptography>=3.4.7"],
    "uvloop": ["uvloop>=0.15.2"],
}

setup(
    name="pyncette",
    version="0.8.0",
    license="MIT",
    description="A reliable distributed scheduler with pluggable storage backends",
    long_description="%s\n%s"
    % (
        re.compile("^.. start-badges.*^.. end-badges", re.M | re.S).sub(
            "", read("README.rst")
        ),
        re.sub(":[a-z]+:`~?(.*?)`", r"``\1``", read("CHANGELOG.rst")),
    ),
    author="Tibor Djurica Potpara",
    author_email="tibor.djurica@ojdip.net",
    url="https://github.com/tibordp/pyncette",
    package_data={"pyncette": ["py.typed"]},
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Operating System :: POSIX",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Utilities",
    ],
    project_urls={
        "Documentation": "https://pyncette.readthedocs.io/",
        "Changelog": "https://pyncette.readthedocs.io/en/latest/changelog.html",
        "Issue Tracker": "https://github.com/tibordp/pyncette/issues",
    },
    keywords=[
        # eg: 'keyword1', 'keyword2', 'keyword3',
    ],
    python_requires=">=3.7",
    install_requires=[
        "typing-extensions>=3.7.4.2",
        "croniter>=0.3.34",
        "aiosqlite>=0.13.0",
        "aiohttp>=3.6.2",
        "python-dateutil",
        "coloredlogs",
    ],
    extras_require={
        **extras,
        # A convenience all extras
        "all": list(itertools.chain(*extras.values())),
    },
)
