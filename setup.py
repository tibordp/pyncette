#!/usr/bin/env python

import itertools
import re
from pathlib import Path

from setuptools import find_packages
from setuptools import setup


def read(*names, **kwargs):
    with Path(__file__).parent.joinpath(*names).open(encoding=kwargs.get("encoding", "utf8")) as fh:
        return fh.read()


extras = {
    "redis": ["redis>=4.5.4"],
    "prometheus": ["prometheus_client>=0.16.0"],
    "postgres": ["asyncpg>=0.27.0"],
    "dynamodb": ["aioboto3>=11.1.0"],
    "mysql": ["aiomysql>=0.1.1", "cryptography>=40.0.2"],
    "uvloop": ["uvloop>=0.16.0"],
}

setup(
    name="pyncette",
    version="0.10.1",
    license="MIT",
    description="A reliable distributed scheduler with pluggable storage backends",
    long_description="{}\n{}".format(
        re.compile("^.. start-badges.*^.. end-badges", re.M | re.S).sub("", read("README.rst")),
        re.sub(":[a-z]+:`~?(.*?)`", r"``\1``", read("CHANGELOG.rst")),
    ),
    author="Tibor Djurica Potpara",
    author_email="tibor.djurica@ojdip.net",
    url="https://github.com/tibordp/pyncette",
    package_data={"pyncette": ["py.typed", "**/*.lua"]},
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[path.stem for path in Path("src").glob("*.py")],
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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
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
    python_requires=">=3.8",
    install_requires=[
        "croniter>=1.3.14",
        "aiosqlite>=0.19.0",
        "aiohttp>=3.8.4",
        "python-dateutil>=2.8.2",
        "coloredlogs",
    ],
    extras_require={
        **extras,
        # A convenience all extras
        "all": list(itertools.chain(*extras.values())),
    },
)
