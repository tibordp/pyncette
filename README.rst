========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis|
        |
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/pyncette/badge/?style=flat
    :target: https://readthedocs.org/projects/pyncette
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.org/tibordp/pyncette.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/tibordp/pyncette

.. |version| image:: https://img.shields.io/pypi/v/pyncette.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/pyncette

.. |wheel| image:: https://img.shields.io/pypi/wheel/pyncette.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/pyncette

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/pyncette.svg
    :alt: Supported versions
    :target: https://pypi.org/project/pyncette

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/pyncette.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/pyncette

.. |commits-since| image:: https://img.shields.io/github/commits-since/tibordp/pyncette/v0.0.8.svg
    :alt: Commits since latest release
    :target: https://github.com/tibordp/pyncette/compare/v0.0.8...master



.. end-badges

A reliable distributed cron with pluggable storage backends

* Free software: MIT license

Installation
============

::

    pip install pyncette
    # Or, for Redis support: pip install pyncette[redis] 

You can also install the in-development version with::

    pip install https://github.com/tibordp/pyncette/archive/master.zip

Documentation
=============


https://pyncette.readthedocs.io


Usage example
=============

Simple in-memory cron (does not persist state)

.. code:: python

    from pyncette import Pyncette, Context

    app = Pyncette()

    @app.task(schedule='* * * * *')
    async def foo(context: Context):
        print('This will run every minute')

    if __name__ == '__main__':
        app.main()
        # alternatively asyncio.run(app.run())

Persistent distributed cron using Redis (coordinates execution with parallel instances and survives restarts)

.. code:: python

    from pyncette import Pyncette, Context
    from pyncette.repository.redis import redis_repository

    app = Pyncette(repository_factory=redis_repository, redis_url='redis://localhost')

    @app.task(schedule='* * * * * */10')
    async def foo(context: Context):
        print('This will run every 10 seconds')

    if __name__ == '__main__':
        app.main()


Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
