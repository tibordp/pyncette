========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |github-ci|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/pyncette/badge/?style=flat
    :target: https://readthedocs.org/projects/pyncette
    :alt: Documentation Status

.. |codecov| image:: https://codecov.io/gh/tibordp/pyncette/branch/master/graphs/badge.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/tibordp/pyncette

.. |github-ci| image:: https://github.com/tibordp/pyncette/workflows/Python%20package/badge.svg?branch=master
    :alt: Github Actions Build Status
    :target: https://github.com/tibordp/pyncette/actions?query=branch%3Amaster+workflow%3A%22Python+package%22

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

.. |commits-since| image:: https://img.shields.io/github/commits-since/tibordp/pyncette/v0.10.1.svg
    :alt: Commits since latest release
    :target: https://github.com/tibordp/pyncette/compare/v0.10.1...master



.. end-badges

A reliable distributed scheduler with pluggable storage backends for Async Python.

* Free software: MIT license

Installation
============

Minimal installation (just SQLite persistence):

::

    pip install pyncette

Full installation (all the backends and Prometheus metrics exporter):

::

    pip install pyncette[all]

You can also install the in-development version with::

    pip install https://github.com/tibordp/pyncette/archive/master.zip


Documentation
=============


https://pyncette.readthedocs.io


Usage example
=============

Simple in-memory scheduler (does not persist state)

.. code:: python

    from pyncette import Pyncette, Context

    app = Pyncette()

    @app.task(schedule='* * * * *')
    async def foo(context: Context):
        print('This will run every minute')

    if __name__ == '__main__':
        app.main()

Persistent distributed cron using Redis (coordinates execution with parallel instances and survives restarts)

.. code:: python

    from pyncette import Pyncette, Context
    from pyncette.redis import redis_repository

    app = Pyncette(repository_factory=redis_repository, redis_url='redis://localhost')

    @app.task(schedule='* * * * * */10')
    async def foo(context: Context):
        print('This will run every 10 seconds')

    if __name__ == '__main__':
        app.main()


See the `examples` directory for more examples of usage.

Use cases
=========

Pyncette is designed for reliable (at-least-once or at-most-once) execution of recurring tasks (think cronjobs) whose
lifecycles are managed dynamically, but can work effectively for non-reccuring tasks too.

Example use cases:

- You want to perform a database backup every day at noon
- You want a report to be generated daily for your 10M users at the time of their choosing
- You want currency conversion rates to be refreshed every 10 seconds
- You want to allow your users to schedule non-recurring emails to be sent at an arbitrary time in the future

Pyncette might not be a good fit if:

- You want your tasks to be scheduled to run (ideally) once as soon as possible. It is doable, but you will be better served by a general purpose reliable queue like RabbitMQ or Amazon SQS.
- You need tasks to execute at sub-second intervals with low jitter. Pyncette coordinates execution on a per task-instance basis and this corrdination can add overhead and jitter.


Supported backends
==================

Pyncette comes with an implementation for the following backends (used for persistence and coordination) out-of-the-box:

- SQLite (included)
- Redis (``pip install pyncette[redis]``)
- PostgreSQL (``pip install pyncette[postgres]``)
- MySQL 8.0+ (``pip install pyncette[mysql]``)
- Amazon DynamoDB (``pip install pyncette[dynamodb]``)

Pyncette imposes few requirements on the underlying datastores, so it can be extended to support other databases or
custom storage formats / integrations with existing systems. For best results, the backend needs to provide:

- Some sort of serialization mechanism, e.g. traditional transactions, atomic stored procedures or compare-and-swap
- Efficient range queries over a secondary index, which can be eventually consistent


Development
===========

To run integration tests you will need Redis, PostgreSQL, MySQL and Localstack (for DynamoDB) running locally.

To run the all tests run::

    tox

Alternatively, there is a Docker Compose environment that will set up all the backends so that integration tests can run seamlessly::

    docker-compose up -d
    docker-compose run --rm shell
    tox

To run just the unit tests (excluding integration tests)::

    tox -e py310  # or your Python version of choice

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
