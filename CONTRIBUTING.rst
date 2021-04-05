============
Contributing
============

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

Bug reports
===========

When `reporting a bug <https://github.com/tibordp/pyncette/issues>`_ please include:

    * Your operating system name and version.
    * Any details about your local setup that might be helpful in troubleshooting.
    * Detailed steps to reproduce the bug.

Documentation improvements
==========================

Pyncette could always use more documentation, whether as part of the
official Pyncette docs, in docstrings, or even on the web in blog posts,
articles, and such.

Feature requests and feedback
=============================

The best way to send feedback is to file an issue at https://github.com/tibordp/pyncette/issues.

If you are proposing a feature:

* Explain in detail how it would work.
* Keep the scope as narrow as possible, to make it easier to implement.
* Remember that this is a volunteer-driven project, and that code contributions are welcome :)

Development
===========

To set up `pyncette` for local development:

1. Fork `pyncette <https://github.com/tibordp/pyncette>`_
   (look for the "Fork" button).
2. Clone your fork locally::

    git clone git@github.com:tibordp/pyncette.git

3. Create a branch for local development::

    git checkout -b name-of-your-bugfix-or-feature
   
   Now you can make your changes locally.

4. Running integration tests assumes that there will be Redis, PostgreSQL, MySQL and Localstack (for DynamoDB) running on localhost. Alternatively, there is a Docker Compose environment that will set up all the backends so that integration tests can run seamlessly::

    docker-compose up -d
    docker-compose run --rm shell

5. When you're done making changes run all the checks and docs builder with `tox <https://tox.readthedocs.io/en/latest/install.html>`_ one command::

    tox

6. Pyncette uses `black` and `isort` to enforce formatting and import ordering. If you want to auto-format the code, you can do it like this::

    tox -e fmt

7. Commit your changes and push your branch to GitHub::

    git add .
    git commit -m "Your detailed description of your changes."
    git push origin name-of-your-bugfix-or-feature

8. Submit a pull request through the GitHub website.


If you run into issues setting up a local environment or testing the code locally, feel free to submit the PR anyway and GitHub Actions will test it for you.

Pull Request Guidelines
-----------------------

If you need some code review or feedback while you're developing the code just make the pull request.

For merging, you should:

1. Update documentation when there's new API, functionality etc.
2. Add a note to ``CHANGELOG.rst`` about the changes.
3. Add yourself to ``AUTHORS.rst``.

Tips
----

To run a subset of tests::

    tox -e envname -- pytest -k test_myfeature

To run all the test environments in *parallel* (see 
[tox documentation](https://tox.readthedocs.io/en/latest/example/basic.html#parallel-mode))::

    tox --parallel auto
