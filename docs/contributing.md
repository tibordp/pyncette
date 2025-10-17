# Contributing

Contributions are welcome, and they are greatly appreciated! Every
little bit helps, and credit will always be given.

## Bug reports

When [reporting a bug](https://github.com/tibordp/pyncette/issues) please include:

- Your operating system name and version.
- Any details about your local setup that might be helpful in troubleshooting.
- Detailed steps to reproduce the bug.

## Documentation improvements

Pyncette could always use more documentation, whether as part of the
official Pyncette docs, in docstrings, or even on the web in blog posts,
articles, and such.

## Feature requests and feedback

The best way to send feedback is to file an issue at https://github.com/tibordp/pyncette/issues.

If you are proposing a feature:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that this is a volunteer-driven project, and that code contributions are welcome :)

## Development

To set up `pyncette` for local development:

1. Fork [pyncette](https://github.com/tibordp/pyncette)
   (look for the "Fork" button).

1. Clone your fork locally:

   ```bash
   git clone git@github.com:tibordp/pyncette.git
   ```

1. Create a branch for local development:

   ```bash
   git checkout -b name-of-your-bugfix-or-feature
   ```

   Now you can make your changes locally.

1. Set up your development environment:

   ```bash
   uv sync --extra all --extra dev
   ```

1. Running integration tests assumes that there will be Redis, PostgreSQL, MySQL and Localstack (for DynamoDB) running on localhost. Alternatively, there is a Docker Compose environment that will set up all the backends so that integration tests can run seamlessly:

   ```bash
   docker-compose up -d
   docker-compose run --rm shell
   ```

1. When you're done making changes, run all the checks:

   ```bash
   # Run linting and formatting
   uv run pre-commit run --all-files

   # Run type checking
   uv run ty check src examples

   # Run tests
   uv run pytest tests

   # Build documentation
   uv run mkdocs build
   ```

1. Commit your changes and push your branch to GitHub:

   ```bash
   git add .
   git commit -m "Your detailed description of your changes."
   git push origin name-of-your-bugfix-or-feature
   ```

1. Submit a pull request through the GitHub website.

If you run into issues setting up a local environment or testing the code locally, feel free to submit the PR anyway and GitHub Actions will test it for you.

## Pull Request Guidelines

If you need some code review or feedback while you're developing the code just make the pull request.

For merging, you should:

1. Update documentation when there's new API, functionality etc.
1. Add a note to `docs/changelog.md` about the changes.

## Tips

To run a subset of tests:

```bash
uv run pytest -k test_myfeature
```

To run tests for a specific Python version:

```bash
uv venv --python 3.11
uv sync --extra all --extra dev
uv run pytest tests
```
