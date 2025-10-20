# Changelog

## 1.1.0 (2025-10-20)

### Features

- Added MongoDB backend support (`pip install pyncette[mongodb]`)
  - Uses Motor (async MongoDB driver) for efficient async operations
  - Implements optimistic locking using version-based CAS (Compare-And-Swap)
  - Compatible with MongoDB 4.2+
  - Supports all Pyncette features including dynamic tasks and partitioning
- Added `force` parameter to `schedule_task` to control behavior when updating dynamic task instances
  - Default (`force=False`): Fails with `TaskLockedException` if task is locked, preserves sooner execution time to prevent schedule postponement
  - With `force=True`: Unconditionally overwrites task state including locks and schedule
- Added new task state query API for inspecting both static and dynamic tasks
  - `get_task(task, instance_name=None)` - Query the state of any task (static or dynamic)
    - For static tasks: `get_task(my_task)`
    - For dynamic tasks: `get_task(template, "instance")` or `get_task(concrete_instance)`
  - `list_tasks(parent_task, limit=None, continuation_token=None)` - List all instances of a dynamic task with pagination
  - Returns `TaskState` objects containing task, scheduled_at, locked_until, and locked_by
  - Implemented across all backends: PostgreSQL, MySQL, SQLite, DynamoDB, Redis, MongoDB

### Bug Fixes

- Fixed race condition in `schedule_task` where calling it while a task was executing could clear locks and cause duplicate execution
- Fixed schedule starvation issue where repeated `schedule_task` calls would indefinitely postpone task execution

### Performance Improvements

- Switched MySQL backend to READ COMMITTED isolation level to eliminate gap lock deadlocks and improve concurrency
- Optimized Postgres and MySQL `register_task` to use upsert (`INSERT ON CONFLICT`/`INSERT ON DUPLICATE KEY UPDATE`) for force mode

### API Changes

- Added `TaskLockedException` exception type for clearer error handling when tasks are locked
- Updated `register_task` across all backends to accept `force` parameter
- Added new `get_task_state()` method to Repository interface for querying individual task state
- Added new `list_task_states()` method to Repository interface for paginated listing of dynamic task instances
- Added new public API methods to `PyncetteContext`:
  - `get_task(task, instance_name=None)` - Query state of static or dynamic tasks
  - `list_tasks(parent_task, limit=None, continuation_token=None)` - List dynamic task instances with pagination
- Added new model classes: `TaskState` (task state snapshot) and `ListTasksResponse` (paginated results)

## 1.0.0 (2025-10-17)

### Breaking Changes

- **Dropped Python 3.8 support** - Minimum Python version is now 3.9

### Packaging and Tooling Modernization

- Migrated from `setup.py` to modern PEP 621 `pyproject.toml` with hatchling build backend
- Replaced tox with uv for dependency management and testing across all workflows
- Updated GitHub Actions workflows to use native uv commands (`uv sync`, `uv run`, `uv build`)
- Migrated documentation from Sphinx/reStructuredText to MkDocs Material/Markdown
- Set up automatic API documentation generation with mkdocstrings
- Replaced mypy with ty for type checking
- Consolidated linting/formatting to use Ruff (replacing black, isort, pyupgrade)
- Modernized pre-commit hooks configuration
- Updated Docker development environment to install uv

### Bug Fixes

- Fixed Python 3.14 compatibility: Converted all SQLite SQL queries to use consistent named parameter style (`:name`) instead of mixing PostgreSQL-style (`$1`), qmark (`?`), and named parameters
- Fixed latent bug in `poll_task` where lease comparison failed due to UUID vs string type mismatch
- Fixed bug in `poll_dynamic_task` where optimistic locking was not working due to incorrect parameter binding

### Documentation

- Converted all documentation files from `.rst` to `.md` format
- Updated all development instructions to use uv commands
- Added relevant PyPI keywords for better discoverability

## 0.11.0 (2024-11-25)

- Add support for Python 3.12 and 3.13

## 0.10.1 (2023-05-09)

- Include missing lua files in the built wheel

## 0.10.0 (2023-05-08)

- Drop support for Python 3.7
- Add support for Python 3.11
- Modernize Python package structure and linters
- Fix a few bugs and type annotations

## 0.8.1 (2021-04-08)

- Improve performance for calculation of the next execution time
- Add ability for repositories to pass a pagination token
- Add `add_to_context()` to inject static data to context
- Clean up documentation and add additional examples

## 0.8.0 (2021-04-05)

- Added Amazon DynamoDB backend
- Added MySQL backend
- Added support for partitioned dynamic tasks

## 0.7.0 (2021-03-31)

- Added support for automatic and cooperative lease heartbeating
- PostgreSQL backend can now skip automatic table creation
- Improved signal handling
- CI: Add Codecov integration
- Devenv: Run integration tests in Docker Compose

## 0.6.1 (2020-04-02)

- Optimize the task querying on Postgres backend
- Fix: ensure that there are no name colissions between concrete instances of different dynamic tasks
- Improve fairness of polling tasks under high contention.

## 0.6.0 (2020-03-31)

- Added PostgreSQL backend
- Added Sqlite backend and made it the default (replacing `InMemoryRepository`)
- Refactored test suite to cover all conformance/integration tests on all backends
- Refactored Redis backend, simplifying the Lua scripts and improving exceptional case handling (e.g. tasks disappearing between query and poll)
- Main loop only sleeps for the rest of remaining `poll_interval` before next tick instead of the full amount
- General bug fixes, documentation changes, clean up

## 0.5.0 (2020-03-27)

- Fixes bug where a locked dynamic task could be executed again on next tick.
- poll_task is now reentrant with regards to locking. If the lease passed in matches the lease on the task, it behaves as though it were unlocked.

## 0.4.0 (2020-02-16)

- Middleware support and optional metrics via Prometheus
- Improved the graceful shutdown behavior
- Task instance and application context are now available in the task context
- Breaking change: dynamic task parameters are now accessed via `context.args['name']` instead of `context.name`
- Improved examples, documentation and packaging

## 0.2.0 (2020-01-08)

- Timezone support
- More efficient poling when Redis backend is used

## 0.1.1 (2020-01-08)

- First release that actually works.

## 0.0.0 (2019-12-31)

- First release on PyPI.
