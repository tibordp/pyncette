
Changelog
=========

0.5.0 (2020-03-27)
------------------

* Fixes bug where a locked dynamic task could be executed again on next tick.
* poll_task is now reentrant with regards to locking. If the lease passed in matches the lease on the task, it behaves as though it were unlocked.


0.4.0 (2020-02-16)
------------------

* Middleware support and optional metrics via Prometheus
* Improved the graceful shutdown behavior
* Task instance and application context are now available in the task context
* Breaking change: dynamic task parameters are now accessed via `context.args['name']` instead of `context.name`
* Improved examples, documentation and packaging


0.2.0 (2020-01-08)
------------------

* Timezone support
* More efficient poling when Redis backend is used 


0.1.1 (2020-01-08)
------------------

* First release that actually works.


0.0.0 (2019-12-31)
------------------

* First release on PyPI.
