=====
Usage
=====

The core unit of execution in Pyncette is a :class:`~pyncette.Task`. Each task is a Python coroutine that specifies what needs to be executed. 

.. code-block:: py

    from pyncette import Pyncette, Context

    app = Pyncette()

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        print("This will execute every second")

    if __name__ == "__main__":
        app.main()


Running the main loop
---------------------

The usual use case is that Pyncette runs as its own process, so the standard way to start the main loop is with :meth:`~pyncette.Pyncette.main` method of the :class:`~pyncette.Pyncette`. This sets up the logging to standard output and signal handler allowing for graceful shutdown (first SIGINT initiates the graceful shutdown and the second one terminates the process).

If Pyncette is run alongside other code, :meth:`~pyncette.Pyncette.run` can be used::

    import asyncio
    from pyncette import Pyncette
    
    app = Pyncette()

    ...

    asyncio.run(app.run())


Specifying the schedule
-----------------------

There are two ways a schedule can be specified, one is with the cron-like syntax (uses :mod:`croniter` under the hood to support the calculation)::

    @app.task(schedule="* * * * *")
    async def every_minute(context: Context):
        ...

    @app.task(schedule="* * * * * */10")
    async def every_10_seconds(context: Context):
        ...

    @app.task(schedule="30 4 * * * *")
    async def every_day_at_4_30_pm(context: Context):
        ...

The other way is with an interval::

    @app.task(interval=datetime.timedelta(seconds=12))
    async def every_12_seconds(context: Context):
        ...


Execution modes
---------------

Pyncette supports multiple different execution modes which provide different levels of reliability guarantees, depending on the nature of the task.

The default task configuration:

- When the task is scheduled for execution, it is locked for 60 seconds
- If the task execution succeeds, the next execution is scheduled and the task is unlocked
- If the task execution fails (exception is raised), the lock is not released, so it will be retried after the lease expires.
- If the task execution exceeds the lease duration, it will be executed again (so there could be two executions at the same time)

Best-effort tasks
+++++++++++++++++

If the task is run in a best-effort mode, locking will not be employed, and the next execution will be scheduled immediately when it becomes ready.::

    from pyncette import ExecutionMode

    @app.task(interval=datetime.timedelta(seconds=10), execution_mode=ExecutionMode.BEST_EFFORT)
    async def every_10_seconds(context: Context):
        print("Ping")

.. caution:: If best effort is used, there is no way to retry a failed execution, and exceptions thrown by the task will only be logged.

Failure behavior
++++++++++++++++

Failure behavior can be specified with ``failure_mode`` parameter::

    from pyncette import ExecutionMode

    @app.task(interval=datetime.timedelta(seconds=10), failure_mode=FailureMode.UNLOCK)
    async def every_10_seconds(context: Context):
        print("Ping")


- ``FailureMode.NONE`` the task will stay locked until the lease expires. This is the default.
- ``FailureMode.UNLOCK`` the task will be immediately unlocked if an exception is thrown, so it will be retried on the next tick.
- ``FailureMode.COMMIT`` treat the exception as a success and schedule the next execution in case the exception is thrown.

Fixtures
--------

Fixtures provide a convenient way for injecting dependencies into tasks, and specifying the set-up and tear-down code. For example, let's say we want to inject the database and a logfile as dependencies to all our tasks::

    app = Pyncette()

    @app.fixture()
    async def db():
        db = await database.connect(...)
        try:
            yield db
        finally:
            await db.close()

    @app.fixture(name="super_log_file")
    async def logfile():
        with open("log.txt", "a") as file:
            yield file

    @app.task(interval=datetime.timedelta(seconds=2))
    async def successful_task(context: Context) -> None:
        context.super_log_file.write("Querying the database")
        results = await context.db.query(...)
        ...

The lifetime of a fixture is that of a Pyncette application, i.e. the setup code for all fixtures runs before the first tick and the tear-down code runs after the graceful shutdown is initiated and all the pending tasks have finished.


Persistence
-----------

By default Pyncette runs without persistence. This means that the schedule is mainteined in-memory and there is no coordination between multiple instances of the app.

Enabling persistence allows the aplication to recover from restarts as well as the ability to run multiple instances of an app concurrently without duplicate executions of tasks. At the moment, Redis is the only persistence backend included (though you can write your own!)

Redis
+++++

Redis can be enabled by passing :meth:`~pyncette.repository.redis.redis_repository` as ``repository_factory`` parameter to the :class:`~pyncette.Pyncette` constructor.

.. code-block:: py

    from pyncette import Pyncette, Context
    from pyncette.repository.redis import redis_repository

    app = Pyncette(repository_factory=redis_repository, redis_url='redis://localhost')

    @app.task(schedule='* * * * * */10')
    async def foo(context: Context):
        print('This will run every 10 seconds')

    if __name__ == '__main__':
        app.main()

Optionally, the tasks can be namespaced if the Redis server is shared among different Pyncette apps::

    app = Pyncette(repository_factory=redis_repository, redis_url='redis://localhost', redis_namespace='my_super_app')