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

If Pyncette is run alongside other code or for customization, :meth:`~pyncette.Pyncette.create` can be used to initialize the runtime environment and then the main loop can be run with :meth:`~pyncette.PyncetteContext.run`::

    import asyncio
    from pyncette import Pyncette
    
    app = Pyncette()

    ...
    
    async with app.create() as ctx:
        await ctx.run()

Specifying the schedule
-----------------------

There are two ways a schedule can be specified, one is with the cron-like syntax (uses :mod:`croniter` under the hood to support the calculation)::

    @app.task(schedule="* * * * *")
    async def every_minute(context: Context):
        ...

    @app.task(schedule="* * * * * */10")
    async def every_10_seconds(context: Context):
        ...

    @app.task(schedule="20 4 * * * *")
    async def every_day_at_4_20_am(context: Context):
        ...

The other way is with an interval::

    @app.task(interval=datetime.timedelta(seconds=12))
    async def every_12_seconds(context: Context):
        ...

Customizing tasks
-----------------

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

    @app.task(interval=datetime.timedelta(seconds=10), execution_mode=ExecutionMode.AT_MOST_ONCE)
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

Timezone support
++++++++++++++++

Pyncette is timezone-aware, the timezone for a task can be specified by ``timezone`` parameter:

.. code-block:: python

    from pyncette import ExecutionMode

    @app.task(schedule="0 12 * * *", timezone="Europe/Dublin") 
    async def task1(context: Context):
        print(f"Hello from Dublin!")

    @app.task(schedule="0 12 * * *", timezone="UTC+12") 
    async def task2(context: Context):
        print(f"Hello from Камча́тка!")

The accepted values are all that :meth:`dateutil.tz.gettz` accepts. 

.. caution:: There is a known issue where the execution is offset by 1 hour in the day DST takes effect. See `details here <https://github.com/taichino/croniter/issues/116>`_

Task parameters
++++++++++++++++

The :meth:`~pyncette.Pyncette.task` decorator accepts an arbitrary number of additional parameters, which are available through the ``context`` parameter

.. code-block:: python

    from pyncette import ExecutionMode

    # If we use multiple decorators on the same coroutine, we must explicitely provide the name
    @app.task(name="task1", interval=datetime.timedelta(seconds=10), username="abra") 
    @app.task(name="task2", interval=datetime.timedelta(seconds=20), username="kadabra")
    @app.task(name="task3", interval=datetime.timedelta(seconds=30), username="alakazam")
    async def task(context: Context):
        print(f"{context.username}")

This allows for parametrized tasks with multiple decorators, this is an essential feature needed to support :ref:`dynamic-tasks`.

.. note:: There is a restriction that all the values of the parameters must be JSON-serializable, since they are persisted in storage when dynamic tasks are used.

Middlewares
-----------

If you have common logic that should execute around every task invocation, middlewares can be used. Good examples of middlewares are ones used for logging and metrics.

.. code-block:: py

    app = Pyncette()
    
    @app.middleware
    async def retry(context: Context, next: Callable[[], Awaitable[None]]):
        # Prefer to rely on Pyncette to drive task retry logic
        for _ in range(5):
            try:
                await next()
                return
            except Exception as e:
                pass
        raise Exception(f"Task {context.task.name} failed too many times.")

    @app.middleware
    async def logging(context: Context, next: Callable[[], Awaitable[None]]):
        logger.info(f"Task {context.task.name} started")
        try:
            await next()
        except Exception as e:
            logger.error(f"Task {context.task.name} failed", e)
            raise        

    @app.middleware
    async def db_transaction(context: Context, next: Callable[[], Awaitable[None]]):
        context.db.begin_transaction()
        try:
            await next()
        except Exception:
            context.db.rollback()
            raise            
        else:
            context.db.commit()

Middlewares execute in order they are defined.

Fixtures
--------

Fixtures provide a convenient way for injecting dependencies into tasks, and specifying the set-up and tear-down code. They can be though of as application-level middlewares. For example, let's say we want to inject the database and a logfile as dependencies to all our tasks::

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

The lifetime of a fixture is that of a Pyncette application, i.e. the setup code for all fixtures runs before the first tick and the tear-down code runs after the graceful shutdown is initiated and all the pending tasks have finished. Like middlewares, fixtures execute in the order they are defined (and in reverse order on shutdown).


Persistence
-----------

By default Pyncette runs without persistence. This means that the schedule is mainteined in-memory and there is no coordination between multiple instances of the app.

Enabling persistence allows the aplication to recover from restarts as well as the ability to run multiple instances of an app concurrently without duplicate executions of tasks. At the moment, Redis is the only persistence backend included (though you can write your own!)

Redis
+++++

Redis can be enabled by passing :meth:`~pyncette.redis.redis_repository` as ``repository_factory`` parameter to the :class:`~pyncette.Pyncette` constructor.

.. code-block:: py

    from pyncette import Pyncette, Context
    from pyncette.redis import redis_repository

    app = Pyncette(repository_factory=redis_repository, redis_url='redis://localhost')

    @app.task(schedule='* * * * * */10')
    async def foo(context: Context):
        print('This will run every 10 seconds')

    if __name__ == '__main__':
        app.main()

Optionally, the tasks can be namespaced if the Redis server is shared among different Pyncette apps::

    app = Pyncette(repository_factory=redis_repository, redis_url='redis://localhost', redis_namespace='my_super_app')

.. _dynamic-tasks:

Dynamic tasks
-------------

Pyncette supports a use case where the tasks are not necessarily known in advance with :meth:`~pyncette.PyncetteContext.schedule_task`.

.. code-block:: python

    @app.dynamic_task()
    async def hello(context: Context) -> None:
        print(f"Hello {context.username}")

    async with app.create() as ctx:
        await asyncio.gather(
            ctx.schedule_task(hello, "bill_task", schedule="0 * * * *", username="bill"),
            ctx.schedule_task(hello, "steve_task", schedule="20 * * * *", username="steve"),
            ctx.schedule_task(hello, "john_task", schedule="40 * * * *", username="john"),
        )
        await ctx.run()

When persistence is used, the schedules and task parameters of the are persisted alongside the execution data, which allows the tasks to be registered and unregistered at will. 

An example use case is a web application where every user can have something happen at their chosen schedule. Polling is relatively efficient, since the concrete instances of the dynamic class are only loaded from the storage if the are already due, instead of being polled all the time. 

The task instances can be removed by :meth:`~pyncette.PyncetteContext.unschedule_task`

.. code-block:: python

    ...

    async with app.create() as ctx:
        await ctx.schedule_task(hello, "bill_task", schedule="0 * * * *", username="bill")
        await ctx.unschedule_task(hello, "bill_task")
        await ctx.run()

.. note::

    If the number of dynamic tasks is large, it is a good idea to limit the batch size::

        app = Pyncette(
            repository_factory=redis_repository, 
            redis_url='redis://localhost', 
            redis_batch_size=100
        )

    This will cause that only a specified number of dynamic tasks are scheduled for execution during a single tick, as well as allow potential multiple instances of the same app to load balance effectively.

Performance
-----------

Tasks are executed in parallel. If you have a lot of long running tasks, you can set ``concurrency_limit`` in :class:`~pyncette.Pyncette` constructor, as this ensures that there are at most that many executing tasks at any given time. If there are no free slots in the semaphore, this will serve as a back-pressure and ensure that we don't poll additional tasks until some of the currently executing ones finish, enabling the pending tasks to be scheduled on other instances of your app. Setting ``concurrency_limit`` to 1 is equivalent of serializing the execution of all the tasks.