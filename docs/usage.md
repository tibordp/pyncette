# Usage

The core unit of execution in Pyncette is a `Task`. Each task is a Python coroutine that specifies what needs to be executed.

```python
from pyncette import Pyncette, Context

app = Pyncette()


@app.task(interval=datetime.timedelta(seconds=2))
async def successful_task(context: Context) -> None:
    print("This will execute every second")


if __name__ == "__main__":
    app.main()
```

## Running the main loop

The usual use case is that Pyncette runs as its own process, so the standard way to start the main loop is with `main` method of the `Pyncette`. This sets up the logging to standard output and signal handler allowing for graceful shutdown (first SIGINT initiates the graceful shutdown and the second one terminates the process).

If Pyncette is run alongside other code or for customization, `create` can be used to initialize the runtime environment and then the main loop can be run with `run`:

```python
import asyncio
from pyncette import Pyncette

app = Pyncette()

...

async with app.create() as app_context:
    await app_context.run()
```

## Specifying the schedule

There are two ways a schedule can be specified, one is with the cron-like syntax (uses `croniter` under the hood to support the calculation):

```python
@app.task(schedule="* * * * *")
async def every_minute(context: Context): ...


@app.task(schedule="* * * * * */10")
async def every_10_seconds(context: Context): ...


@app.task(schedule="20 4 * * * *")
async def every_day_at_4_20_am(context: Context): ...
```

The other way is with an interval:

```python
@app.task(interval=datetime.timedelta(seconds=12))
async def every_12_seconds(context: Context): ...
```

## Customizing tasks

Pyncette supports multiple different execution modes which provide different levels of reliability guarantees, depending on the nature of the task.

The default task configuration:

- When the task is scheduled for execution, it is locked for 60 seconds
- If the task execution succeeds, the next execution is scheduled and the task is unlocked
- If the task execution fails (exception is raised), the lock is not released, so it will be retried after the lease expires.
- If the task execution exceeds the lease duration, it will be executed again (so there could be two executions at the same time)

### Best-effort tasks

If the task is run in a best-effort mode, locking will not be employed, and the next execution will be scheduled immediately when it becomes ready.

```python
from pyncette import ExecutionMode


@app.task(
    interval=datetime.timedelta(seconds=10), execution_mode=ExecutionMode.AT_MOST_ONCE
)
async def every_10_seconds(context: Context):
    print("Ping")
```

!!!caution
If best effort is used, there is no way to retry a failed execution, and exceptions thrown by the task will only be logged.

### Failure behavior

Failure behavior can be specified with `failure_mode` parameter:

```python
from pyncette import ExecutionMode


@app.task(interval=datetime.timedelta(seconds=10), failure_mode=FailureMode.UNLOCK)
async def every_10_seconds(context: Context):
    print("Ping")
```

- `FailureMode.NONE` the task will stay locked until the lease expires. This is the default.
- `FailureMode.UNLOCK` the task will be immediately unlocked if an exception is thrown, so it will be retried on the next tick.
- `FailureMode.COMMIT` treat the exception as a success and schedule the next execution in case the exception is thrown.

### Timezone support

Pyncette is timezone-aware, the timezone for a task can be specified by `timezone` parameter:

```python
from pyncette import ExecutionMode


@app.task(schedule="0 12 * * *", timezone="Europe/Dublin")
async def task1(context: Context):
    print(f"Hello from Dublin!")


@app.task(schedule="0 12 * * *", timezone="UTC+12")
async def task2(context: Context):
    print(f"Hello from Камча́тка!")
```

The accepted values are all that `dateutil.tz.gettz` accepts.

### Disabling a task

Tasks can be disabled by passing an `enabled=False` in the parameters. This can be used for example
to conditionally enable tasks only on certain instances.

```python
@app.task(schedule="* * * * *", enabled=False)
async def task1(context: Context):
    print(f"This will never run.")
```

Tasks can be disabled also in the initialization code:

```python
from pyncette import Pyncette, Context

app = Pyncette()


@app.task(schedule="* * * * *")
async def task1(context: Context):
    print(f"This will never run.")


async with app.create() as app_context:
    task1.enabled = False
    await app_context.run()
```

### Task parameters

The `task` decorator accepts an arbitrary number of additional parameters, which are available through the `context` parameter

```python
from pyncette import ExecutionMode


# If we use multiple decorators on the same coroutine, we must explicitely provide the name
@app.task(name="task1", interval=datetime.timedelta(seconds=10), username="abra")
@app.task(name="task2", interval=datetime.timedelta(seconds=20), username="kadabra")
@app.task(name="task3", interval=datetime.timedelta(seconds=30), username="alakazam")
async def task(context: Context):
    print(f"{context.args['username']}")
```

This allows for parametrized tasks with multiple decorators, this is an essential feature needed to support dynamic tasks.

!!!note
There is a restriction that all the values of the parameters must be JSON-serializable, since they are persisted in storage when dynamic tasks are used.

## Middlewares

If you have common logic that should execute around every task invocation, middlewares can be used. Good examples of middlewares are ones used for logging and metrics.

```python
app = Pyncette()


@app.middleware
async def retry(context: Context, next: Callable[[], Awaitable[None]]):
    # Example only, prefer to rely on Pyncette to drive task retry logic
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
```

Middlewares execute in order they are defined.

## Fixtures

Fixtures provide a convenient way for injecting dependencies into tasks, and specifying the set-up and tear-down code. They can be though of as application-level middlewares. For example, let's say we want to inject the database and a logfile as dependencies to all our tasks:

```python
app = Pyncette()


@app.fixture()
async def db(app_context: PyncetteContext):
    db = await database.connect(...)
    try:
        yield db
    finally:
        await db.close()


@app.fixture(name="super_log_file")
async def logfile(app_context: PyncetteContext):
    with open("log.txt", "a") as file:
        yield file


@app.task(interval=datetime.timedelta(seconds=2))
async def successful_task(context: Context) -> None:
    context.super_log_file.write("Querying the database")
    results = await context.db.query(...)
    ...
```

The lifetime of a fixture is that of a Pyncette application, i.e. the setup code for all fixtures runs before the first tick and the tear-down code runs after the graceful shutdown is initiated and all the pending tasks have finished. Like middlewares, fixtures execute in the order they are defined (and in reverse order on shutdown).

## Persistence

By default Pyncette runs without persistence. This means that the schedule is mainteined in-memory and there is no coordination between multiple instances of the app.

Enabling persistence allows the aplication to recover from restarts as well as the ability to run multiple instances of an app concurrently without duplicate executions of tasks.

See [Backends](backends.md) for instructions on how to configure persistence for a database of your choice.

## Heartbeating

If have tasks that have an unpredictable run time, it can be hard to come up with an appropriate lease duration in advance. If set too short, lease will expire, leading to duplicate task execution and if too long, there can be insufficient protection against unhealthy workers.

A way to mitigate is to use heartbeating. Heartbeating will periodically extend the lease on the task as long as task is still running. Pyncette supports two approaches to heartbeating:

- Cooperative heartbeating: your task periodically calls `context.heartbeat()` to extend the lease
- Automatic heartbeating: your task is decorated with `with_heartbeat` and it heartbeats automatically in the background for as long as the task is executing.

Beware that automatic heartbeating can potentially be dangerous if, for example, your task is stuck in an infinite loop or an I/O operation that does not have a proper time out. In this case the lease can be kept alive indefinitely and the task will not make any progress. Cooperative heartbeating may be more verbose, but offers a greater degree of control.

If `context.heartbeat()` is called when the lease is already lost, the call will raise `LeaseLostException`, allowing you to bail out early, since another instance is likely already processing the same task.

```python
from pyncette.utils import with_heartbeat


@app.task(schedule="* * * * * */10")
@with_heartbeat()
async def foo(context: Context):
    # The task will be kept alive by the heartbeat
    await asyncio.sleep(3600)


if __name__ == "__main__":
    app.main()
```

## Dynamic tasks

Pyncette supports a use case where the tasks are not necessarily known in advance with `schedule_task`.

```python
@app.dynamic_task()
async def hello(context: Context) -> None:
    print(f"Hello {context.args['username']}")


async with app.create() as app_context:
    await asyncio.gather(
        app_context.schedule_task(
            hello, "bill_task", schedule="0 * * * *", username="bill"
        ),
        app_context.schedule_task(
            hello, "steve_task", schedule="20 * * * *", username="steve"
        ),
        app_context.schedule_task(
            hello, "john_task", schedule="40 * * * *", username="john"
        ),
    )
    await app_context.run()
```

When persistence is used, the schedules and task parameters of the are persisted alongside the execution data, which allows the tasks to be registered and unregistered at will.

An example use case is a web application where every user can have something happen at their chosen schedule. Polling is efficient, since the concrete instances of the dynamic class are only loaded from the storage if the are already due, instead of being polled all the time.

The task instances can be removed by `unschedule_task`

```python
...

async with app.create() as app_context:
    await app_context.schedule_task(
        hello, "bill_task", schedule="0 * * * *", username="bill"
    )
    await app_context.unschedule_task(hello, "bill_task")
    await app_context.run()
```

!!!note
If the number of dynamic tasks is large, it is a good idea to limit the batch size:

````
```python
app = Pyncette(
    repository_factory=redis_repository,
    redis_url='redis://localhost',
    batch_size=10
)
```

This will cause that only a specified number of dynamic tasks are scheduled for execution during a single tick, as well as allow potential multiple instances of the same app to load balance effectively.
````

## Once-off dynamic tasks

Dynamic tasks can also be scheduled to execute only once at a specific date.

```python
@app.dynamic_task()
async def task(context: Context) -> None:
    print(f"Hello {context.task.name}!")


async with app.create() as app_context:
    await app_context.schedule_task(
        task, "y2k38", execute_at=datetime(2038, 1, 19, 3, 14, 7)
    )
    await app_context.schedule_task(
        task, "tomorrow", execute_at=datetime.now() + timedelta(days=1)
    )

    # This will execute once immediately, since it is already overdue
    await app_context.schedule_task(
        task, "overdue", execute_at=datetime.now() - timedelta(days=1)
    )
    await app_context.run()
```

Once-off tasks have the same reliability guarantees as recurrent tasks, which is controlled by `execution_mode` and `failure_mode` parameters, but in case of success, they will not be scheduled again.

## Performance

Tasks are executed in parallel. If you have a lot of long running tasks, you can set `concurrency_limit` in `Pyncette` constructor, as this ensures that there are at most that many executing tasks at any given time. If there are no free slots in the semaphore, this will serve as a back-pressure and ensure that we don't poll additional tasks until some of the currently executing ones finish, enabling the pending tasks to be scheduled on other instances of your app. Setting `concurrency_limit` to 1 is equivalent of serializing the execution of all the tasks.

Depending on the backend used, having a dynamic task with a very large number of instances can lead to diminished performance. See [Advanced Usage](advanced_usage.md) for a way to address this issue.
