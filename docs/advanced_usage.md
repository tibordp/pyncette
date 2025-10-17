# Advanced usage

## Partitioned dynamic tasks

Certain backends, like Redis and Amazon DynamoDB have a natural partitioning to them. Generally, when using
dynamic tasks, the task name is used as a partition key. For example, in DynamoDB, each dynamic task instance
is associated with one row/document, but they all share the same partition id.

Similarly for Redis, each task instance record is stored in its own key, but the index that sets them in order of
next execution is stored in a single key, so a single large task will not benefit from a clustered Redis setup.

If there is a very large number of dynamic task instances associated with a single task or they are polled
very frequently, this can lead to hot partitions and degraded performance. There can also be limits as to how many
task instances can even be stored in a single partition. For DynamoDB, the limit is 10GB.

Pyncette supports transparent partitioning of tasks through `partitioned_task` decorator.

```python
from pyncette import Pyncette, Context

app = Pyncette()


@app.partitioned_task(partition_count=32)
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

This splits the dynamic task into 32 partitions and the task instances are automatically assigned to them based on the hash of the task instance name.

The default partition selector uses SHA1 hash of the instance name, but a custom selector can be provided:

```python
def custom_partition_selector(partition_count: int, task_id: str) -> int:
    return (
        hash(task_id) % partition_count
    )  # Do not use this, as the hash() is not stable


@app.partitioned_task(partition_count=32, partition_selector=custom_partition_selector)
async def hello(context: Context) -> None:
    print(f"Hello {context.args['username']}")
```

### Choosing the partition count

Care must be taken when selecting a pertition count, as it is not easy to change it later after tasks have already been
scheduled. Changing a partition count will generally map task instances to a different partition, making them not run and also
making it impossible to unschedule them through `unschedule_task`.

There is also a tradeoff as the time complexity as a single Pyncette poll grows linearly with the total number of tasks (or their
partitions). Setting the number of partitions too high can lead to diminished performance due to the polling overhead.

It is possible to configure Pyncette to only poll certain partitions using the `enabled_partitions` parameter. This will allow the
tasks to be scheduled and unscheduled by any application instance, but only the partitions selected will be polled. You may use
this if you have a large number of instances for a given task in order to spread the load evenly among them.

```python
@app.partitioned_task(
    partition_count=8,
    # Partitions 4, 5, 6 and 7 will not be polled
    enabled_partitions=[0, 1, 2, 3],
)
async def hello(context: Context) -> None:
    print(f"Hello {context.args['username']}")
```
