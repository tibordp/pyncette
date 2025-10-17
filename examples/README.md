# Examples

## [basic.py](./basic.py)

Hello world example.

## [persistence.py](./persistence.py)

This example stores the state of the scheduler in a variety of backends supported by Pyncette

By having a persistent backend, you can run multiple multiple processes and they will coordinate
execution among them, making sure that tasks are only executed by one of them on schedule.

## [dynamic_tasks.py](./dynamic_tasks.py)

This example illustrates dynamic tasks i.e. tasks that are not pre-defined in code and
can be scheduled at runtime.

Marking the function with `@app.dynamic_task` serves as a template and individual task
instances can be scheduled with `schedule_task` (and unscheduled with `unschedule_task`).

Using a persistent backend, Pyncette supports efficient execution of a large number of
dynamic task instances.

## [delay_queue.py](./delay_queue.py)

This example uses Pyncette to implement a reliable delay queue (persistence is needed for durability
or for running multiple instances of the app at the same time, see [examples/persistence.py](./persistence.py) for details)

After the task instance suceeds it will not be scheduled again as with recurrent tasks, however,
if an exception is raised, it will be retried if `ExecutionMode.AT_LEAST_ONCE` is used.

## [fixtures_and_middlewares.py](./fixtures_and_middlewares.py)

This example illustrates the use of fixtures and middlewares.

Middlewares are functions that wrap the execution of every defined task, so they are a good
place to put cross-cutting concerns such as logging, database session management, metrics, ...

Fixtures can be thought of application-level middlewares. They wrap the lifecycle of the entire
Pyncette app and can be used to perform initialization, cleanup and can inject resources such as
service clients to the task context.

## [healthcheck.py](./healthcheck.py)

This example illustrates the use of healthcheck HTTP server. It exposes the /health endpoint
which returns 200 if last successfull poll was less than 2 poll intervals ago, 500 otherwise.

```
curl localhost:8080/health
```

## [heartbeat.py](./heartbeat.py)

This example demonstrates the heartbeating functionality, which allows for the lease on the
task to be extended. This can be useful if tasks have an unpredictable run time to minimize
the risk of another instance taking over the lease.

Heartbeating can be either cooperative or automatic.

## [prometheus_metrics.py](./prometheus_metrics.py)

Pyncette ships with an optional Prometheus instrumentation based on the official prometheus_client
Python package. It includes the following metrics:

- Tick duration [Histogram]
- Tick volume [Counter]
- Tick failures [Counter]
- Number of currently executing ticks [Gauge]
- Task duration [Histogram]
- Task volume [Counter]
- Task failures [Counter]
- Number of currently executing tasks [Gauge]
- Task run staleness (i.e. how far behind the scheduled time the actual executions are) [Histogram]
- Repository operation duration [Histogram]
- Repository operation volume [Counter]
- Repository operation volume [Failures]
- Number of currently repository operations [Gauge]

It pushes the metrics to default registry (`prometheus_client.REGISTRY`), so it can be combined with other
code alongside it.

To see the exported metrics while running this example, use something like

```
curl localhost:9699/metrics
```

## [benchmark.py](./benchmark.py)

This example schedules a large number of dynamic tasks and then runs them (in multiple processes) as a way
to gauge the total throughput of Pyncette for a particular backend.

To run this example, configure the selected backend in the Pyncette constructor, then run populate the database.

```
python examples/benchmark.py populate -n <number of tasks to insert>
```

While the tasks are populating you can run

```
python examples/benchmark.py run --processes <# of processes>
```

The process will continuously print the overall throughput (task executions per second) and the lag (seconds since the last successful tick).
