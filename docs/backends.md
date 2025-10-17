# Backends

By default Pyncette runs without persistence. This means that the schedule is maintained in-memory and there is no coordination between multiple instances of the app.

Enabling persistence allows the aplication to recover from restarts as well as the ability to run multiple instances of an app concurrently without duplicate executions of tasks.

## SQLite

SQLite is the default peristence engine and is included in the base Python package.

```python
from pyncette import Pyncette, Context

app = Pyncette(sqlite_database="pyncette.db")


@app.task(schedule="* * * * * */10")
async def foo(context: Context):
    print("This will run every 10 seconds")


if __name__ == "__main__":
    app.main()
```

## Redis

Redis can be enabled by passing `redis_repository` as `repository_factory` parameter to the `Pyncette` constructor.

```python
from pyncette import Pyncette, Context
from pyncette.redis import redis_repository

app = Pyncette(repository_factory=redis_repository, redis_url="redis://localhost")
```

Optionally, the tasks can be namespaced if the Redis server is shared among different Pyncette apps:

```python
app = Pyncette(
    repository_factory=redis_repository,
    redis_url="redis://localhost",
    redis_namespace="my_super_app",
)
```

## PostgreSQL

Redis can be enabled by passing `postgres_repository` as `repository_factory` parameter to the `Pyncette` constructor.

```python
from pyncette import Pyncette, Context
from pyncette.postgres import postgres_repository

app = Pyncette(
    repository_factory=postgres_repository,
    postgres_url='postgres://postgres@localhost/pyncette'
    postgres_table_name='pyncette_tasks'
)
```

The table will be automatically initialized on startup if it does not exists unless `postgres_skip_table_create` is set to `True`.

## MySQL

MySQL can be configured by passing `mysql_repository` as `repository_factory` parameter to the `Pyncette` constructor.

The MySQL backend requires MySQL version 8.0+.

```python
from pyncette import Pyncette, Context
from pyncette.postgres import mysql_repository

app = Pyncette(
    repository_factory=mysql_repository,
    mysql_host="localhost",
    mysql_database="pyncette",
    mysql_user="pyncette",
    mysql_password="password",
    mysql_table_name="pyncette_tasks",
)
```

The table will be automatically initialized on startup if it does not exists unless `mysql_skip_table_create` is set to `True`.

## Amazon DynamoDB

Amazon DynamoDB backend can be configured with `dynamodb_repository`.

```python
from pyncette import Pyncette, Context
from pyncette.dynamodb import dynamodb_repository

app = Pyncette(
    repository_factory=dynamodb_repository,
    dynamodb_region_name="eu-west-1",
    dynamodb_table_name="pyncette",
)
```

DynamoDB repository will use [ambient credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#guide-credentials), such as environment variables, `~/.aws/config` or EC2 metadata service if e.g. running on EC2 or a Kubernetes cluster with kiam/kube2iam.

For convenience, an appropriate DynamoDB table will be automatically created on startup if it does not exist. The created table uses on-demand pricing model. If you would like to customize this behavior, you can manually create the table beforehand and pass `dynamodb_skip_table_create=True` in parameters.

Expected table schema should look something like this

```json
{
    "AttributeDefinitions": [
        { "AttributeName": "partition_id", "AttributeType": "S" },
        { "AttributeName": "ready_at", "AttributeType": "S" },
        { "AttributeName": "task_id", "AttributeType": "S" }
    ],
    "KeySchema": [
        { "AttributeName": "partition_id", "KeyType": "HASH" },
        { "AttributeName": "task_id", "KeyType": "RANGE" }
    ],
    "LocalSecondaryIndexes": [
        {
            "IndexName": "ready_at",
            "KeySchema": [
                { "AttributeName": "partition_id", "KeyType": "HASH" },
                { "AttributeName": "ready_at", "KeyType": "RANGE" }
            ],
            "Projection": {
                "ProjectionType": "ALL"
            }
        }
    ]
}
```
