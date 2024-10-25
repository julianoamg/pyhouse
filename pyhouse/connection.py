from env import from_env
import clickhouse_connect
import os

from_env()

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', default='localhost')
CLICKHOUSE_USERNAME = os.getenv('CLICKHOUSE_USERNAME', default='default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', default='password')


def connection_factory(
    host=CLICKHOUSE_HOST,
    username=CLICKHOUSE_USERNAME,
    password=CLICKHOUSE_PASSWORD
):
    return clickhouse_connect.get_client(
        host=host,
        username=username,
        password=password
    )


try:
    connection = connection_factory()
except clickhouse_connect.driver.exceptions.OperationalError:
    connection = None
