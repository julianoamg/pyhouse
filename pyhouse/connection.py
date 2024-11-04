import os
from pyhouse.env import from_env
from clickhouse_driver import connect

from_env()

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', default='localhost')
CLICKHOUSE_PORT = os.getenv('CLICKHOUSE_USERNAME', default='9000')
CLICKHOUSE_USERNAME = os.getenv('CLICKHOUSE_USERNAME', default='default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', default='password')
CLICKHOUSE_DATABASE = os.getenv('CLICKHOUSE_DATABASE', default='default')


def connection_factory(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USERNAME,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE,
):
    return connect(f'clickhouse://{username}:{password}@{host}:{port}/{database}')


connection = connection_factory()
cursor = connection.cursor()
