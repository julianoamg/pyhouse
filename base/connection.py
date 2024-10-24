import clickhouse_connect
import os


def connection_factory(
    custom_connection=None, 
    host=os.getenv('CLICKHOUSE_HOST', default='localhost'), 
    username=os.getenv('CLICKHOUSE_USERNAME', default='default'), 
    password=os.getenv('CLICKHOUSE_PASSWORD', default='password')):
        if custom_connection:
            return custom_connection

        return clickhouse_connect.get_client(
            host=host,
            username=username,
            password=password
        )


connection = connection_factory()
