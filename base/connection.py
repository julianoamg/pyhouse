import clickhouse_connect


def connection_factory(custom_connection=None, host='localhost', username='default', password='password'):
        if custom_connection:
            return custom_connection

        return clickhouse_connect.get_client(
            host=host,
            username=username,
            password=password
        )


connection = connection_factory()
