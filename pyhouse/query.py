import sqlglot
from pyhouse.connection import connection
from pyhouse.head import DataType
from pyhouse.utils import m, as_dict, as_entity, scan_attrs


def props_spec(entity, props):
    return (
        entity.__class__.__name__,
        ', '.join(props.keys()),
        ', '.join(props.values()))


def props_factory(entity, changed=None):
    if not changed:
        changed = {}

    props = {}

    for name, prop in entity.__class__.__dict__.items():
        if changed and name not in changed:
            continue

        if isinstance(prop, DataType):
            body = getattr(entity, name)
            body = getattr(body, 'content', body)
            props[name] = body or prop.params.get('predef')

            if callable(props[name]):
                props[name] = props[name]()
            else:
                props[name] = f"'{props[name]}'"

    return props_spec(entity, props)


def head_spec(entity, config):
    return config.get('_head') or [
        k for k, v in entity.__dict__.items() if isinstance(v, DataType)
    ]


def where_mount(entity, config):
    exp = m([
        f"{k} = '{config[k]}'"
        for k, v in entity.__dict__.items()
        if isinstance(v, DataType) and k in config
    ], ' AND ')

    return f"WHERE {exp}" if exp else ''


def search_spec(entity, config):
    _max = config.get('_max')
    _max = f'LIMIT {_max}' if _max else ''

    return (
        _max,
        config.get('_raw', False),
        config.get('_tiny', False),
        config.get('_dict', False),
        config.get('_count', False),
        head_spec(entity, config),
        entity.__name__
    )


def head_mount(_count, _head):
    return 'count()' if _count else m(_head)


def pretty_query(query):
    return sqlglot.transpile(query, read='clickhouse', pretty=True)[0]


def search_query(entity, **config):
    _max, _raw, _tiny, _dict, _count, _head, entity_name = search_spec(entity, config)
    query = f"SELECT {head_mount(_count, _head)} FROM {entity_name} {where_mount(entity, config)} {_max};"

    if _raw:
        return pretty_query(query)

    rows = connection.query(query).result_rows

    if _count:
        return rows[0][0]
    if _tiny:
        return rows, _head
    if _dict:
        return as_dict(rows, _head)

    return as_entity(entity, rows, _head)


def write_spec(entity, changed=None):
    spec = props_factory(entity, changed)
    return spec[0], spec[1], spec[2]


def add_query(entity, _raw):
    spec = write_spec(entity)
    query = f"INSERT INTO {spec[0]} ({spec[1]}) VALUES ({spec[2]})"

    if _raw:
        return pretty_query(query)

    return connection.command().summary


def edit_query(entity, changed, _raw):
    props = write_spec(entity, changed)
    spec = m([f'{x}={y}' for x, y in zip(props[1], props[2]) for props in props])
    query = f"ALTER TABLE {props[0]} UPDATE ({spec})"

    if _raw:
        return pretty_query(query)

    return connection.command().summary


def create_query(entity, _raw):
    definition = [f'{k} {v.name}' for k, v in scan_attrs(entity).items() if isinstance(v, DataType)]
    query = f"""
        CREATE TABLE IF NOT EXISTS {entity.__name__} 
        ({m(definition)})
        ENGINE = MergeTree 
        ORDER BY id PRIMARY KEY id;
    """
    if _raw:
        return pretty_query(query)
    return connection.command(query).summary


def drop_query(entity, _raw):
    query = f"DROP TABLE IF EXISTS {entity.__name__};"
    return pretty_query(query) if _raw else connection.command(query).summary
