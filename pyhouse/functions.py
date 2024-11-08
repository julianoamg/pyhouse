import sqlglot
from pyhouse.connection import cursor
from pyhouse.fields import DataType
from pyhouse.utils import m, as_dict, as_entity, scan_attrs
from pyhouse.settings import Settings


def props_spec(entity, props):
    return (
        entity.__class__.__name__,
        ', '.join(props.keys()),
        ', '.join(props.values()))


def props_factory(entity, changed=None):
    if not changed:
        changed = {}

    props = {}

    for name, prop in entity._attrs.items():
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

    return entity, props


def head_spec(entity, config):
    return config.get('_fields') or scan_attrs(entity).keys()


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


def head_mount(_count, _fields):
    return 'count()' if _count else m(_fields)


def pretty_query(query):
    try:
        return sqlglot.transpile(query, read='clickhouse', pretty=True)[0]
    except sqlglot.errors.ParseError:
        return query


def search_query(entity, **config):
    _max, _raw, _tiny, _dict, _count, _fields, entity_name = search_spec(entity, config)
    query = f"SELECT {head_mount(_count, _fields)} FROM {entity_name} {where_mount(entity, config)} {_max};"

    if _raw:
        return pretty_query(query)

    cursor.execute(query)
    rows = cursor.fetchall()

    if _count:
        return rows[0][0]
    if _tiny:
        return rows, _fields
    if _dict:
        return as_dict(rows, _fields)

    return as_entity(entity, rows, _fields)


def write_spec(entity, changed=None):
    return props_factory(entity, changed)


def add_query(entity, _raw, _async):
    spec = props_spec(*write_spec(entity))
    settings = Settings()

    if _async:
        settings.add('async_insert', '1')
        settings.add('wait_for_async_insert', '0')

    query = f"INSERT INTO {spec[0]} ({spec[1]}) {settings.as_query()} VALUES ({spec[2]})"

    if _raw:
        return pretty_query(query)

    cursor.execute(query)
    return cursor.fetchone()


def edit_query(entity, changed, _raw):
    spec = write_spec(entity, changed)
    entity = spec[0]
    name = entity.__class__.__name__

    definition = ', '.join([f'{k}={v}' for k, v in spec[1].items()])
    query = f"ALTER TABLE {name} UPDATE {definition} WHERE id='{entity.id}'"

    if _raw:
        return pretty_query(query)

    cursor.execute(query)
    return cursor.fetchone()


def create_query(entity, _raw):
    definition = []

    mapping = {
        'DateTime': 'timezone',
        'DateTime64': 'timezone'
    }

    for name, prop in scan_attrs(entity).items():
        param = ''

        if prop.name in mapping:
            param = prop.params.get(mapping[prop.name])
            param = f"('{param}')" if param else ''

        definition.append(f'{name} {prop.name}{param}')

    query = f"""
        CREATE TABLE IF NOT EXISTS {entity.__name__} 
        ({m(definition)})
        ENGINE = MergeTree 
        ORDER BY id PRIMARY KEY id;
    """
    if _raw:
        return pretty_query(query)
    return cursor.execute(query)


def drop_query(entity, _raw):
    query = f"DROP TABLE IF EXISTS {entity.__name__};"
    return pretty_query(query) if _raw else cursor.execute(query)
