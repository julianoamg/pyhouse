from pyhouse.fields import DataType
from pyhouse.connection import cursor


def m(parts, between=', '):
    return f'{between}'.join(parts)


def as_dict(entries, _fields):
    return [dict(zip(_fields, entry)) for entry in entries]


def as_entity(entity, entries, _fields):
    rows = []

    for row in as_dict(entries, _fields):
        obj = entity()

        for k, v in row.items():
            if k in entity.__dict__:
                setattr(obj, k, v)

        setattr(obj, '_added', False)
        setattr(obj, '_undefined', False)
        rows.append(obj)

    return rows


def scan_attrs(instance):
    attrs = {}

    for attr in dir(instance):
        if not attr.startswith('__'):
            attr_v = getattr(instance, attr)

            if not isinstance(attr_v, DataType):
                continue

            try:
                attrs[attr] = attr_v
            except AttributeError:
                pass

    return attrs


def chquery(query):
    cursor.execute(query)
    return cursor.fetchall()
