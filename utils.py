def m(parts, between=', '):
    return f'{between}'.join(parts)


def as_dict(entries, _head):
    return [dict(zip(_head, entry)) for entry in entries]


def as_entity(entity, entries, _head):
    rows = []

    for row in as_dict(entries, _head):
        obj = entity()

        for k, v in row.items():
            if k in entity.__dict__:
                setattr(obj, k, v)

        setattr(obj, '_added', False)
        rows.append(obj)

    return rows