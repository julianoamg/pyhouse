from pyhouse.connection import connection
from pyhouse.head import DataType, Type
from pyhouse.query import add_query, edit_query, search_query, create_query, drop_query


# noinspection SqlDialectInspection
class Entity:
    _changed = None
    _added = None
    id = Type.UUID()

    def __init__(self):
        self._changed = {}
        self._added = True

    def __setattr__(self, prop, content):
        super().__setattr__(prop, content)
        if isinstance(content, DataType):
            self._changed.update({prop: True})

    @classmethod
    def _execute(cls, query, _raw=False):
        return connection.command(query).summary

    @classmethod
    def drop(cls, _raw=False):
        return drop_query(cls, _raw)

    @classmethod
    def create(cls, _raw=False, _recreate=False):
        if _recreate:
            drop_query(cls, _raw=True)
        return create_query(cls, _raw=_raw)

    @classmethod
    def search(cls, **config):
        return search_query(cls, **config)

    @classmethod
    def find(cls, **config):
        config['_max'] = 1
        rows = search_query(cls, **config)

        if config.get('_raw', False):
            return rows

        if rows:
            return rows[0]

    def save(self):
        if self._added:
            add_query(self)
        else:
            self._added = False
            edit_query(self, self._changed)

        return self

    @classmethod
    def count(cls, **config):
        config.update({'_count': True})
        return cls.search(**config)
