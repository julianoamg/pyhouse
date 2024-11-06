from pyhouse.connection import connection
from pyhouse.fields import DataType, Type
from pyhouse.query import add_query, edit_query, search_query, create_query, drop_query
from pyhouse.utils import scan_attrs
from pyhouse.builder.base import Query


class Meta(type):
    def __init__(cls, name, bases, class_dict):
        super().__init__(name, bases, class_dict)

        id_attr = Type.UUID()
        id_attr._entity = cls

        setattr(cls, 'id', id_attr)

    def __getattribute__(cls, name):
        attr = super().__getattribute__(name)

        if isinstance(attr, DataType) and not hasattr(attr, '_name'):
            attr._name = name
        if isinstance(attr, DataType) and not hasattr(attr, '_entity'):
            attr._name = cls

        return attr


# noinspection SqlDialectInspection
class Entity(metaclass=Meta):
    _changed = None
    _added = None

    def __init__(self):
        self._undefined = True
        self._changed = {}
        self._added = True
        self._attrs = scan_attrs(self)

    def __setattr__(self, prop, content):
        super().__setattr__(prop, content)

        if not prop.startswith('_') and not getattr(self, '_undefined', True) and not self._added:
            if prop in self._attrs:
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
            cls.drop(_raw=_raw)
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

    def save(self, _raw=False, _async=False):
        if self._added:
            return add_query(self, _raw, _async)
        else:
            self._added = False
            return edit_query(self, self._changed, _raw)

    @classmethod
    def count(cls, **config):
        config.update({'_count': True})
        return cls.search(**config)
