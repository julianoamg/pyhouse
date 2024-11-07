from pyhouse.utils import chquery, as_dict, m


def chain(method):
    def wrapper(invoker, *args, **kwargs):
        method(invoker, *args, **kwargs)
        return invoker
    return wrapper


def get_fields(fields):
    return [f.split(' AS ')[-1] for f in fields]


def get_grouped_fields(fields):
    return [f.split(' AS ')[-1] for f in fields if 'sum(' not in f.lower()]


def format_exp(f, func, with_entity=True, fsuffix=''):
    if isinstance(f, SubQuery):
        return f'({f.query()})'
    _entity_name = f'{f._entity.__name__}.' if with_entity else ''
    _exp = f if isinstance(f, str) else f'{_entity_name}{f._name}{fsuffix}'
    _exp = f'{func}({_exp})' if func else _exp
    return _exp


def mount_fields(fields, kw_fields, with_entity=True, func='', fsuffix='', suffix=''):
    _fields = []

    for f in fields:
        _exp = format_exp(f, func, with_entity, fsuffix)
        field = f'{_exp} AS {f._name}{suffix}'

        if field not in _fields:
            _fields.append(field)

    for n, f in kw_fields.items():
        _exp = format_exp(f, func, with_entity, fsuffix)
        field = f'{_exp} AS {n}'

        if field not in _fields:
            _fields.append(field)

    return _fields


class CombineType:
    INNER = 'INNER'


class Mounter:
    def __init__(self):
        self.query = []

    def add(self, piece):
        self.query.append(piece)

    def produce(self):
        return m(self.query, ' ')


class Query:
    def __init__(self, entity, alias=None, as_field=None):
        self._entity = entity

        if alias:
            self._alias = f'{entity.__name__} AS {alias}'
        else:
            self._alias = entity.__name__

        self._fields = []
        self._offset = 0
        self._name = as_field or f'{entity.__name__}_query'
        self._max = 100
        self._combine = []
        self._grouped = False
        self._where = []
        self._order_by = []
        self._query = None

    def __len__(self):
        return self.count()

    @chain
    def combine(self, *conditions, _type=CombineType.INNER):
        for c in conditions:
            _0 = c[0] if isinstance(c[0], str) else f'{c[0]._entity.__name__}.{c[0]._name}'
            _1 = c[1] if isinstance(c[1], str) else f'{c[1]._entity.__name__}.{c[1]._name}'
            _0_name = _0.split('.')[0]
            self._combine.append(f'{_type} JOIN {_0_name} ON {_0} = {_1}')

    @chain
    def read(self, *fields, **kw_fields):
        for f in mount_fields(fields, kw_fields):
            if f not in self._fields:
                self._fields.append(f)

    @chain
    def sum(self, *fields, **kw_fields):
        self._grouped = True

        for f in mount_fields(fields, kw_fields, func='SUM', suffix='_sum'):
            if f not in self._fields:
                self._fields.append(f)

    @chain
    def offset(self, n: int):
        self._offset = n

    @chain
    def max(self, n: int):
        self._max = n

    @chain
    def where(self, *conditions, _type='AND'):
        cache = []

        for c in conditions:
            if c not in self._where:
                cache.append(c.format)

        if cache:
            _type = f' {_type.strip()} '
            self._where.append(f'({m(cache, _type)})')

    @chain
    def order_by(self, *fields):
        for f in fields:
            if isinstance(f, str):
                order = f'{f.lstrip("-")} {"DESC" if f.startswith("-") else "ASC"}'
            else:
                exp = f[0] if isinstance(f[0], str) else f'{f[0]._entity.__name__}.{f[0]._name}'
                order = f'{exp} {f[1]}'

            self._order_by.append(order)

    def _produce_query(self, _max):
        entity_name = self._alias

        mounter = Mounter()
        mounter.add(f'SELECT {m(self._fields)}')
        mounter.add(f'FROM {entity_name}')
        mounter.add(m(self._combine, '\n'))

        if self._where:
            mounter.add(f'WHERE {m(self._where, ' AND ')}')

        if self._grouped:
            mounter.add(f'GROUP BY {m(get_grouped_fields(self._fields))}')

        if self._order_by:
            mounter.add(f'ORDER BY {m(self._order_by)}')

        if self._max > 0 and _max > 0:
            mounter.add(f'LIMIT {self._max}')
            mounter.add(f'OFFSET {self._offset}')

        return mounter.produce()

    def query(self, _max=None):
        if _max is None:
            _max = self._max
        self._query = self._produce_query(_max=_max)
        return self._query

    def fetch(self):
        return as_dict(chquery(self.query(_max=self._max)), get_fields(self._fields))

    def count(self, _raw=False):
        query = f'SELECT count(*) FROM ({self.query()})'
        return query if _raw else chquery(query)[0][0]

    def unify(self, *fields, func="SUM", suffix='_unify', _raw=False, **kw_fields):
        _fields = mount_fields(fields, kw_fields, with_entity=False, func=func, fsuffix='_sum', suffix=suffix)
        query = f'SELECT {m(_fields)} FROM ({self.query()})'
        return query if _raw else as_dict(chquery(query), get_fields(_fields))


class SubQuery(Query):
    ...
