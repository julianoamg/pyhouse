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
    _entity_name = f'{f._entity.__name__}.' if with_entity else ''
    _exp = f'{_entity_name}{f._name}{fsuffix}'
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
    query = []

    def add(self, piece):
        self.query.append(piece)

    def produce(self):
        return m(self.query, ' ')


class Query:
    _fields = []
    _offset = 0
    _max = 100
    _combine = []
    _grouped = False
    _where = []
    _order_by = []
    _query = None

    def __init__(self, entity):
        self._entity = entity

    def __len__(self):
        return self.count()

    @chain
    def combine(self, *conditions, _type=CombineType.INNER):
        for c in conditions:
            _0_name = c[0]._entity.__name__
            _0 = f'{_0_name}.{c[0]._name}'
            _1 = f'{c[1]._entity.__name__}.{c[1]._name}'
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
            exp = f[0] if isinstance(f[0], str) else f'{f[0]._entity.__name__}.{f[0]._name}'
            self._order_by.append(f'{exp} {f[1]}')

    def _produce_query(self, _max=_max):
        entity_name = self._entity.__name__

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

    def query(self, _max=_max):
        if not self._query:
            self._query = self._produce_query(_max=_max)
        return self._query

    def fetch(self):
        return as_dict(chquery(self.query()), get_fields(self._fields))

    def count(self):
        return chquery(f'SELECT count(*) FROM ({self.query()})')[0][0]

    def unify(self, *fields, func="SUM", suffix='_unify', _raw=False, **kw_fields):
        _fields = mount_fields(fields, kw_fields, with_entity=False, func=func, fsuffix='_sum', suffix=suffix)
        query = f'SELECT {m(_fields)} FROM ({self.query(_max=0)})'
        return query if _raw else as_dict(chquery(query), get_fields(_fields))
