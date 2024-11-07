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
    def read(self, *fields):
        for f in fields:
            field = f'{f._entity.__name__}.{f._name} AS {f._name}'

            if field not in self._fields:
                self._fields.append(field)

    @chain
    def sum(self, *fields):
        self._grouped = True

        for f in fields:
            field = f'SUM({f._entity.__name__}.{f._name}) AS {f._name}_sum'

            if field not in self._fields:
                self._fields.append(field)

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

    def _produce_query(self):
        entity_name = self._entity.__name__

        mounter = Mounter()
        mounter.add(f'SELECT {m(self._fields)}')
        mounter.add(f'FROM {entity_name}')
        mounter.add(m(self._combine, '\n'))

        if self._where:
            mounter.add(f'WHERE {m(self._where, ' AND ')}')

        if self._grouped:
            mounter.add(f'GROUP BY {m(get_grouped_fields(self._fields))}')

        if self._max > 0:
            mounter.add(f'LIMIT {self._max}')
            mounter.add(f'OFFSET {self._offset}')

        return mounter.produce()

    def fetch(self):
        return as_dict(chquery(self._produce_query()), get_fields(self._fields))

    def count(self):
        return chquery(f'SELECT count(*) FROM ({self._produce_query()})')[0][0]
