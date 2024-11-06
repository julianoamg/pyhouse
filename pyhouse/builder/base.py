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

    def __init__(self, entity):
        self._entity = entity

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
            self._fields.append(f'{f._entity.__name__}.{f._name} AS {f._name}')

    @chain
    def sum(self, *fields):
        self._grouped = True

        for f in fields:
            self._fields.append(f'SUM({f._entity.__name__}.{f._name}) AS {f._name}_sum')

    @chain
    def where(self, **conditions: dict):
        ...

    @chain
    def offset(self, n: int):
        self._offset = n

    @chain
    def max(self, n: int):
        self._max = n

    def fetch(self):
        entity_name = self._entity.__name__

        mounter = Mounter()
        mounter.add(f'SELECT {m(self._fields)}')
        mounter.add(f'FROM {entity_name}')
        mounter.add(m(self._combine, '\n'))

        if self._grouped:
            mounter.add(f'GROUP BY {m(get_grouped_fields(self._fields))}')

        mounter.add(f'LIMIT {self._max}')
        mounter.add(f'OFFSET {self._offset}')

        return as_dict(chquery(mounter.produce()), get_fields(self._fields))
