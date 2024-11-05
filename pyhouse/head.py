from functools import partial


def func(definition):
    return partial(lambda d: d, definition)


class DataType:
    def __init__(self, name, **params):
        self.name = name
        self.content = None
        self._params = params

    @property
    def params(self):
        return self._params


def ctype(name, **kwargs):
    return partial(DataType, name, **kwargs)


class IntegerType:
    UInt8 = ctype('UInt8', predef=0)
    UInt16 = ctype('UInt16', predef=0)
    UInt32 = ctype('UInt32', predef=0)
    UInt64 = ctype('UInt64', predef=0)
    UInt128 = ctype('UInt128', predef=0)
    UInt256 = ctype('UInt256', predef=0)
    Int8 = ctype('Int8', predef=0)
    Int16 = ctype('Int16', predef=0)
    Int32 = ctype('Int32', predef=0)
    Int64 = ctype('Int64', predef=0)
    Int128 = ctype('Int128', predef=0)
    Int256 = ctype('Int256', predef=0)


class FloatType:
    Float32 = ctype('Float32', predef=0.0)
    Float64 = ctype('Float64', predef=0.0)


class BooleanType:
    Bool = ctype('Bool', predef=0)


class StringType:
    String = ctype('String', predef='')
    FixedString = ctype('FixedString', predef='')


class DateType:
    Date = ctype('Date', predef=func('now()'))
    Date32 = ctype('Date32', predef=func('now()'))
    DateTime = ctype('DateTime', predef=func('now()'))
    DateTime64 = ctype('DateTime64', predef=func('now()'))


class UUIDType:
    UUID = ctype('UUID', predef=func('generateUUIDv4()'))


class Type(
    IntegerType,
    FloatType,
    BooleanType,
    StringType,
    DateType,
    UUIDType
):
    pass
