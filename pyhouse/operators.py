from datetime import datetime
from pyhouse.utils import m


def exp(field):
    return f'{field._entity.__name__}.{field._name}'


def cast_exp(func, field):
    return f'{func}({exp(field)})' if func else exp(field)


class ILIKE():
    def __init__(self, field, value):
        self.format = f"LOWER({exp(field)}) LIKE LOWER('%{value}%')"


class ISTARTS():
    def __init__(self, field, value):
        self.format = f"LOWER({exp(field)}) LIKE LOWER('{value}%')"


class IENDS():
    def __init__(self, field, value):
        self.format = f"LOWER({exp(field)}) LIKE LOWER('%{value}')"


class BETWEEN:
    def __init__(self, field, start, end, *, cast=None):
        self.date_checker(start)
        self.date_checker(end)
        self.format = f"{cast_exp(cast, field)} BETWEEN '{start}' AND '{end}'"

    def date_checker(self, date):
        datetime.fromisoformat(date)


class IN:
    def __init__(self, field, values, cast=None):
        values = [f"'{v}'" for v in values]
        self.format = f"{cast_exp(cast, field)} IN ({m(values)})"


class EQ:
    operator = '='

    def __init__(self, field, value, cast=None):
        self.format = f"{cast_exp(cast, field)} {self.operator} '{value}'"


class NEQ(EQ):
    operator = '!='


class GT(EQ):
    operator = '>'


class GTE(EQ):
    operator = '>='


class LT(EQ):
    operator = '<'


class LTE(EQ):
    operator = '<='

