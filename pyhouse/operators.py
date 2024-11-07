from datetime import datetime


def exp(field):
    return f'{field._entity.__name__}.{field._name}'


def cast_exp(func, field):
    return f'{func}({exp(field)})' if func else exp(field)


class BETWEEN:
    def __init__(self, field, start, end, *, cast=None):
        self.date_checker(start)
        self.date_checker(end)
        self.format = f"{cast_exp(cast, field)} BETWEEN '{start}' AND '{end}'"

    def date_checker(self, date):
        datetime.fromisoformat(date)


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

