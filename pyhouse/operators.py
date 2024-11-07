from datetime import datetime


def exp(field):
    return f'{field._entity.__name__}.{field._name}'


def cast_exp(func, field):
    return f'{func}({exp(field)})' if func else exp(field)


class Between:
    def __init__(self, field, start, end, *, cast=None):
        self.date_checker(start)
        self.date_checker(end)
        self.format = f"{cast_exp(cast, field)} BETWEEN '{start}' AND '{end}'"

    def date_checker(self, date):
        datetime.fromisoformat(date)
