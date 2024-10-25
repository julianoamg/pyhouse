from pyhouse.head import DataType


class PropertyNotFound(Exception):
    def __init__(self, name):
        super().__init__(f'{name} attributes was not previous declared in class definition')


class DataTypeError(Exception):
    def __init__(self, name):
        super().__init__(f'{name} must be of type {DataType.__name__}')
