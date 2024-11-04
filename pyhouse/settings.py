class Settings:
    def __init__(self):
        self._items = []

    def add(self, name, value):
        self._items.append(f'{name}={value}')

    def remove(self, name):
        for i, setting in enumerate(self._items):
            match, _ = setting.split('=')

            if name == match:
                del self._items[i]

    def as_query(self):
        return f"SETTINGS {', '.join(self._items)}"
