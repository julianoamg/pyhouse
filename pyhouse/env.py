import os


def from_env(name='.env'):
    if not os.path.exists(name):
        return

    with open(name) as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)

                if value.startswith(("'", '"')) and value.endswith(("'", '"')):
                    value = value[1:-1]

                os.environ[key] = value

