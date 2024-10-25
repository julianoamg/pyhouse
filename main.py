from head import Type
from entities import Entity


class User(Entity):
    first_name = Type.String()
    last_name = Type.String()
    age = Type.Int8()


print(User.create(_raw=True))
# User.create()
# User.drop()
# User.search()
# User.find()
# User.add()
# User.remove()
# User.update()
# User.save_as()
# User.save()