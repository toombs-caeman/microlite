class Desc:
    def __set_name__(self, owner, name):
        self.owner = owner
        self.name = name

    def __init__(self, type) -> None:
        self.type = type

    def __get__(self, obj, _=None):
        if obj is None:
            pass  # called on type owner
        else:
            pass  # called on instance

        print(f"{self.owner}.{self.name}.__get__({self!r}, {obj!r}, {type!r}")

    def __set__(self, obj, value):
        print(f"{obj}.{self.name}.__set__({self!r}, {obj!r}, {value!r}")


class model:
    name = Desc(str)
    age = Desc(int)


model.name
model().name
model().name = 1
