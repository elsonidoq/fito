from fito import Operation, OperationField, PrimitiveField
from fito import DictDataStore


class GetAge(Operation):
    user_id = PrimitiveField(0)

    def _apply(self, data_store):
        return data_store.get(self)

    def add(self, other):
        return AddOperation(self, other)

    def __repr__(self):
        return "get_age(user_id={})".format(self.user_id)


class AddOperation(Operation):
    left = OperationField(0)
    right = OperationField(1)

    def _apply(self, data_store):
        return data_store.execute(self.left) + data_store.execute(self.right)

    def __repr__(self):
        return "{} + {}".format(self.left, self.right)


data_store = DictDataStore()
data_store[GetAge(1)] = 30
data_store[GetAge(2)] = 41

op = GetAge(1).add(GetAge(2))

print
print "Data store"
print data_store.data

print
print "Operation: {}".format(op)

print
print "Result: {}".format(data_store.execute(op))
