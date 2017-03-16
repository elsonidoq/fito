import unittest
from random import Random

from fito import DictDataStore
from fito import SpecField
from fito.operations.decorate import as_operation
from fito.operations.operation import Operation, MemoryObject
from fito.specs.fields import UnboundPrimitiveField, UnboundSpecField
from fito.specs.base import PrimitiveField, Spec


@as_operation()
def base_case(i=0):
    return i


@as_operation(b=UnboundPrimitiveField)
def partial(a, b):
    return a + b


@as_operation(op=SpecField)
def f(op, val=1): return val


class ObjectWithOperations(object):
    @as_operation(method_type='instance', op=SpecField(base_type=base_case))
    def op1(self, op, val=1): return 1

    @as_operation(method_type='class', op=SpecField(base_type=base_case))
    def op2(cls, op): return 1


class SpecWithOperations(Operation):
    a = PrimitiveField(0)

    @as_operation(method_type='instance')
    def instance_method(self, b):
        return self.a + b

    @as_operation(method_type='class')
    def class_method(cls):
        return cls.a

    def apply(self, runner):
        return self.a + runner.execute(self.b)

    @as_operation(method_type='instance', b=UnboundPrimitiveField)
    def unbound_instance_method(self, b):
        return self.a + b

    @as_operation(method_type='class', b=UnboundSpecField)
    def unbound_class_method(cls, b):
        return b


def get_test_operations():
    res = [
        base_case(),
        f(base_case()),
        ObjectWithOperations.op2(base_case()),
        SpecWithOperations(1).instance_method(2),
        SpecWithOperations.class_method(),
        ObjectWithOperations().op1(base_case())
    ]

    input = DictDataStore()
    numbers = [Number(i) for i in xrange(10)]
    for number in numbers: input[number] = number.n * 2
    #
    rnd = Random(42)
    for i in xrange(10):
        numbers.append(rnd.choice(numbers) + rnd.choice(numbers))

    return res + numbers + get_unbound_operations(bound=True)


def get_unbound_operations(bound):
    primitive_bind = map(partial, range(10)) + [
        SpecWithOperations(0).unbound_instance_method(),
    ]
    spec_bind = [
        SpecWithOperations.unbound_class_method(),
    ]
    if bound:
        res = [e.bind(1) for e in primitive_bind] + [e.bind(Spec()) for e in spec_bind]
    return res


class Numeric(Operation):
    def __add__(self, other):
        return AddOperation(self, other)


class Number(Numeric):
    n = PrimitiveField(0)

    def apply(self, runner):
        return self.n

    def __repr__(self):
        return str(self.n)


class AddOperation(Numeric):
    left = SpecField(0, base_type=Numeric)
    right = SpecField(1, base_type=Numeric)

    def apply(self, runner):
        return runner.execute(self.left) + runner.execute(self.right)

    def __repr__(self):
        return '{} + {}'.format(self.left, self.right)


class TestOperation(unittest.TestCase):
    def test_methods(self):
        for op in get_test_operations():
            assert isinstance(op, Operation)
            op.execute()
            repr(op)

    def test_memory_object(self):
        l = MemoryObject(range(10))
        assert l.obj == MemoryObject.dict2spec(l.to_dict()).obj
