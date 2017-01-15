import unittest

from fito import DictDataStore
from fito import SpecField
from fito.operations.decorate import as_operation
from fito.operations.operation import Operation, MemoryObject
from fito.specs.base import PrimitiveField


@as_operation()
def base_case(i=0):
    return i


@as_operation(op=SpecField)
def f(op, val=1): return val


class Test(object):
    @as_operation(method_type='instance', op=SpecField(base_type=base_case))
    def op1(self, op, val=1): return 1

    @as_operation(method_type='class', op=SpecField(base_type=base_case))
    def op2(cls, op): return 1


def get_test_operations():
    res = [
        base_case(),
        f(base_case()),
        Test().op1(base_case()),
        Test.op2(base_case()),
    ]

    input = DictDataStore()
    numbers = [Number(i) for i in xrange(10)]
    for number in numbers: input[number] = number.n * 2

    # rnd = Random(42)
    # for i in xrange(10):
    #     numbers.append(rnd.choice(numbers) + rnd.choice(numbers))

    return res + numbers


class Numeric(Operation):
    def __add__(self, other):
        return AddOperation(self, other)


class Number(Numeric):
    n = PrimitiveField(0)

    def apply(self, runner):
        return self.n


class AddOperation(Numeric):
    left = SpecField(0, base_type=Numeric)
    right = SpecField(1, base_type=Numeric)

    def apply(self, runner):
        return runner.execute(self.left) + runner.execute(self.right)

    def __repr__(self):
        return '{} + {}'.format(self.left, self.right)


class List(MemoryObject):
    l = PrimitiveField(0)


class TestOperation(unittest.TestCase):
    def test_methods(self):
        for op in get_test_operations():
            assert isinstance(op, Operation)
            op.execute()
            repr(op)

    def test_memory_object(self):
        l = List(range(10))
        assert l.l == List.dict2spec(l.to_dict()).l
