import unittest

from fito import SpecField
from fito.operations.decorate import as_operation
from fito.operations.operation import Operation
from fito.specs.base import InvalidSpecInstance, Spec


class TestOperation(unittest.TestCase):

    def test_as_operation(self):

        @as_operation(op=SpecField)
        def f(op, val=1): return val

        class Test(object):
            @as_operation(method_type='instance', op=SpecField)
            def op1(self, op, val=1): return 1

            @as_operation(method_type='class', op=SpecField)
            def op2(cls, op): return 1

        ops = [f, Test().op1, Test.op2]
        for op in ops:
            assert issubclass(op, Operation)
            self.assertRaises(InvalidSpecInstance, op, 1)
            op(Spec()).apply(None)
