import unittest
from random import Random

from fito.operation_runner import OperationRunner
from fito.operations.operation import Operation
from fito.specs.base import NumericField, SpecField


class SentinelOperation(Operation):
    def __init__(self, *args, **kwargs):
        super(SentinelOperation, self).__init__(*args, **kwargs)
        self.times_run = 0

    def apply(self, runner):
        self.times_run += 1


class Numeric(SentinelOperation):
    def __mul__(self, other):
        return MultiplyOperation(self, other)


class GetNumber(Numeric):
    input = NumericField(0)

    def apply(self, runner):
        super(GetNumber, self).apply(runner)
        return self.input + 1

    def __repr__(self):
        return "{}".format(self.input)


class MultiplyOperation(Numeric):
    a = SpecField(0)
    b = SpecField(1)

    def apply(self, runner):
        super(MultiplyOperation, self).apply(runner)
        return runner.execute(self.a) * runner.execute(self.b)

    def __repr__(self):
        return '{} * {}'.format(self.a, self.b)


class TestOperation(unittest.TestCase):
    def setUp(self):
        # create some numbers
        numbers = [GetNumber(i) for i in xrange(10)]

        # multiply random numbers
        rnd = Random(42)
        for i in xrange(15):
            a = rnd.choice(numbers)
            b = rnd.choice(numbers)
            numbers.append(a * b)

        self.operations = numbers

    def test_base(self):
        # test whether everything works without caching
        runner = OperationRunner(cache_size=0)
        self.run_and_assert(runner, 1)

    def test_fifo(self):
        runner = OperationRunner(cache_size=len(self.operations))

        # everything should be run once
        self.run_and_assert(runner, 1)

        # everything should be cached
        self.run_and_assert(runner, 1)

        # forget first operation
        # fifo shittiest case
        runner.execute(GetNumber(100))

        # run everything again
        self.run_and_assert(runner, 2)

    def run_and_assert(self, runner, cnt):
        for op in self.operations:
            runner.execute(op)
            assert op.times_run == cnt
