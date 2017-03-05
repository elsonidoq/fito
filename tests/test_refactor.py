import unittest

from fito import DictDataStore, Operation
from fito import as_operation
from fito.data_store.refactor import StorageRefactor
from test_data_store import get_test_data_stores, clean_data_stores


@as_operation()
def test_operation_1(a, b=100): return a + b


@as_operation()
def test_operation_2(b, c=2): return b + c


@as_operation(a=Operation, b=Operation)
def test_operation_3(a, b=2):
    return a + b


class TestRefactor(unittest.TestCase):
    def setUp(self):
        self.data_stores = get_test_data_stores()
        ones = map(test_operation_1, xrange(10))
        twos = map(test_operation_2, xrange(10))

        threes = []
        for one in ones[:3]:
            for two in twos[:3]:
                threes.append(test_operation_3(one, two))

        operations = ones + twos + threes

        for ds in self.data_stores:
            for i, op in enumerate(operations):
                ds[op] = i

    def tearDown(self):
        clean_data_stores(self.data_stores)

    def test_change_type(self):
        refactor = (
            StorageRefactor()
            .change_type(test_operation_1, test_operation_2)
            .add_field(test_operation_1, 'c', 3)
            .remove_field(test_operation_1, 'a')
        )

        for ds in self.data_stores:
            out_ds = DictDataStore()
            ds.refactor(refactor, out_ds)

            for op, v in out_ds.iteritems():
                assert not isinstance(op, test_operation_1)
                if isinstance(op, test_operation_3):
                    op = op.a

                # It was originally test_operation_1
                if op.b == 100:
                    assert op.c == 3
                else:
                    assert op.c == 2

    def test_rename(self):
        refactor = (
            StorageRefactor()
            .rename_field(test_operation_1, 'tmp', 'b')
            .rename_field(test_operation_1, 'b', 'a')
            .rename_field(test_operation_1, 'a', 'tmp')
        )

        for ds in self.data_stores:
            out_ds = DictDataStore()
            ds.refactor(refactor, out_ds)

            bs = set()
            for op, v in out_ds.iteritems():
                if isinstance(op, test_operation_1):
                    # It was originally test_operation_1
                    assert op.a == 100
                    bs.add(op.b)
                elif isinstance(op, test_operation_3):
                    assert op.a.a == 100

            assert sorted(bs) == range(len(bs))


