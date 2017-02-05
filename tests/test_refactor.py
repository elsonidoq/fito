import unittest

from fito import DictDataStore
from fito import as_operation
from fito.data_store import MongoHashMap
from fito.data_store.refactor import StorageRefactor
from test_data_store import get_test_data_stores, clean_data_stores


@as_operation()
def test_operation_1(a, b=100): return a + b


@as_operation()
def test_operation_2(b, c=2): return b + c


class TestRefactor(unittest.TestCase):
    def setUp(self):
        self.data_stores = get_test_data_stores()
        operations = map(test_operation_1, xrange(10))
        operations.extend(map(test_operation_2, xrange(10)))

        for ds in self.data_stores:
            for i, op in enumerate(operations):
                ds[op] = i

    def tearDown(self):
        clean_data_stores(self.data_stores)

    def test_change_type(self):
        refactor = (
            StorageRefactor()
                .add_field('c', 3)
                .remove_field('a')
                .change_type(test_operation_2)
                .filter_by_type(test_operation_1)
        )

        for ds in self.data_stores:
            out_ds = DictDataStore()
            ds.refactor(refactor, out_ds)

            for op, v in out_ds.iteritems():
                assert isinstance(op, test_operation_2)
                # It was originally test_operation_1
                if op.b == 100:
                    assert op.c == 3
                else:
                    assert op.c == 2

    def test_rename(self):
        refactor = (
            StorageRefactor()
                .rename_field('a', 'tmp')
                .rename_field('b', 'a')
                .rename_field('tmp', 'b')
                .filter_by_type(test_operation_1)
        )

        for ds in self.data_stores:
            out_ds = DictDataStore()
            ds.refactor(refactor, out_ds)

            bs = set()
            for op, v in out_ds.iteritems():
                if isinstance(op, test_operation_2): continue
                # It was originally test_operation_1
                assert op.a == 100
                bs.add(op.b)

            assert sorted(bs) == range(len(bs))

