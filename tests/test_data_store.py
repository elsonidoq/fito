import inspect
import os
from random import Random
import shutil
import tempfile
import unittest

from fito import Spec
from fito import as_operation
from fito.data_store import file, dict_ds, mongo
from fito.data_store.mongo import get_collection, global_client
from test_operation import get_test_operations, partial, AddOperation
from test_spec import get_test_specs


def delete(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.unlink(path)


def get_test_data_stores():
    file_data_store_preffix = tempfile.mktemp()
    base_mongo_collection = get_collection(global_client, 'test.test')
    base_mongo_collection.drop()

    res = [
        dict_ds.DictDataStore(),

        mongo.MongoHashMap(base_mongo_collection),
        mongo.MongoHashMap(base_mongo_collection.with_get_cache, get_cache_size=10),
        mongo.MongoHashMap(base_mongo_collection.with_exec_cache, execute_cache_size=5),
        mongo.MongoHashMap(base_mongo_collection.with_incremental_id, add_incremental_id=True),

        file.FileDataStore(file_data_store_preffix),
        file.FileDataStore(file_data_store_preffix + '_with_get_cache', get_cache_size=10),
        file.FileDataStore(file_data_store_preffix + '_with_exec_cache', execute_cache_size=5),
        file.FileDataStore(file_data_store_preffix + '_dont_split_keys', split_keys=False),
        file.FileDataStore(file_data_store_preffix + '_use_class_name', use_class_name=True),
    ]

    clean_data_stores(res)

    return res


def clean_data_stores(data_stores):
    for store in data_stores:
        if isinstance(store, file.FileDataStore):
            delete(store.path)
        elif isinstance(store, mongo.MongoHashMap):
            store.clean()


class TestDataStore(unittest.TestCase):
    def setUp(self):
        self.data_stores = get_test_data_stores()

        # This is just because MongoHashMap does not handle ints on dictionary keys
        test_specs = get_test_specs(only_lists=True)
        test_operations = get_test_operations()

        self.rnd = Random(42)
        self.rnd.shuffle(test_specs)
        self.rnd.shuffle(test_operations)

        self.indexed_operations = test_operations[:len(test_operations) / 2]
        self.indexed_specs = test_specs[:len(test_specs) / 2] + self.indexed_operations
        self.not_indexed_specs = test_specs[len(test_specs) / 2:] + test_operations[len(test_operations) / 2:]

        self.cached_functions = []
        for i, ds in enumerate(self.data_stores):
            # Populate the data stores
            for j, spec in enumerate(self.indexed_specs):
                ds[spec] = j

        # it's defined inside an instance method hehehe
        if type(func).__name__ == 'FunctionWrapper':
            # func might be changed during tests, we need it to be a function
            module = inspect.getmodule(TestDataStore)
            setattr(
                module,
                'func',
                func.operation_class.func
            )

    def tearDown(self):
        clean_data_stores(self.data_stores)

    def test_iter_items(self):
        for ds in self.data_stores:
            for spec, i in ds.iteritems():
                assert self.indexed_specs[i] == spec

    def test_to_dict(self):
        for ds in self.data_stores:
            assert ds == Spec.dict2spec(ds.to_dict())

    def test_get(self):
        for ds in self.data_stores:
            for i, spec in enumerate(self.indexed_specs):
                assert ds[spec] == i
                assert ds.get(spec) == i
                assert ds.get_or_none(spec) == i
                assert spec in ds

            for spec in self.not_indexed_specs:
                self.assertRaises(KeyError, ds.get, spec)
                assert ds.get_or_none(spec) is None
                assert spec not in ds

    def test_keys(self):
        for ds in self.data_stores:
            assert sorted(ds.iterkeys()) == sorted(self.indexed_specs)

    def test_cache(self):
        orig_func = func

        module = inspect.getmodule(TestDataStore)
        for i, ds in enumerate(self.data_stores):
            OperationClass = as_operation(cache_on=ds)(orig_func)
            setattr(module, 'func', OperationClass)

            for j in xrange(10):
                op = OperationClass(j)
                assert op not in ds
                value = ds.execute(op)
                assert op in ds
                assert op.apply(ds) == value

    def test_autosave(self):
        orig_func = func

        module = inspect.getmodule(TestDataStore)
        for i, ds in enumerate(self.data_stores):
            autosaved_func = ds.autosave()(orig_func)
            setattr(module, 'func', autosaved_func)

            for j in xrange(10):
                op = autosaved_func.operation_class(j)
                assert op not in ds
                value = autosaved_func(j)
                assert op in ds
                assert op.execute() == value

            if ds.execute_cache is not None:
                assert len(ds.execute_cache.queue) == 5  # all instances with execute cache have a size == 5
                for j in xrange(5, 10):
                    assert autosaved_func.operation_class(j) in ds.execute_cache.queue

    def test_find_similar(self):
        add_operations = [
            e for e in self.rnd.sample(self.indexed_operations, 2) + self.rnd.sample(self.not_indexed_specs, 2)
            if isinstance(e, AddOperation)
            ]

        for i, ds in enumerate(self.data_stores):
            for j in xrange(5):
                p = partial(j).bind(1)
                matching = ds.find_similar(p)

                d_p = p.to_dict()

                for match, score in matching:
                    d_match = match.to_dict()

                    expected = 0
                    for k, v in d_p.iteritems():
                        in_match = k in d_match
                        if in_match:
                            expected += v == d_match[k]
                        expected += in_match

                    assert score == expected

            for add_op in add_operations:
                matching = ds.find_similar(add_op)
                # It's a mess to compute the score without using matching_fields
                best_match, score = matching[0]
                assert (best_match == add_op) == (add_op in ds)

    def test_remove(self):
        for ds in self.data_stores:
            # Copy the keys to avoid errors in DictDataStore
            for spec in list(ds.iterkeys()):
                ds.remove(spec)

            self.assertRaises(StopIteration, ds.iteritems().next)

    def test_get_by_id(self):
        for i, ds in enumerate(self.data_stores):
            for j, (id, doc) in enumerate(ds.iterkeys(raw=True)):
                # Not gonna perform this test on these kind of specs, I might even remove them in the future
                if isinstance(doc['type'], basestring) and '@' in doc['type']: continue

                spec = Spec.dict2spec(doc)
                v = ds[spec]
                assert ds[id] == v
                assert ds[doc] == v
                assert ds[ds.get_id(spec)] == v

            for spec in self.not_indexed_specs:
                self.assertRaises(KeyError, ds.get_id, spec)
                ds[spec] = 1
                assert ds[ds.get_id(spec)] == 1


def func(i):
    return i
