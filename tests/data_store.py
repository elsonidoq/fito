import os
import shutil
import tempfile
import unittest

from fito.data_store import file, dict_ds, mongo
from fito.data_store.base import Get
from fito.data_store.mongo import get_collection, global_client
from spec import get_test_specs
from operation import get_test_operations


def delete(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.unlink(path)


class TestDataStore(unittest.TestCase):
    def setUp(self):
        file_data_store_preffix = tempfile.mktemp()
        base_mongo_collection = get_collection(global_client, 'test.test')
        base_mongo_collection.drop()

        self.data_stores = [
            mongo.MongoHashMap(base_mongo_collection),
            mongo.MongoHashMap(base_mongo_collection.with_get_cache, get_cache_size=10),
            mongo.MongoHashMap(base_mongo_collection.with_incremental_id, add_incremental_id=True),

            file.FileDataStore(file_data_store_preffix),
            file.FileDataStore(file_data_store_preffix + '_with_get_cache', get_cache_size=10),
            file.FileDataStore(file_data_store_preffix + '_dont_split_keys', split_keys=False),

            dict_ds.DictDataStore(),
        ]

        # This is just because of MongoHashMap
        test_specs = get_test_specs(only_lists=True)
        test_operations = get_test_operations()
        self.indexed_operations = test_operations[:len(test_operations) / 2]
        self.indexed_specs = test_specs[:len(test_specs) / 2] + self.indexed_operations
        self.not_indexed_specs = test_specs[len(test_specs) / 2:] + test_operations[len(test_operations) / 2:]

        for ds in self.data_stores:
            for i, spec in enumerate(self.indexed_specs):
                ds[spec] = i

    def tearDown(self):
        for store in self.data_stores:
            if isinstance(store, file.FileDataStore):
                delete(store.path)
            elif isinstance(store, mongo.MongoHashMap):
                store.coll.drop()

    def test_iter_items(self):
        for ds in self.data_stores:
            for spec, i in ds.iteritems():
                assert self.indexed_specs[i] == spec

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
        def func(i):
            return i

        for ds in self.data_stores:
            cached_func = ds.cache()(func)
            computed_values = map(cached_func, xrange(10))
            for i in xrange(10):
                assert cached_func.operation_class(i) in ds

            assert computed_values == map(cached_func, xrange(10))
