import os
import shutil
import tempfile
import unittest

from fito.data_store import file, dict_ds, mongo
from fito.data_store.mongo import get_collection, global_client
from spec import get_test_specs


def delete(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.unlink(path)


class TestDataStore(unittest.TestCase):
    def setUp(self):
        self.file_data_store_path = tempfile.mktemp()
        self.mongo_collection = get_collection(global_client, 'test.test')
        self.mongo_collection.drop()

        self.data_stores = [
            mongo.MongoHashMap(self.mongo_collection),
            mongo.MongoHashMap(self.mongo_collection.with_get_cache, get_cache_size=10),
            file.FileDataStore(self.file_data_store_path),
            file.FileDataStore(self.file_data_store_path + '_with_get_cache', get_cache_size=10),
            file.FileDataStore(self.file_data_store_path + '_dont_split_keys', split_keys=False),
            dict_ds.DictDataStore(),
        ]

        # This is just because of MongoHashMap
        test_specs = get_test_specs(only_lists=True)
        self.indexed_specs = test_specs[:len(test_specs)/2]
        self.not_indexed_specs = test_specs[len(test_specs)/2:]

        for ds in self.data_stores:
            for i, spec in enumerate(self.indexed_specs):
                ds[spec] = i

    def tearDown(self):
        delete(self.file_data_store_path)
        delete(self.file_data_store_path + '_with_get_cache')
        delete(self.file_data_store_path + '_dont_split_keys')

        self.mongo_collection.drop()
        self.mongo_collection.with_get_cache.drop()

    def test_iter_items(self):
        for ds in self.data_stores:
            for spec, i in ds.iteritems():
                assert self.indexed_specs[i] == spec

    def test_get(self):
        for ds in self.data_stores:
            for i, spec in enumerate(self.indexed_specs):
                assert ds[spec] == i
                assert ds.get(spec) == i

            for spec in self.not_indexed_specs:
                self.assertRaises(KeyError, ds.get, spec)

    def test_keys(self):
        for ds in self.data_stores:
            assert sorted(ds.iterkeys()) == sorted(self.indexed_specs)

