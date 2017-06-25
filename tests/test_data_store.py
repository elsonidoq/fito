import inspect
import os
import shutil
import tempfile
import unittest

from fito import Spec
from fito import as_operation
from fito.data_store import file, dict_ds, mongo
from fito.data_store.mongo import get_collection, global_client
from test_operation import get_test_operations
from test_spec import get_test_specs, CustomKey


def delete(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.unlink(path)


def get_test_data_stores():
    file_data_store_preffix = tempfile.mktemp()
    base_mongo_collection = get_collection(global_client, 'test.test')
    base_mongo_collection.drop()

    return [
        mongo.MongoHashMap(base_mongo_collection),
        mongo.MongoHashMap(base_mongo_collection.with_get_cache, get_cache_size=10),
        mongo.MongoHashMap(base_mongo_collection.with_incremental_id, add_incremental_id=True),

        file.FileDataStore(file_data_store_preffix),
        file.FileDataStore(file_data_store_preffix + '_with_get_cache', get_cache_size=10),
        file.FileDataStore(file_data_store_preffix + '_dont_split_keys', split_keys=False),
        file.FileDataStore(file_data_store_preffix + '_use_class_name', use_class_name=True),
        file.FileDataStore(file_data_store_preffix + '_auto_init_fs', auto_init_file_system=True),

        dict_ds.DictDataStore(),
    ]


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

        self.indexed_operations = test_operations[:len(test_operations) / 2]
        self.indexed_specs = test_specs[:len(test_specs) / 2] + self.indexed_operations
        self.not_indexed_specs = test_specs[len(test_specs) / 2:] + test_operations[len(test_operations) / 2:]

        self.cached_functions = []
        for i, ds in enumerate(self.data_stores):
            # Populate the data stores
            for j, spec in enumerate(self.indexed_specs):
                if isinstance(spec, CustomKey) and ds.to_dict().get('store_key'): continue
                ds[spec] = j

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
                if isinstance(spec, CustomKey) and ds.to_dict().get('store_key'): continue

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
            if ds.to_dict().get('store_key'):
                to_compare = [e for e in sorted(self.indexed_specs) if not isinstance(e, CustomKey)]
            else:
                to_compare = sorted(self.indexed_specs)

            assert sorted(ds.iterkeys()) == to_compare

    def test_cache(self):
        orig_func = func

        module = inspect.getmodule(TestDataStore)
        for i, ds in enumerate(self.data_stores):
            OperationClass = as_operation(cache_on=ds)(orig_func)
            setattr(module, 'func', OperationClass)

            for i in xrange(10):
                op = OperationClass(i)
                assert op not in ds
                value = op.execute()
                assert op in ds
                assert op.execute() == value


def func(i):
    return i
