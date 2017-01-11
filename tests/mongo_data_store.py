import tempfile
import unittest

from fito.data_store.file import FileDataStore, RawSerializer, PickleSerializer
from data_store import delete
from fito.data_store.mongo import MongoHashMap
from spec import get_test_specs


class TestMongoDataStore(unittest.TestCase):
    def setUp(self):
        self.data_stores = [
            MongoHashMap('test.with_grid_fs', use_gridfs=True),
            MongoHashMap('test.test')
        ]
        for ds in self.data_stores:
            ds.coll.drop()

        self.test_specs = get_test_specs(only_lists=True)

        for ds in self.data_stores:
            for spec in self.test_specs:
                ds[spec] = "asdf"


    def test_delete(self):
        for ds in self.data_stores:
            for spec in ds.iterkeys():
                ds.delete(spec)

            assert len(ds) == 0

    def test_choice(self):
        for ds in self.data_stores:
            ds.choice()
