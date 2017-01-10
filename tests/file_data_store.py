import tempfile
import unittest

from fito.data_store.file import FileDataStore, RawSerializer
from data_store import delete
from spec import get_test_specs


class TestFileDataStore(unittest.TestCase):
    def setUp(self):
        self.file_data_store_path = tempfile.mktemp()

        self.ds = FileDataStore(self.file_data_store_path, serializer=RawSerializer())

        self.test_specs = get_test_specs(only_lists=True)

    def tearDown(self):
        delete(self.file_data_store_path)

    def test_existing_path(self):
        assert self.ds.serializer == FileDataStore(self.file_data_store_path).serializer
        self.assertRaises(RuntimeError, FileDataStore, self.file_data_store_path, serializer=RawSerializer)

    def test_clean(self):
        self.ds[self.test_specs[0]] = ""

        try:
            self.ds.iterkeys().next()
        except:
            assert False

        self.ds.clean()

        self.assertRaises(StopIteration, self.ds.iterkeys().next)





