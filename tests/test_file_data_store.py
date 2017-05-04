import tempfile
import unittest

from fito.data_store.file import FileDataStore, RawSerializer, PickleSerializer
from test_data_store import delete
from test_spec import get_test_specs


class TestFileDataStore(unittest.TestCase):
    def setUp(self):
        self.data_stores = [
            FileDataStore(tempfile.mktemp(), serializer=RawSerializer()),
            FileDataStore(tempfile.mktemp(), use_class_name=True),
            FileDataStore(tempfile.mktemp(), serializer=PickleSerializer()),
            FileDataStore(tempfile.mktemp(), serializer=RawSerializer(), auto_init_file_system=True),
            FileDataStore(tempfile.mktemp(), use_class_name=True, auto_init_file_system=True),
            FileDataStore(tempfile.mktemp(), serializer=PickleSerializer(), auto_init_file_system=True),
            FileDataStore(tempfile.mktemp(), auto_init_file_system=True)
        ]

        self.test_specs = get_test_specs(only_lists=True)

    def tearDown(self):
        for ds in self.data_stores:
            delete(ds.path)

    def test_existing_path(self):
        for ds in self.data_stores:
            ds.init_file_system()

            self.assertRaises(
                RuntimeError,
                FileDataStore,
                ds.path,
                use_class_name=not ds.use_class_name,
                auto_init_file_system=True
            )
            other_serializer = PickleSerializer() if isinstance(ds.serializer, RawSerializer) else RawSerializer()
            self.assertRaises(
                RuntimeError,
                FileDataStore,
                ds.path,
                serializer=other_serializer,
                auto_init_file_system=True
            )

    def test_clean(self):
        for ds in self.data_stores:
            ds[self.test_specs[0]] = ""

            try:
                ds.iterkeys().next()
            except:
                assert False

            ds.clean()

            self.assertRaises(StopIteration, ds.iterkeys().next)





