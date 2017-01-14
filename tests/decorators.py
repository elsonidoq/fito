from fito import DictDataStore
from fito import Operation
from fito import SpecField
from fito import as_operation
from psycopg2.tests import unittest
from pymongo import MongoClient

ds = DictDataStore()


class BaseOp(Operation): pass


@as_operation(out_type=BaseOp)
def get_collection(db, name):
    client = MongoClient()
    return client[db][name]


@as_operation(collection=SpecField(base_type=get_collection), out_type=BaseOp)
def get_data(collection, query):
    return list(collection.find(query))


@as_operation(data=SpecField, out_type=BaseOp)
def filter_odd(data):
    return [e for e in data if e['id'] % 2 == 0]


@as_operation(data=SpecField, out_type=BaseOp)
def sum_all(data):
    return sum([e['id'] for e in data])


def push_data():
    collection = get_collection('test', 'test').execute()
    collection.drop()

    for i in xrange(10):
        collection.insert_one(
            {
                'id': i,
                'data': [10] * 10
            }
        )


class TestChainedOperations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        push_data()

    def setUp(self):
        self.operations = [
            (
                sum_all(
                    get_data(
                        get_collection('test', 'test'), {'id': {'$lte': 5}}
                    )
                ),
                sum(range(6))
            ),
            (
                sum_all(
                    filter_odd(
                        get_data(
                            get_collection('test', 'test'), {'id': {'$lte': 5}}
                        )
                    )
                ),
                sum(range(0, 6, 2))
            )

        ]

    def test_run(self):
        for op, expected_value in self.operations:
            assert op.execute() == expected_value

    def test_cache(self):
        ds.data = {}
        BaseOp.out_data_store = ds

        self.test_run()
        BaseOp.out_data_store = None

        for op, val in ds.iteritems():
            assert op.execute() == val
