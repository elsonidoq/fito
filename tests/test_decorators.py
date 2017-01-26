import unittest
from fito import DictDataStore
from fito import Operation
from fito import OperationField
from fito import as_operation
from pymongo import MongoClient


class BaseOp(Operation): pass


# get_collection('test', 'test').execute() returns the corresponding collection
# get_collection('test', 'test') is an instance of BaseOp
# get_collection is a subclass of BaseOp
@as_operation(out_type=BaseOp)
def get_collection(db, name):
    client = MongoClient()
    return client[db][name]


# collection must be the output from a subclass of get_collection
# Telling fito that collection is an Operation allows him to automatically execute it before calling this function
# So we can assume that collection is a already pymongo.Collection
@as_operation(collection=OperationField(base_type=get_collection), out_type=BaseOp)
def get_data(collection, query):
    return list(collection.find(query))


# data must be an Operation, but I'm not specifying from which base type, it could be any
@as_operation(data=OperationField, out_type=BaseOp)
def filter_odd(data):
    return [e for e in data if e['id'] % 2 == 0]


@as_operation(data=OperationField, out_type=BaseOp)
def sum_all(data):
    self = as_operation.get_current_operation()
    factor = 1 + isinstance(self.data, filter_odd)
    return factor * sum([e['id'] for e in data])


# Pushes some data into the mongo collection
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
                2*sum(range(0, 6, 2))
            )

        ]

    def test_out_type(self):
        for op_class in sum_all, filter_odd, get_data, get_collection:
            assert issubclass(op_class, BaseOp)

    def test_run(self):
        for op, expected_value in self.operations:
            assert op.execute() == expected_value

    def test_cache(self):
        # Now BaseOp sublcasses auto save their output on ds
        ds = DictDataStore()
        BaseOp.out_data_store = ds

        # Execute all and autosave it to ds
        self.test_run()
        # Disable autosaving
        BaseOp.out_data_store = None

        # Check that the saved values correspond to the output of execute
        for op, val in ds.iteritems():
            assert op.execute() == val
