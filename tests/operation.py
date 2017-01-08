from StringIO import StringIO
import unittest
from datetime import datetime
from random import Random

import re

from fito import Operation
from fito import OperationField
from fito import PrimitiveField
from fito import as_operation
from fito.operations.base import OperationCollection, InvalidOperationInstance
from fito.operations.utils import general_append
from fito.operations import base as operations_base


class OperationA(Operation):
    field1 = PrimitiveField(0)
    field2 = PrimitiveField(1, default=None)

    def __repr__(self):
        return "A(field1={}, field2={})".format(self.field1, self.field2)


class OperationB(Operation):
    operation_a = OperationField()

    def __repr__(self):
        return "B(operation_a={})".format(self.operation_a)


class OperationC(Operation):
    op_list = OperationCollection(0)

    def __repr__(self):
        return "C(op_list={})".format(self.op_list)


class TestOperation(unittest.TestCase):
    def setUp(self):
        self.instances = [
            OperationA(0),
            OperationA(datetime(2017, 1, 1), True),
            OperationB(operation_a=OperationA(0)),
            OperationB(operation_a=OperationA(1)),
        ]

        rnd = Random(42)
        for i in xrange(4):
            collection = rnd.choice([list, dict])()
            for i in xrange(4):
                general_append(collection, i, rnd.choice(self.instances))

            self.instances.append(OperationC(collection))

    def _test_serialization(self, module_name):
        """
        :param module_name: either "json" or "yaml"
        """
        for op in self.instances:
            try:
                # Get the dumps serialization
                op_dumps = getattr(op, module_name).dumps()

                f = StringIO()
                getattr(op, module_name).dump(f)
                # Get the dump serialization, they should be equivalent
                op_dump = f.getvalue()
            except TypeError, e:
                if len(e.args) == 0: raise e

                message = e.args[0]
                if not isinstance(message, basestring): raise e

                is_datetime_error = re.match('^datetime*? is not JSON is not JSON serializable$', message) is not None

                assert is_datetime_error == (not hasattr(operations_base, 'json_util'))
                continue

            # Hack: TODO do this better
            load_func = getattr(Operation, 'from_{}'.format(module_name))
            loaded_op = load_func(op_dump)
            assert loaded_op == load_func(op_dumps)
            assert loaded_op == op

    def test_json_serializable(self):
        self._test_serialization('json')

    def test_yaml_serializable(self):
        self._test_serialization('yaml')

    def test_operation_argspec(self):
        invalid_ops = [
            # OperationA has 1 arguments
            lambda: OperationA(),
            lambda: OperationA(0, 1, 2),
            lambda: OperationA(field1=1, field2=2, field3=3),

            # this field does not exist
            lambda: OperationA(param=0),

            # OperationB has 1 Operation arguments
            lambda: OperationB(),
            lambda: OperationB(1),
            lambda: OperationB(a=1),
            lambda: OperationB(operation_a=1),

            # OperationB has 1 OperationCollection arguments
            lambda: OperationC(a=1),
            lambda: OperationC(a=OperationA(1)),
        ]

        for i, invalid_op in enumerate(invalid_ops):
            self.assertRaises(InvalidOperationInstance, invalid_op)

    def test_key_caching(self):
        # Shouldn't have a key
        op = OperationA(1)
        assert not hasattr(op, '_key')

        # Executed key, should now
        _ = op.key
        assert hasattr(op, '_key')

        # Changed the object, the cache shouldn't be there
        op.field1 = 10
        assert not hasattr(op, '_key')

    def test_hasheable(self):
        d = {}
        for i, op in enumerate(self.instances):
            d[op] = i

        for i, op in enumerate(self.instances):
            assert d[op] == i
            assert d[op.dict2operation(op.to_dict())] == i

    def test_copy(self):
        for op in self.instances:
            assert op.to_dict() == op.copy().to_dict()

    def test_replace(self):
        for op in self.instances:
            for field_name, field_spec in op.get_fields():
                op_dict = op.to_dict()
                if isinstance(field_spec, PrimitiveField):
                    op_dict[field_name] = replace_val = 1
                else:
                    self.assertRaises(RuntimeError, op.replace, **{field_name: 1})
                    if isinstance(field_spec, OperationField):
                        replace_val = Operation()
                        op_dict[field_name] = replace_val.to_dict()
                    else:
                        replace_val = [Operation()]
                        op_dict[field_name] = [replace_val[0].to_dict()]

                replaced_op_dict = op.replace(**{field_name: replace_val}).to_dict()
                assert replaced_op_dict == op_dict

    def test_key(self):
        for op in self.instances:
            assert op == Operation.key2operation(op.key)

    def test_as_operation(self):
        @as_operation(op=OperationField)
        def f(op, val=1): return val

        class Test(object):
            @as_operation(method_type='instance', op=OperationField)
            def op1(self, op, val=1): return 1

            @as_operation(method_type='class', op=OperationField)
            def op2(cls, op): return 1

        ops = [f, Test().op1, Test.op2]
        for op in ops:
            assert issubclass(op, Operation)
            self.assertRaises(InvalidOperationInstance, op, 1)
            op(self.instances[0])._apply(None)



