from StringIO import StringIO
import unittest
from datetime import datetime
from random import Random

import re

from fito import Operation, SpecField, PrimitiveField, as_operation
from fito.specs.base import NumericField, CollectionField, SpecCollection, InvalidSpecInstance, BaseSpecField, Spec
from fito.specs.utils import general_append
from fito.specs import base as specs_base


class SpecA(Operation):
    field1 = NumericField(0)
    field2 = PrimitiveField(1, default=None)

    def __repr__(self):
        return "A(field1={}, field2={})".format(self.field1, self.field2)


class AnotherSpec(Operation):
    l = CollectionField(0)


class OperationB(Operation):
    operation_a = SpecField(base_type=SpecA)

    def __repr__(self):
        return "B(operation_a={})".format(self.operation_a)


class OperationC(Operation):
    op_list = SpecCollection(0)

    def __repr__(self):
        return "C(op_list={})".format(self.op_list)


class TestOperation(unittest.TestCase):
    def setUp(self):
        self.instances = [
            SpecA(0),
            SpecA(1, datetime(2017, 1, 1)),
            OperationB(operation_a=SpecA(0)),
            OperationB(operation_a=SpecA(1)),
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

                assert is_datetime_error == (not hasattr(specs_base, 'json_util'))
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
            # SpecA has 1 arguments
            lambda: SpecA(),
            lambda: SpecA(0, 1, 2),
            lambda: SpecA(field1=1, field2=2, field3=3),

            # colleciton field
            lambda: AnotherSpec(1),

            # base_type
            lambda: OperationB(operation_a=AnotherSpec([])),

            # this field does not exist
            lambda: SpecA(param=0),

            # OperationB has 1 Operation arguments
            lambda: OperationB(),
            lambda: OperationB(1),
            lambda: OperationB(a=1),
            lambda: OperationB(operation_a=1),

            # OperationB has 1 OperationCollection arguments
            lambda: OperationC(a=1),
            lambda: OperationC(a=SpecA(1)),
        ]

        for i, invalid_op in enumerate(invalid_ops):
            self.assertRaises(InvalidSpecInstance, invalid_op)

    def test_key_caching(self):
        # Shouldn't have a key
        op = SpecA(1)
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
            assert d[op.dict2spec(op.to_dict())] == i

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
                    self.assertRaises(InvalidSpecInstance, op.replace, **{field_name: 1})
                    if isinstance(field_spec, BaseSpecField):
                        replace_val = [e for e in self.instances if field_spec.check_valid_value(e)][0]
                        op_dict[field_name] = replace_val.to_dict()
                    else:
                        replace_val = [Operation()]
                        op_dict[field_name] = [replace_val[0].to_dict()]

                replaced_op_dict = op.replace(**{field_name: replace_val}).to_dict()
                assert replaced_op_dict == op_dict

    def test_key(self):
        for op in self.instances:
            assert op == Operation.key2spec(op.key)

    def test_type2spec_class(self):
        assert Spec == Spec.type2spec_class('fito:Spec')
        assert Spec == Spec.type2spec_class('fito.specs.base:Spec')

