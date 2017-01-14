import warnings
from StringIO import StringIO
import unittest
from datetime import datetime
from random import Random

import re

from fito import Spec, SpecField, PrimitiveField
from fito.specs.base import NumericField, CollectionField, SpecCollection, InvalidSpecInstance, BaseSpecField, \
    KwargsField, ArgsField
from fito.specs.utils import general_append
from fito.specs import base as specs_base


class SpecA(Spec):
    field1 = NumericField(0)
    field2 = PrimitiveField(1, default=None)
    func = PrimitiveField(default=general_append)

    def __repr__(self):
        return "A(field1={}, field2={})".format(self.field1, self.field2)


class AnotherSpec(Spec):
    l = CollectionField(0)


class SpecB(Spec):
    operation_a = SpecField(base_type=SpecA)

    def __repr__(self):
        return "B(operation_a={})".format(self.operation_a)


class SpecC(Spec):
    op_list = SpecCollection(0)

    def __repr__(self):
        return "C(op_list={})".format(self.op_list)


class SpecD(Spec):
    the_args = ArgsField()
    the_kwargs = KwargsField()


def get_test_specs(only_lists=True, easy=False):
    if easy:
        warnings.warn("get_test_specs(easy=True)")

    instances = [
        SpecA(0),
        SpecA(1, datetime(2017, 1, 1)),
        SpecA(1, func=NumericField),
        SpecB(operation_a=SpecA(0)),
        SpecB(operation_a=SpecA(1)),
        SpecD(),
        SpecD(a=1),
        SpecD(4, a=1),
        SpecD(4),
    ]

    if easy: return instances

    collections = [list] + ([dict] if not only_lists else [])
    rnd = Random(42)
    for i in xrange(4):
        collection = rnd.choice(collections)()
        for i in xrange(4):
            general_append(collection, i, rnd.choice(instances))

        instances.append(SpecC(collection))

    return instances


class TestSpec(unittest.TestCase):
    def setUp(self):
        self.instances = get_test_specs()

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
            load_func = getattr(Spec, 'from_{}'.format(module_name))
            loaded_op = load_func(op_dump)
            assert loaded_op == load_func(op_dumps)
            try: assert loaded_op == op
            except: import ipdb;ipdb.set_trace()

    def test_json_serializable(self):
        self._test_serialization('json')

    def test_yaml_serializable(self):
        self._test_serialization('yaml')

    def test_argspec(self):
        invalid_ops = [
            # SpecA has 1 arguments
            lambda: SpecA(),
            lambda: SpecA(0, 1, 2),
            lambda: SpecA(field1=1, field2=2, field3=3),

            # colleciton field
            lambda: AnotherSpec(1),

            # base_type
            lambda: SpecB(operation_a=AnotherSpec([])),

            # this field does not exist
            lambda: SpecA(param=0),

            # SpecB has 1 Spec arguments
            lambda: SpecB(),
            lambda: SpecB(1),
            lambda: SpecB(a=1),
            lambda: SpecB(operation_a=1),

            # SpecB has 1 Spec arguments
            lambda: SpecC(a=1),
            lambda: SpecC(a=SpecA(1)),
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
                        replace_val = [Spec()]
                        op_dict[field_name] = [replace_val[0].to_dict()]

                replaced_op_dict = op.replace(**{field_name: replace_val}).to_dict()
                assert replaced_op_dict == op_dict

    def test_key(self):
        for op in self.instances:
            assert op == Spec.key2spec(op.key)

    def test_type2spec_class(self):
        assert Spec == Spec.type2spec_class('fito:Spec')
        assert Spec == Spec.type2spec_class('fito.specs.base:Spec')
