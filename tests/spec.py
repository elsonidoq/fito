import os
from tempfile import mktemp, mkdtemp
import warnings
from StringIO import StringIO
import unittest
from datetime import datetime
from random import Random

import re
import shutil
import yaml

from fito import Spec, SpecField, PrimitiveField
from fito.specs.base import NumericField, CollectionField, SpecCollection, InvalidSpecInstance, BaseSpecField, \
    KwargsField, ArgsField
from fito.specs.utils import general_append
from fito.specs import base as specs_base


class SpecA(Spec):
    field1 = NumericField(0)
    field2 = PrimitiveField(1, default=None)
    func = PrimitiveField(default=general_append)
    verbose = PrimitiveField(default=False, serialize=False)

    def __repr__(self):
        return "A(field1={}, field2={})".format(self.field1, self.field2)


class AnotherSpec(Spec):
    l = CollectionField(0)


class SpecB(Spec):
    spec_a = SpecField(base_type=SpecA)

    def __repr__(self):
        return "B(spec_a={})".format(self.spec_a)


class SpecC(Spec):
    spec_list = SpecCollection(0)

    def __repr__(self):
        return "C(spec_list={})".format(self.spec_list)


class SpecD(Spec):
    the_args = ArgsField()
    the_kwargs = KwargsField()


class SpecWithDefault(Spec):
    a = SpecField(default=SpecA(10))


def get_test_specs(only_lists=True, easy=False):
    if easy:
        warnings.warn("get_test_specs(easy=True)")

    instances = [
        SpecA(0),
        SpecA(10, verbose=True),
        SpecA(1, datetime(2017, 1, 1)),
        SpecA(1, func=NumericField),
        SpecB(spec_a=SpecA(0)),
        SpecB(spec_a=SpecA(1)),
        SpecD(),
        SpecD(a=1),
        SpecD(4, a=1),
        SpecD(4),
        SpecWithDefault(),
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
        for spec in self.instances:
            try:
                # Get the dumps serialization
                spec_dumps = getattr(spec, module_name).dumps()

                f = StringIO()
                getattr(spec, module_name).dump(f)
                # Get the dump serialization, they should be equivalent
                spec_dump = f.getvalue()
            except TypeError, e:
                if len(e.args) == 0: raise e

                message = e.args[0]
                if not isinstance(message, basestring): raise e

                is_datetime_error = re.match('^datetime*? is not JSON is not JSON serializable$', message) is not None

                assert is_datetime_error == (not hasattr(specs_base, 'json_util'))
                continue

            # Hack: TODO do this better
            load_func = getattr(Spec, 'from_{}'.format(module_name))
            importer = load_func()
            loaded_op = importer.loads(spec_dump)
            assert loaded_op == importer.loads(spec_dumps)
            assert loaded_op == spec

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
            lambda: SpecB(spec_a=AnotherSpec([])),

            # this field does not exist
            lambda: SpecA(param=0),

            # SpecB has 1 Spec arguments
            lambda: SpecB(),
            lambda: SpecB(1),
            lambda: SpecB(a=1),
            lambda: SpecB(spec_a=1),

            # SpecB has 1 Spec arguments
            lambda: SpecC(a=1),
            lambda: SpecC(a=SpecA(1)),
        ]

        for i, invalid_op in enumerate(invalid_ops):
            self.assertRaises(InvalidSpecInstance, invalid_op)

    def test_key_caching(self):
        # Shouldn't have a key
        spec = SpecA(1)
        assert not hasattr(spec, '_key')

        # Executed key, should now
        _ = spec.key
        assert hasattr(spec, '_key')

        # Changed the object, the cache shouldn't be there
        spec.field1 = 10
        assert not hasattr(spec, '_key')

    def test_hasheable(self):
        d = {}
        for i, spec in enumerate(self.instances):
            d[spec] = i

        for i, spec in enumerate(self.instances):
            assert d[spec] == i
            assert d[spec.dict2spec(spec.to_dict())] == i

    def test_copy(self):
        for spec in self.instances:
            assert spec.to_dict() == spec.copy().to_dict()

    def test_replace(self):
        for spec in self.instances:
            for field_name, field_spec in spec.get_fields():
                spec_dict = spec.to_dict(include_all=True)

                if isinstance(field_spec, PrimitiveField):
                    spec_dict[field_name] = replace_val = 1

                else:

                    self.assertRaises(InvalidSpecInstance, spec.replace, **{field_name: 1})

                    if isinstance(field_spec, BaseSpecField):
                        replace_val = [e for e in self.instances if field_spec.check_valid_value(e)][0]
                        spec_dict[field_name] = replace_val.to_dict(include_all=True)
                    else:
                        replace_val = [Spec()]
                        spec_dict[field_name] = [replace_val[0].to_dict(include_all=True)]

                replaced_spec_dict = spec.replace(**{field_name: replace_val}).to_dict(include_all=True)
                assert replaced_spec_dict == spec_dict

    def test_key(self):
        for spec in self.instances:
            assert spec.to_dict() == Spec.key2spec(spec.key).to_dict()

    def test_type2spec_class(self):
        assert Spec == Spec.type2spec_class('fito:Spec')
        assert Spec == Spec.type2spec_class('fito.specs.base:Spec')

    def test_serialize(self):
        s = SpecA(0, verbose=True)
        assert 'verbose' not in s.to_dict()
        assert 'verbose' in s.to_dict(include_all=True)
        assert Spec.dict2spec(s.to_dict()) == s
        assert Spec.dict2spec(s.to_dict(include_all=True)) == s

    def test_empty_load(self):
        assert SpecWithDefault() == Spec.dict2spec({'type': 'SpecWithDefault'})

    def test_reference(self):
        for spec in self.instances:
            for use_relative_paths in True, False:
                try:
                    tmp_dir = mkdtemp()
                    fnames = splitted_serialize(spec, tmp_dir, use_relative_paths=use_relative_paths)
                    assert spec == Spec.from_yaml().load(fnames[spec])
                finally:
                    shutil.rmtree(tmp_dir)


def splitted_serialize(spec, dir, use_relative_paths):
    fnames = {}
    spec_fields = spec.get_spec_fields()
    for attr, subspec in spec_fields.iteritems():
        fnames.update(splitted_serialize(subspec, dir, use_relative_paths))

    spec_fname = fnames[spec] = mktemp(suffix='_spec.yaml', dir=dir)
    d = spec.to_dict(include_all=True)
    for k, v in d.iteritems():
        if k in spec_fields:
            d[k] = fnames[getattr(spec, k)]
            if use_relative_paths: d[k] = os.path.basename(d[k])

    with open(spec_fname, 'w') as f:
        yaml.dump(d, f, default_flow_style=False)

    return fnames
