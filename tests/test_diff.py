from random import Random
import unittest
from fito import as_operation, Spec, SpecField, PrimitiveField
from fito.specs.diff import Diff
from test_spec import get_test_specs, SpecB, SpecWithDefault
from test_data_store import get_test_data_stores


@as_operation(a=Spec)
def func(a):
    return a


class A(Spec):
    t = SpecField(0, default=None)


class H(Spec):
    b = PrimitiveField(0)


class TestDiff(unittest.TestCase):
    def test_create_refactor(self):
        specs = get_test_specs(easy=True) + get_test_data_stores()
        specs = [
            e for e in specs
            if not isinstance(e, SpecB)
            and not isinstance(e, SpecWithDefault)
        ]
        specs.append(func(Spec()))
        specs.append(func(A()))
        specs.append(func(A(H(0))))
        specs.append(func(A(H(1))))

        for s1 in specs:
            for s2 in specs:
                s1_dict = s1.to_dict()
                s2_dict = s2.to_dict()
                diff = Diff.build(s1_dict, s2_dict)
                r = diff.create_refactor()

                refactored_dict = r.bind(s1_dict).execute()
                assert refactored_dict == s2_dict
