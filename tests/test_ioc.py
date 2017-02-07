from tempfile import mktemp
import unittest
import yaml

from fito import Spec
from fito import SpecField
from fito import ioc
from fito.ioc import ApplicationContext
from test_spec import SpecA
from test_spec import SpecC


class OtherSpec(Spec):
    spec = SpecField()


a_contexts = [
    """
some:
    type: test_spec:SpecA
    field1: 1
""",
    """
some:
    type: test_spec:SpecA
    field1: 2
"""
]

b_contexts = [
    """
thing:
    type: test_spec:SpecC
    spec_list:
        - $some
""",

    """
thing:
    type: test_spec:SpecC
    spec_list:
        - $some
        - $some
"""
]

c_contexts = [
    """
nice:
    type: test_ioc:OtherSpec
    spec: $some
""",
    """
nice:
    type: test_ioc:OtherSpec
    spec: $thing
"""
]


class TestIOC(unittest.TestCase):
    def setUp(self):
        self.combinations = []

        for i, a in enumerate(a_contexts):
            for j, b in enumerate(b_contexts):
                for k, c in enumerate(c_contexts):
                    self.combinations.append(
                        {
                            'meta': (i, j, k),
                            'contexts': (a, b, c)
                        }
                    )

    def test_ioc(self):
        def some_field(ctx):
            return ctx.get('some').field1

        def thing_len(ctx):
            return len(ctx.get('thing').spec_list)

        def nice_type(ctx):
            return type(ctx.get('nice').spec)

        answers = {
            some_field: {'id': 0, 'ans': [1, 2]},
            thing_len: {'id': 1, 'ans': [1, 2]},
            nice_type: {'id': 2, 'ans': [SpecA, SpecC]}
        }

        for doc in self.combinations:

            fnames = []
            for c in doc['contexts']:
                fname = mktemp()
                fnames.append(fname)
                with open(fname, 'w') as f:
                    f.write(c)

            general_fname = mktemp()
            with open(general_fname, 'w') as f:
                yaml.dump(
                    {
                        'import': fnames,
                        'some': 1,  # this is to test that the value is actually overridden
                        'other_value': 1,  # this is to test that the value is not overridden
                    },
                    f
                )

            ctx = ApplicationContext.load(general_fname)
            assert ctx.get('other_value') == 1

            for func, ans in answers.iteritems():
                func_answer = ans['ans'][doc['meta'][ans['id']]]

                if func(ctx) != func_answer:
                    print '\n'.join(doc['contexts'])
                    print 'func:', func
                    print 'actual answer:', func()
                    print 'expected answer:', func_answer
                    assert False
