import unittest

from fito import DictDataStore
from fito import Spec
from fito import SpecField
from fito import ioc
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
        def some_field(): return ioc.ctx.get('some').field1
        def thing_len(): return len(ioc.ctx.get('thing').spec_list)
        def nice_type(): return type(ioc.ctx.get('nice').spec)

        answers = {
            some_field: {'id': 0, 'ans': [1, 2]},
            thing_len: {'id': 1, 'ans': [1, 2]},
            nice_type: {'id': 2, 'ans': [SpecA, SpecC]}
        }

        for doc in self.combinations:

            reset_ctx()
            ioc.ctx.load_from_strings(*doc['contexts'])

            for func, ans in answers.iteritems():
                func_answer = ans['ans'][doc['meta'][ans['id']]]

                if func() != func_answer:
                    print '\n'.join(doc['contexts'])
                    print 'func:', func
                    print 'actual answer:',  func()
                    print 'expected answer:',  func_answer
                    assert False


def reset_ctx():
    ioc.ctx.objects = None
    ioc.ctx.get.operation_class.default_data_store.clean()



