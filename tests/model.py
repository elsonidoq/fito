import unittest

from fito.model import Model, ModelParameter, ModelField


class Stage1(Model): pass


class Stage1Impl1(Stage1):
    a = ModelParameter(grid=[1, 2])


class Stage1Impl2(Stage1):
    b = ModelParameter(grid=[1, 2])
    c = ModelParameter(grid=[2, 3])


class Stage2(Model): pass


class Stage2Impl1(Stage2):
    d = ModelParameter(grid=[6, 5])


class Pepe(Model):
    stage1 = ModelField(base_type=Stage1)
    stage2 = ModelField(base_type=Stage2)


class TestModel(unittest.TestCase):
    def test_hyper_parameters(self):
        stage1 = []
        for a in Stage1Impl1.a.grid:
            stage1.append(Stage1Impl1(a=a))

        for b in Stage1Impl2.b.grid:
            for c in Stage1Impl2.c.grid:
                stage1.append(Stage1Impl2(b=b, c=c))

        stage2 = []
        for d in Stage2Impl1.d.grid:
            stage2.append(Stage2Impl1(d=d))

        expected_result = []
        for s1 in stage1:
            for s2 in stage2:
                expected_result.append(Pepe(stage1=s1, stage2=s2).to_dict())

        assert sorted(Pepe.get_hyper_parameters_grid()) == sorted(expected_result)

