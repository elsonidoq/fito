from fito import PrimitiveField
from fito.model.model import Model, ModelParameter
from sklearn.linear_model import LinearRegression as SKLinearRegression
from sklearn.linear_model import LogisticRegression as SKLogisticRegression
from sklearn.ensemble import GradientBoostingClassifier as SKGradientBoostingClassifier


class SKLearnModel(Model):
    def instance_model(self, constructor):
        return constructor(**self.to_kwargs())


class LinearRegression(SKLearnModel):
    fit_intercept = ModelParameter(default=True, grid=[True, False])
    normalize = ModelParameter(default=False, grid=[True, False])
    copy_X = PrimitiveField(default=True)
    n_jobs = PrimitiveField(default=1)

    def apply(self, runner):
        return self.instance_model(SKLinearRegression)


class LogisticRegression(SKLearnModel):
    penalty = ModelParameter(default='l2', grid=['l1', 'l2'])
    dual = ModelParameter(default=False, grid=[True, False])
    tol = PrimitiveField(default=-4)
    C = ModelParameter(default=1.0, grid=[0.01, 0.1, 1.0])
    fit_intercept = ModelParameter(default=True, grid=[True, False])
    intercept_scaling = ModelParameter(default=1, grid=[0.1, 1, 10])
    class_weight = ModelParameter(default=None, grid=[None, 'auto'])
    random_state = PrimitiveField(default=None)
    solver = PrimitiveField(default='liblinear')
    max_iter = PrimitiveField(default=100)
    multi_class = ModelParameter(default='ovr', grid=['ovr', 'multinomial'])
    verbose = PrimitiveField(default=0)
    warm_start = PrimitiveField(default=False)
    n_jobs = PrimitiveField(default=1)

    def apply(self, runner):
        return self.instance_model(SKLogisticRegression)


class GradientBoostingClassifier(SKLearnModel):
    loss = ModelParameter(default='deviance', grid=['deviance', 'exponential'])
    learning_rate = ModelParameter(default=0.1, grid=[0.01, 0.1, 0.3])
    n_estimators = ModelParameter(default=100, grid=[30, 100, 300])
    subsample = ModelParameter(default=1.0, grid=[0.3, 1.0, 3.0])
    min_samples_split = ModelParameter(default=2, grid=[2, 10])
    min_samples_leaf = ModelParameter(default=1, grid=[1, 10])
    min_weight_fraction_leaf = ModelParameter(default=0., grid=[0., 0.01])
    max_depth = ModelParameter(default=3, grid=[3, 4])
    init = PrimitiveField(default=None)
    random_state = PrimitiveField(default=None)
    max_features = ModelParameter(default=None, grid=['auto', 'sqrt', 'log2', None])
    verbose = PrimitiveField(default=0)
    max_leaf_nodes = ModelParameter(default=None, grid=[None])
    warm_start = PrimitiveField(default=False)
    presort = PrimitiveField(default='auto')

    def apply(self, runner):
        return self.instance_model(SKGradientBoostingClassifier)
