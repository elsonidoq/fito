from fito import PrimitiveField
from fito.model.model import Model, ModelParameter
from sklearn.linear_model import LinearRegression as SKLinearRegression


class LinearRegression(Model):
    fit_intercept = ModelParameter(grid=[True, False], default=True)
    normalize = ModelParameter(grid=[True, False], default=False)
    copy_X = PrimitiveField(default=True)
    n_jobs = PrimitiveField(default=)

    def apply(self, runner):
        pass