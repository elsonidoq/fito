import traceback
import warnings
from collections import defaultdict
from itertools import product

from fito import Operation
from fito.specs.base import Spec
from fito.specs.fields import PrimitiveField, _no_default, BaseSpecField

try:
    from sklearn.model_selection import ParameterGrid
except ImportError:
    traceback.print_exc()
    warnings.warn('Could not import ParameterGrid, without it the model selection module can not be used')


class ModelParameter(PrimitiveField):
    def __init__(self, pos=None, default=_no_default, serialize=True, grid=None, *args, **kwargs):
        super(ModelParameter, self).__init__(pos, default, serialize, *args, **kwargs)
        self.grid = grid or ([] if default is _no_default else [default])


class BaseModelField(BaseSpecField):
    def __init__(self, pos=None, default=_no_default, base_type=None, serialize=True, grid=None, *args, **kwargs):
        super(BaseModelField, self).__init__(pos, default, base_type, serialize, *args, **kwargs)
        self.grid = grid or ([] if default is _no_default else [default])


def ModelField(pos=None, default=_no_default, base_type=None, serialize=True, grid=None):
    if base_type is not None:
        assert issubclass(base_type, Model)
    else:
        base_type = Model

    return BaseModelField(pos=pos, default=default, base_type=base_type, serialize=serialize, grid=grid)


class Model(Operation):
    @classmethod
    def get_primitive_param_grid(cls):
        res = {}
        for field_name, field_spec in cls.get_fields():
            if isinstance(field_spec, ModelParameter):
                res[field_name] = field_spec.grid
        return ParameterGrid(res)

    @classmethod
    def get_hyper_parameters_grid(cls):
        submodels_params = defaultdict(list)

        for field_name, field_spec in cls.get_fields():
            if not isinstance(field_spec, BaseModelField): continue

            for impl in field_spec.grid:
                submodels_params[field_name].extend(impl.get_hyper_parameters_grid())

        res = []

        if len(submodels_params) > 0:
            submodel_fields, submodels_params = zip(*submodels_params.iteritems())

        for model_fields_combination in cls.get_primitive_param_grid():

            if len(submodels_params) > 0:
                # If there are submodels, let's combine them
                for params in product(*submodels_params):
                    params = map(Spec.dict2spec, params)
                    model_fields_combination.update(dict(zip(submodel_fields, params)))

                    res.append(
                        cls(**model_fields_combination).to_dict()
                    )
            else:
                res.append(
                    cls(**model_fields_combination).to_dict()
                )

        return res
