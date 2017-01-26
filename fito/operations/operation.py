import ctypes

from fito import OperationRunner
from fito.specs.base import Spec, SpecField, _no_default, PrimitiveField, load_object


class Operation(Spec):
    out_data_store = SpecField(default=None, serialize=False)

    def execute(self, runner=None):
        if self.out_data_store is not None:
            return self.out_data_store.get_or_execute(self, runner)
        else:
            return (runner or OperationRunner()).execute(self)

    def apply(self, runner):
        raise NotImplementedError()


class MemoryObject(Operation):
    obj = PrimitiveField(0)

    def apply(self, runner):
        return self.obj

    def to_dict(self, include_all=False):
        res = super(MemoryObject, self).to_dict(include_all=include_all)
        res['obj'] = id(self.obj)
        return res

    @classmethod
    def _from_dict(cls, kwargs, path=None):
        res = super(MemoryObject, cls)._from_dict(kwargs, path=path)
        res.obj = load_object(res.obj)
        return res


def OperationField(pos=None, default=_no_default, base_type=None):
    return SpecField(pos=pos, default=default, base_type=base_type or Operation)
