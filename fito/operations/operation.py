from fito import OperationRunner
from fito.specs.base import Spec, load_object
from fito.specs.fields import SpecField, _no_default, PrimitiveField


class Operation(Spec):
    out_data_store = SpecField(default=None, serialize=False)
    default_data_store = None

    def execute(self):
        return OperationRunner().execute(self)

    def apply(self, runner):
        raise NotImplementedError()

    def get_out_data_store(self):
        if self.out_data_store is not None:
            return self.out_data_store
        else:
            return type(self).default_data_store


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


