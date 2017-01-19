import ctypes

from fito import OperationRunner
from fito.specs.base import Spec, SpecField, _no_default


class Operation(Spec):
    out_data_store = SpecField(default=None)

    def execute(self, runner=None):
        if self.out_data_store is not None:
            return self.out_data_store.get_or_execute(self, runner)
        else:
            return (runner or OperationRunner()).execute(self)

    def apply(self, runner):
        raise NotImplementedError()


class MemoryObject(Operation):
    def __init__(self, *args, **kwargs):
        super(MemoryObject, self).__init__(*args, **kwargs)
        fields = self.get_primitive_fields()

        if len(fields) != 1:
            raise RuntimeError("Memory object subclasses must have exactly one primitive field to refer the object")

        self.object_field_name = fields.iterkeys().next()

    @property
    def object(self):
        return getattr(self, self.object_field_name)

    def apply(self, runner):
        return self.object

    def to_dict(self, include_toggle_fields=False):
        res = super(MemoryObject, self).to_dict(include_toggle_fields=include_toggle_fields)
        res[self.object_field_name] = id(self.object)
        return res

    @classmethod
    def _from_dict(cls, kwargs):
        res = super(MemoryObject, cls)._from_dict(kwargs)

        setattr(
            res,
            res.object_field_name,
            ctypes.cast(
                getattr(res, res.object_field_name),
                ctypes.py_object
            ).value
        )
        return res


def OperationField(pos=None, default=_no_default, base_type=None):
    return SpecField(pos=pos, default=default, base_type=base_type or Operation)
