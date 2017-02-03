from fito import OperationRunner
from fito.specs.base import Spec, SpecField, _no_default, PrimitiveField, load_object, Field, KwargsField, BaseSpecField


class UnbindedField(object):
    pass


class BaseUnbindedSpec(BaseSpecField, UnbindedField):
    pass


def UnbindedSpecField(pos=None, default=_no_default, base_type=None, spec_field_subclass=None):
    spec_field_subclass = spec_field_subclass or BaseUnbindedSpec
    assert issubclass(spec_field_subclass, BaseUnbindedSpec)
    return SpecField(pos=pos, default=default, base_type=base_type, spec_field_subclass=spec_field_subclass)


class UnbindedPrimitiveField(PrimitiveField, UnbindedField):
    pass


class Operation(Spec):
    out_data_store = SpecField(default=None, serialize=False)
    default_data_store = None

    @classmethod
    def get_fields(cls):
        # cannot call super, because this method is called when the class is being created
        for k in dir(cls):
            v = getattr(cls, k)
            if isinstance(v, Field) and not isinstance(v, UnbindedField):
                yield k, v

    @classmethod
    def get_unbinded_fields(cls):
        for k in dir(cls):
            v = getattr(cls, k)
            if isinstance(v, UnbindedField):
                yield k, v

    def bind(self, *args, **kwargs):
        fields = dict(self.get_unbinded_fields())
        return self.copy().initialize(fields, *args, **kwargs)

    def inplace_bind(self, *args, **kwargs):
        fields = dict(self.get_unbinded_fields())
        return self.initialize(fields, *args, **kwargs)

    def execute(self, runner=None):
        if self.out_data_store is not None:
            out_data_store = self.out_data_store
        else:
            out_data_store = self.default_data_store

        if out_data_store is not None:
            return out_data_store.get_or_execute(self, runner)
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


class A(Operation):
    a = PrimitiveField(0)
    b = UnbindedPrimitiveField(0)

    def apply(self, runner):
        return self.a + self.b
