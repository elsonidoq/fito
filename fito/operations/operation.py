from fito import OperationRunner
from fito.specs.base import Spec, SpecField, _no_default, PrimitiveField, load_object, Field, KwargsField

class UnbindedField(Field):
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

    def execute(self, runner=None, **extra):
        if self.out_data_store is not None:
            out_data_store = self.out_data_store
        else:
            out_data_store = self.default_data_store

        if out_data_store is not None:
            return out_data_store.get_or_execute(self, runner, **extra)
        else:
            return (runner or OperationRunner()).execute(self, **extra)

    def apply(self, runner, context):
        raise NotImplementedError()

    def build_context(self, **extra):
        return Context.build_from_extra(self, **extra)


class MemoryObject(Operation):
    obj = PrimitiveField(0)

    def apply(self, runner, **extra):
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
    b = UnbindedField()

    def apply(self, runner, context):
        return context.b


class Context(Spec):
    target_operation = OperationField(0)
    extra = KwargsField()

    def set(self, attr, val, type):
        setattr(self, attr, val)
        self.specs[attr] = type

    @classmethod
    def build_from_extra(cls, target_operation, **extra):
        new_extra = {}
        for field, field_type in target_operation.get_unbinded_fields():
            if field in extra:
                new_extra[field] = extra[field]
            elif field_type.has_default_value():
                new_extra[field] = field_type.default
            else:
                raise RuntimeError(
                    'Missing unbinded field "{}" for class {}'.format(field, type(self).__name__)
                )
        return cls(target_operation, **new_extra)

    def __getattribute__(self, item):
        extra = super(Context, self).__getattribute__('extra')
        if not isinstance(extra, KwargsField) and item in extra:
            return extra[item]
        else:
            return super(Context, self).__getattribute__(item)
