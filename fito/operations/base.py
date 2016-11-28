import json
from collections import OrderedDict

from memoized_property import memoized_property


class fifo_apply(object):
    def __init__(self, size=500):
        self.queue = OrderedDict()
        self.size = size

    def __call__(self, old_apply):
        def new_f(the_self, data_store):
            if the_self in self.queue:
                return self.queue[the_self]
            else:
                res = old_apply(the_self, data_store)

                if not isinstance(the_self, GetOperation):
                    if len(self.queue) > self.size: self.queue.popitem(False)
                    self.queue[the_self] = res

            return res

        return new_f


def orig_apply(old_apply):
    def f(self, data_store):
        res = self._apply(data_store)
        res.__dict__['name'] = repr(self)
        return res

    return f


# it's a constant that is different from every other object
_no_default = object()


class Field(object):
    def __init__(self, pos=None, default=_no_default):
        self.default = default
        self.pos = pos


class OperationField(Field): pass


class PrimitiveField(Field): pass


class OperationCollection(Field): pass


class OperationMeta(type):
    def __new__(cls, name, bases, dct):
        res = type.__new__(cls, name, bases, dct)
        fields_pos = sorted([attr_type.pos for attr_name, attr_type in res.get_fields() if attr_type.pos is not None])
        if fields_pos != range(len(fields_pos)):
            raise ValueError("Bad fields pos for %s" % name)

        if name != 'Operation':
            method_name = 'is_%s' % (name.replace('Operation', '').lower())
            setattr(Operation, method_name, property(lambda self: isinstance(self, res)))
        return res


def general_new(iterable):
    return type(iterable)()


def general_append(iterable, k, v):
    if isinstance(iterable, list):
        assert k == len(iterable)
        iterable.append(v)
    elif isinstance(iterable, dict):
        iterable[k] = v
    elif isinstance(iterable, tuple):
        assert k == len(iterable)
        iterable = iterable + (v,)
    else:
        raise ValueError()
    return iterable


def general_iterator(iterable):
    if isinstance(iterable, list) or isinstance(iterable, tuple):
        return enumerate(iterable)
    elif isinstance(iterable, dict):
        return iterable.iteritems()
    else:
        raise ValueError()


def is_iterable(obj):
    return isinstance(obj, list) or isinstance(obj, dict) or isinstance(obj, tuple)


def recursive_map(iterable, callable, recursion_condition=None):
    recursion_condition = recursion_condition or is_iterable
    res = general_new(iterable)
    for k, v in general_iterator(iterable):
        if recursion_condition(v):
            res = general_append(res, k, recursive_map(v, callable, recursion_condition))
        else:
            res = general_append(res, k, callable(v))
    return res


class Operation(object):
    __metaclass__ = OperationMeta

    def __init__(self, *args, **kwargs):
        fields = dict(type(self).get_fields())
        pos2name = {attr_type.pos: attr_name for attr_name, attr_type in fields.iteritems() if
                    attr_type.pos is not None}
        if len(pos2name) == 0:
            max_nargs = 0
        else:
            max_nargs = max(pos2name) + 1 + len(
                [attr_type for attr_type in fields.itervalues() if attr_type.pos is None])
        assert len(args) <= max_nargs

        for i, arg in enumerate(args):
            kwargs[pos2name[i]] = arg

        for attr, attr_type in fields.iteritems():
            if attr_type.default is not _no_default and attr not in kwargs:
                kwargs[attr] = attr_type.default

        if len(kwargs) > len(fields):
            raise ValueError("Class %s does not take the following arguments: %s" % (
            type(self).__name__, ", ".join(f for f in kwargs if f not in fields)))
        elif len(kwargs) < len(fields):
            raise ValueError("Missing arguments for class %s: %s" % (
            type(self).__name__, ", ".join(f for f in fields if f not in kwargs)))

        for attr, attr_type in fields.iteritems():
            assert not isinstance(attr_type, OperationField) or isinstance(kwargs[attr],
                                                                           Operation), "Parameter %s should be an Operation" % attr

        for attr, attr_type in kwargs.iteritems():
            setattr(self, attr, attr_type)

        self._involved_operations = None

    def apply(self, data_store):
        res = self._apply(data_store)
        return res

    def copy(self):
        return type(self)._from_dict(self.to_dict())

    def replace(self, **kwargs):
        res = self.copy()
        for attr, val in kwargs.iteritems():
            assert hasattr(self, attr)
            setattr(res, attr, val)
        return res

    def get_suboperations(self):
        res = {}
        for attr, attr_type in type(self).get_fields():
            if isinstance(attr_type, OperationField):
                res[attr] = getattr(self, attr)
        return res

    def get_primitive_fields(self):
        res = {}
        for attr, attr_type in type(self).get_fields():
            if isinstance(attr_type, PrimitiveField):
                res[attr] = getattr(self, attr)
        return res

    @memoized_property
    def key(self):
        return '/%s' % json.dumps(self.__dict2key(self.to_dict()))

    def __setattr__(self, key, value):
        # invalidate key cache if you change the object
        if hasattr(self, '_key'): del self._key
        return super(Operation, self).__setattr__(key, value)

    def to_dict(self):
        res = {'type': type(self).__name__}
        for attr, attr_type in type(self).get_fields():
            if isinstance(attr_type, PrimitiveField):
                res[attr] = getattr(self, attr)
            elif isinstance(attr_type, OperationField):
                res[attr] = getattr(self, attr).to_dict()
            elif isinstance(attr_type, OperationCollection):
                def f(obj):
                    if isinstance(obj, Operation):
                        return obj.to_dict()
                    else:
                        return obj

                val = getattr(self, attr)
                res[attr] = recursive_map(val, f)

        return res

    @classmethod
    def get_fields(cls):
        for k in dir(cls):
            v = getattr(cls, k)
            if isinstance(v, OperationField) or isinstance(v, PrimitiveField) or isinstance(v, OperationCollection):
                yield k, v

    @classmethod
    def _get_all_subclasses(cls):
        res = []
        queue = cls.__subclasses__()
        while len(queue) > 0:
            e = queue.pop()
            l = e.__subclasses__()
            res.append(e)
            queue.extend(l)
        return res

    @staticmethod
    def type2operation_class(operation_type):
        for cls in Operation._get_all_subclasses():
            if cls.__name__ == operation_type: return cls

    @staticmethod
    def dict2operation(dict):
        cls = Operation.type2operation_class(dict['type'])
        if cls is None:
            raise ValueError('Unknown operation type')

        return cls._from_dict(dict)

    @staticmethod
    def key2operation(str):
        if str.startswith('/'): str = str[1:]
        kwargs = Operation.__key2dict(json.loads(str))
        return Operation.dict2operation(kwargs)

    def involved_operations(self):
        if self._involved_operations is not None: return self._involved_operations

        res = set()
        for k, field_type in type(self).get_fields():
            if not isinstance(field_type, OperationField): continue

            v = getattr(self, k)
            res.update(v.involved_operations())

        res = sorted(res)
        self._involved_operations = res
        return res

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return type(other).__name__ == type(self).__name__ and self.key == other.key

    def _apply(self, data_store):
        raise NotImplementedError()

    @classmethod
    def _from_dict(cls, kwargs):
        kwargs = kwargs.copy()
        kwargs.pop('type')
        for attr, attr_type in cls.get_fields():
            if isinstance(attr_type, OperationField):
                kwargs[attr] = Operation.dict2operation(kwargs[attr])
            if isinstance(attr_type, OperationCollection):
                def f(obj):
                    try:
                        return Operation.dict2operation(obj)
                    except:
                        return obj

                def recursion_condition(obj):
                    try:
                        Operation.dict2operation(obj)
                        return False
                    except:
                        return is_iterable(obj)

                kwargs[attr] = recursive_map(kwargs[attr], f, recursion_condition)

        return cls(**kwargs)

    @classmethod
    def __dict2key(cls, d):
        d = d.copy()
        for k, v in d.iteritems():
            if isinstance(v, dict):
                d[k] = cls.__dict2key(v)

        # TODO que devuelva algo que no es un diccionario
        return {'transformed': True, 'dict': sorted(d.iteritems(), key=lambda x: x[0])}

    @classmethod
    def __key2dict(cls, obj):
        if isinstance(obj, dict) and obj.get('transformed') is True and 'dict' in obj:
            res = dict(obj['dict'])
            for k, v in res.iteritems():
                res[k] = cls.__key2dict(v)
            return res
        else:
            return obj


class GetOperation(Operation):
    name = PrimitiveField(0)

    def _apply(self, data_store):
        return data_store.get(self.name)
