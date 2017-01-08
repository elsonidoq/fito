import json

from StringIO import StringIO
from datetime import datetime
from functools import total_ordering

from fito.operations.utils import recursive_map, is_iterable, general_iterator
from memoized_property import memoized_property

# it's a constant that is different from every other object
_no_default = object()
import warnings

try:
    from bson import json_util
    from json import dumps, dump, load, loads


    def json_dumps(*args, **kwargs):
        kwargs['default'] = json_util.default
        return dumps(*args, **kwargs)


    def json_dump(*args, **kwargs):
        kwargs['default'] = json_util.object_hook
        return dump(*args, **kwargs)


    def json_loads(*args, **kwargs):
        kwargs['object_hook'] = json_util.object_hook
        return loads(*args, **kwargs)


    def json_load(*args, **kwargs):
        kwargs['object_hook'] = json_util.object_hook
        return load(*args, **kwargs)


    json.dump = json_dump
    json.dumps = json_dumps
    json.load = json_load
    json.loads = json_loads

except ImportError:
    warnings.warn("Couldnt import json_util from bson, won't be able to handle datetime")


class Field(object):
    """
    Base class for field definition on an :py:class:`Operation`
    """

    def __init__(self, pos=None, default=_no_default, *args, **kwargs):
        self.default = default
        self.pos = pos

    @property
    def allowed_types(self):
        raise NotImplementedError()

    def check_valid_value(self, value):
        return any([isinstance(value, t) for t in self.allowed_types])


class PrimitiveField(Field):
    """
    Specifies a Field whose value is going to be a python object
    """

    @property
    def allowed_types(self):
        return [object]


class CollectionField(PrimitiveField):
    def __len__(self): return

    def __getitem__(self, _): return

    def __setitem__(self, _, __): return

    def __delitem__(self, _): return

    def __reversed__(self): return

    def __contains__(self, _): return

    def __setslice__(self, _, __, ___): return

    def __delslice__(self, _, __): return

    @property
    def allowed_types(self):
        return list, dict, tuple


@total_ordering
class NumericField(PrimitiveField):
    def __lt__(self, _): return

    def __add__(self, _): return

    def __sub__(self, other): return

    def __mul__(self, other): return

    def __floordiv__(self, other): return

    def __mod__(self, other): return

    def __divmod__(self, other): return

    def __pow__(self, _, modulo=None): return

    def __lshift__(self, other): return

    def __rshift__(self, other): return

    def __and__(self, other): return

    def __xor__(self, other): return

    def __or__(self, other): return

    @property
    def allowed_types(self):
        return int, float


class BaseOperationField(Field):
    """
    Specifies a Field whose value will be an Operation
    """

    def __init__(self, pos=None, default=_no_default, base_type=None, *args, **kwargs):
        super(BaseOperationField, self).__init__(pos=pos, default=default, *args, **kwargs)
        self.base_type = base_type

    @property
    def allowed_types(self):
        return [Operation if self.base_type is None else self.base_type]


def OperationField(pos=None, default=_no_default, base_type=None):
    if base_type is not None:
        assert issubclass(base_type, Operation)
        return_type = type(
            'OperationField{}'.format(base_type.__name__),
            (BaseOperationField, base_type),
            {}
        )
    else:
        return_type = BaseOperationField

    return return_type(pos=pos, default=default, base_type=base_type)


class OperationCollection(Field):
    """
    Specifies a Field whose value is going to be a collection of operations
    """

    @property
    def allowed_types(self):
        return list, dict, tuple

    def check_valid_value(self, value):
        if not is_iterable(value): return False

        for k, v in general_iterator(value):
            if not isinstance(v, Operation): return False

        return True


class OperationMeta(type):
    def __new__(cls, name, bases, dct):
        """
        Called when the class is created (i.e. when it is loaded into the module)

        Checks whether the attributes spec makes sense or not.

        :return: New Operation subclass
        """
        res = type.__new__(cls, name, bases, dct)
        fields_pos = sorted([attr_type.pos for attr_name, attr_type in res.get_fields() if attr_type.pos is not None])
        if fields_pos != range(len(fields_pos)):
            raise ValueError("Bad `pos` for attribute %s" % name)

        if name != 'Operation':
            method_name = 'is_%s' % (name.replace('Operation', '').lower())
            setattr(Operation, method_name, property(lambda self: isinstance(self, res)))
        return res


class InvalidOperationInstance(Exception):
    pass


class Operation(object):
    """
    Base class for any operation.

    It handles the spec checking in order to guarantee that only valid instances can be generated

    An example would be

    >>> class GetAge(Operation):
    >>>    user_id = PrimitiveField(0)
    >>>
    >>> def _apply(self, datas_tore):
    >>>     return data_store.get(self)
    >>>
    >>> def add(self, other):
    >>>     return AddOperation(self, other)

    >>> class AddOperation(Operation):
    >>>     left = OperationField(0)
    >>>     right = OperationField(1)
    >>>
    >>>     def _apply(self, data_store):
    >>>         return data_store.execute(self.left) + data_store.execute(self.right)
    >>>
    >>> data_store = {1: 30, 2: 40}
    >>> op = GetAge(1).add(GetAge(2))
    >>>
    """
    __metaclass__ = OperationMeta

    def __init__(self, *args, **kwargs):
        # Get the field spec
        fields = dict(type(self).get_fields())

        #
        pos2name = {attr_type.pos: attr_name for attr_name, attr_type in fields.iteritems() if
                    attr_type.pos is not None}
        if len(pos2name) == 0:
            max_nargs = 0
        else:
            max_nargs = max(pos2name) + 1 + len(
                [attr_type for attr_type in fields.itervalues() if attr_type.pos is None])
        if len(args) > max_nargs:
            raise InvalidOperationInstance(
                (
                    "This operation was instanced with {given_args} positional arguments, but I only know how "
                    "to handle the first {specified_args} positional arguments.\n"
                    "Instance the fields with `pos` keyword argument (e.g. PrimitiveField(pos=0))"
                ).format(given_args=len(args), specified_args=max_nargs)
            )

        for i, arg in enumerate(args):
            kwargs[pos2name[i]] = arg

        for attr, attr_type in fields.iteritems():
            if attr_type.default is not _no_default and attr not in kwargs:
                kwargs[attr] = attr_type.default

        if len(kwargs) > len(fields):
            raise InvalidOperationInstance("Class %s does not take the following arguments: %s" % (
                type(self).__name__, ", ".join(f for f in kwargs if f not in fields)))
        elif len(kwargs) < len(fields):
            raise InvalidOperationInstance("Missing arguments for class %s: %s" % (
                type(self).__name__, ", ".join(f for f in fields if f not in kwargs)))

        for attr, attr_type in fields.iteritems():
            val = kwargs.get(attr)
            if val is None: continue
            if not attr_type.check_valid_value(val):
                raise InvalidOperationInstance(
                    "Invalid value for parameter {}. Received {}, expected {}".format(attr, val, attr_type.allowed_types)
                )

        for attr in kwargs:
            if attr not in fields:
                raise InvalidOperationInstance("Received extra parameter {}".format(attr))

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
            field_spec = self.get_field_spec(attr)

            if not field_spec.check_valid_value(val):
                raise InvalidOperationInstance(
                    "Invalid value for field {}. Received {}, expected {}".format(attr, val, field_spec.allowed_types)
                )

            setattr(res, attr, val)
        return res

    @classmethod
    def get_field_spec(cls, field_name):
        res = getattr(cls, field_name)
        assert isinstance(res, Field)
        return res

    def get_suboperations(self):
        res = {}
        for attr, attr_type in type(self).get_fields():
            if isinstance(attr_type, BaseOperationField):
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
            elif isinstance(attr_type, BaseOperationField):
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

    # This is a little bit hacky, but I just want to write this short
    class Exporter(object):
        def __init__(self, module, what, **kwargs):
            self.what = what
            self.dump = lambda f: module.dump(what, f, **kwargs)
            self.dumps = lambda: module.dumps(what, **kwargs)

    @property
    def yaml(self):
        # lazy import to avoid adding the dependency package wide
        import yaml

        # yaml doesnt provide a dumps function
        def dumps(what, *args, **kwargs):
            f = StringIO()
            yaml.dump(what, f, *args, **kwargs)
            return f.getvalue()

        yaml.dumps = dumps

        return Operation.Exporter(yaml, self.to_dict(), default_flow_style=False)

    @property
    def json(self):
        return Operation.Exporter(json, self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, string):
        return cls.dict2operation(json.loads(string))

    @classmethod
    def from_yaml(cls, string):
        import yaml
        return cls.dict2operation(yaml.load(StringIO(string)))

    @classmethod
    def get_fields(cls):
        for k in dir(cls):
            v = getattr(cls, k)
            if isinstance(v, BaseOperationField) or isinstance(v, PrimitiveField) or isinstance(v, OperationCollection):
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
            if not isinstance(field_type, BaseOperationField): continue

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
            if isinstance(attr_type, BaseOperationField):
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
    name = PrimitiveField(0, base_type=basestring)

    def _apply(self, data_store):
        return data_store.get(self.name)
