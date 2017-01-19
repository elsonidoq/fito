import inspect
import json
from StringIO import StringIO
from functools import total_ordering

from memoized_property import memoized_property

from fito.specs.utils import recursive_map, is_iterable, general_iterator


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
    Base class for field definition on an :py:class:`Spec`
    """

    def __init__(self, pos=None, default=_no_default, *args, **kwargs):
        self.default = default
        self.pos = pos

    @property
    def allowed_types(self):
        raise NotImplementedError()

    def check_valid_value(self, value):
        return any([isinstance(value, t) for t in self.allowed_types])

    def __eq__(self, other):
        return self is other


class ToggleField(Field):
    """
    Useful tu change subtleness on specs that you don't want have impact on
    the serialized spec
    """

    @property
    def allowed_types(self):
        return [object]


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


class BaseSpecField(Field):
    """
    Specifies a Field whose value will be an Spec
    """

    def __init__(self, pos=None, default=_no_default, base_type=None, serialize=True, *args, **kwargs):
        super(BaseSpecField, self).__init__(pos=pos, default=default, *args, **kwargs)
        self.base_type = base_type
        self.serialize = serialize

    @property
    def allowed_types(self):
        return [Spec if self.base_type is None else self.base_type]


def SpecField(pos=None, default=_no_default, base_type=None, serialize=True):
    """
    Builds a SpecField

    :param pos: Position on *args
    :param default: Default value
    :param base_type: Base type, it does some type checkig + avoids some warnings from IntelliJ
    :param serialize: Whether this spec field should be included in the serialization of the object

    :return:
    """
    if serialize and default is _no_default:
        raise RuntimeError("If serialize == False, the field should have a default value")

    if base_type is not None:
        assert issubclass(base_type, Spec)
        return_type = type(
            'SpecField{}'.format(base_type.__name__),
            (BaseSpecField, base_type),
            {}
        )
    else:
        return_type = BaseSpecField

    return return_type(pos=pos, default=default, base_type=base_type)


class SpecCollection(Field):
    """
    Specifies a Field whose value is going to be a collection of specs
    """

    @property
    def allowed_types(self):
        return list, dict, tuple

    def check_valid_value(self, value):
        if not is_iterable(value): return False

        for k, v in general_iterator(value):
            if not isinstance(v, Spec): return False

        return True


class KwargsField(SpecCollection):
    def __init__(self):
        super(KwargsField, self).__init__(default={})

    @property
    def allowed_types(self):
        return [dict]


class ArgsField(SpecCollection):
    def __init__(self):
        super(ArgsField, self).__init__(default=tuple())

    @property
    def allowed_types(self):
        return [tuple, list]


class SpecMeta(type):
    def __new__(cls, name, bases, dct):
        """
        Called when the class is created (i.e. when it is loaded into the module)

        Checks whether the attributes spec makes sense or not.

        :return: New Spec subclass
        """
        res = type.__new__(cls, name, bases, dct)
        fields = dict(res.get_fields())
        fields_pos = sorted([attr_type.pos for attr_name, attr_type in fields.iteritems() if attr_type.pos is not None])

        if fields_pos != range(len(fields_pos)):
            raise ValueError("Bad `pos` for attribute %s" % name)

        if 'key' in fields:
            raise ValueError("Can not use the `key` field, it's reserved")

        return res


class InvalidSpecInstance(Exception):
    pass


@total_ordering
class Spec(object):
    """
    Base class for any spec.

    It handles the spec checking in order to guarantee that only valid instances can be generated

    Suppose you want to specify an experiment

    >>> class Experiment(Spec):
    >>>    input = SpecField()
    >>>    model = SpecField()
    >>>
    >>> class Input(Spec):
    >>>     path = PrimitiveField(0)
    >>>
    >>> class LinearRegression(Spec):
    >>>     regularize = PrimitiveField(default=False)
    >>>

    Now you can specify an experiment like this

    >>> exp = Experiment(input = Input('input.csv'),
    ...                  model = LinearRegression()
    ... )
    >>>

    And write it to a nice yaml
    >>> with open('exp_spec.yaml', 'w') as f:
    >>>     exp.yaml.dump(f)

    And load it back
    >>> with open('exp_spec.yaml') as f:
    >>>     exp = Experiment.from_yaml()
    """
    __metaclass__ = SpecMeta

    def __init__(self, *args, **kwargs):
        # Get the field spec
        fields = dict(self.get_fields())

        pos2name = {}
        kwargs_field = None
        args_field = None
        for attr_name, attr_type in fields.iteritems():
            if attr_type.pos is not None:
                pos2name[attr_type.pos] = attr_name

            elif isinstance(attr_type, KwargsField):
                kwargs_field = attr_name

            elif isinstance(attr_type, ArgsField):
                args_field = attr_name

        if len(pos2name) == 0:
            max_nargs = 0
        else:
            max_nargs = (
                max(pos2name) + 1  # +
                # len([attr_type for attr_type in fields.itervalues() if attr_type.pos is None])
            )
        if len(args) > max_nargs and args_field is None:
            raise InvalidSpecInstance(
                (
                    "This spec was instanced with {given_args} positional arguments, but I only know how "
                    "to handle the first {specified_args} positional arguments.\n"
                    "Instance the fields with `pos` keyword argument (e.g. PrimitiveField(pos=0))"
                ).format(given_args=len(args), specified_args=max_nargs)
            )

        # These guys are the ones that are going to be passed to the instance
        args_param_value = []
        kwargs_param_value = {}

        for i, arg in enumerate(args):
            if args_field is not None and i >= max_nargs:
                args_param_value.append(arg)
            else:
                kwargs[pos2name[i]] = arg

        for attr, attr_type in fields.iteritems():
            if attr_type.default is not _no_default and attr not in kwargs:
                kwargs[attr] = attr_type.default

        if kwargs_field is not None:
            kwargs_param_value = {
                attr: attr_type
                for attr, attr_type in kwargs.iteritems()
                if attr not in fields
                }

            kwargs = {
                attr: attr_type
                for attr, attr_type in kwargs.iteritems()
                if attr in fields and attr != kwargs_field and attr != args_field
                }

        if len(kwargs) > len(fields):
            raise InvalidSpecInstance("Class %s does not take the following arguments: %s" % (
                type(self).__name__, ", ".join(f for f in kwargs if f not in fields)))
        elif len(kwargs) < len(fields) - (args_field is not None) - (kwargs_field is not None):
            raise InvalidSpecInstance("Missing arguments for class %s: %s" % (
                type(self).__name__, ", ".join(f for f in fields if f not in kwargs)))

        for attr, attr_type in fields.iteritems():
            val = kwargs.get(attr)
            if val is None: continue
            if not attr_type.check_valid_value(val):
                raise InvalidSpecInstance(
                    "Invalid value for parameter {} in {}. Received {}, expected {}".format(
                        attr, type(self).__name__, val, attr_type.allowed_types)
                )

        for attr in kwargs:
            if attr not in fields:
                raise InvalidSpecInstance("Received extra parameter {}".format(attr))

        if args_field is not None:
            setattr(self, args_field, tuple(args_param_value))

        if kwargs_field is not None:
            setattr(self, kwargs_field, kwargs_param_value)

        for attr, attr_type in kwargs.iteritems():
            setattr(self, attr, attr_type)

    def copy(self):
        return type(self)._from_dict(self.to_dict())

    def replace(self, **kwargs):
        res = self.copy()
        for attr, val in kwargs.iteritems():
            field_spec = self.get_field_spec(attr)

            if not field_spec.check_valid_value(val):
                raise InvalidSpecInstance(
                    "Invalid value for field {}. Received {}, expected {}".format(attr, val, field_spec.allowed_types)
                )

            setattr(res, attr, val)
        return res

    @classmethod
    def get_field_spec(cls, field_name):
        res = getattr(cls, field_name)
        assert isinstance(res, Field)
        return res

    def get_spec_fields(self):
        res = {}
        for attr, attr_type in type(self).get_fields():
            if isinstance(attr_type, BaseSpecField):
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
        return super(Spec, self).__setattr__(key, value)

    def to_dict(self, include_toggle_fields=False):
        """
        :param include_toggles: Wether to include or not toggle_fields, default=False
        """
        res = {'type': get_import_path(type(self))}

        for attr, attr_type in type(self).get_fields():
            val = getattr(self, attr)

            if isinstance(attr_type, PrimitiveField) or (isinstance(attr_type, ToggleField) and include_toggle_fields):
                if inspect.isfunction(val) or inspect.isclass(val):
                    val = get_import_path(val)

                res[attr] = val

            elif isinstance(attr_type, BaseSpecField) and attr_type.serialize:
                res[attr] = val if val is None else val.to_dict()

            elif isinstance(attr_type, SpecCollection):
                def f(obj):
                    if isinstance(obj, Spec):
                        return obj.to_dict()
                    else:
                        return obj

                res[attr] = recursive_map(val, f)

        return res

    def to_kwargs(self, include_toggle_fields=True, include_out_data_store=False):
        """
        Useful function to call f(**spec.to_kwargs())
        """
        res = self.to_dict(include_toggle_fields=include_toggle_fields)
        res.pop('type')
        if not include_out_data_store: res.pop('out_data_store')
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

        return Spec.Exporter(yaml, self.to_dict(), default_flow_style=False)

    @property
    def json(self):
        return Spec.Exporter(json, self.to_dict(), indent=2)

    @classmethod
    def from_json(cls, string):
        return cls.dict2spec(json.loads(string))

    @classmethod
    def from_yaml(cls, string):
        import yaml
        return cls.dict2spec(yaml.load(StringIO(string)))

    @classmethod
    def get_fields(cls):
        for k in dir(cls):
            v = getattr(cls, k)
            if isinstance(v, Field):
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
    def type2spec_class(spec_type):
        """
        Can be called either by calling Spec.type2spec_class('SomeSpec') or by calling
        Spec.type2spec_class('some.module:SomeSpec')

        :param spec_type: Either the name of the class, which must be imported before calling this function or the
        import path spec
        :return: A subclass of Spec
        """
        if ':' in spec_type:
            cls = obj_from_path(spec_type)
            assert issubclass(cls, Spec), "The provided path does not point to an Spec subclass"
            return cls
        else:
            for cls in Spec._get_all_subclasses():
                if cls.__name__ == spec_type: return cls

    @staticmethod
    def dict2spec(dict):
        cls = Spec.type2spec_class(dict['type'])
        if cls is None:
            raise ValueError(
                "Unknown spec type."
                "This might happen if you are referencing an Spec that hasn't been imported"
            )

        return cls._from_dict(dict)

    @staticmethod
    def key2spec(str):
        try:
            if str.startswith('/'): str = str[1:]
            kwargs = Spec.__key2dict(json.loads(str))
            return Spec.dict2spec(kwargs)
        except ValueError, e:
            raise e
        except Exception, e:
            raise ValueError(e.args)

    def __hash__(self):

        return hash(self.key)

    def __eq__(self, other):
        return type(self).__name__ == type(other).__name__ and self.key == other.key

    def __lt__(self, other):
        return self.key < other.key

    def __ne__(self, other):
        return self.key != other.key

    @classmethod
    def _from_dict(cls, kwargs):
        kwargs = kwargs.copy()
        kwargs.pop('type')
        args = tuple()

        for attr, attr_type in cls.get_fields():
            val = kwargs.get(attr, attr_type.default)

            if isinstance(attr_type, PrimitiveField) and isinstance(val, basestring) and ':' in kwargs[attr]:
                kwargs[attr] = obj_from_path(val)

            elif isinstance(attr_type, BaseSpecField) and val is not None:
                kwargs[attr] = Spec.dict2spec(kwargs[attr])

            elif isinstance(attr_type, SpecCollection):
                def f(obj):
                    try:
                        return Spec.dict2spec(obj)
                    except:
                        return obj

                def recursion_condition(obj):
                    try:
                        Spec.dict2spec(obj)
                        return False
                    except:
                        return is_iterable(obj)

                val = recursive_map(kwargs[attr], f, recursion_condition)
                if isinstance(attr_type, ArgsField):
                    args = tuple(val)
                elif isinstance(attr_type, KwargsField):
                    kwargs.update(val)
                else:
                    kwargs[attr] = val

        return cls(*args, **kwargs)

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


def get_import_path(obj, *attrs):
    """
    Builds a string representing an object.
    The default behaviour is the same than the import statement. Additionaly, you can specify attributes.

    For example:
    >>> get_import_path(Spec)
    'fito.specs.base:Spec'

    >>> get_import_path(Spec, 'dict2spec')
    'fito.specs.base:Spec.dict2spec'

    The inverse function of get_import_path is obj_from_path
    """
    mod = inspect.getmodule(obj)
    res = '{}:{}'.format(mod.__name__, obj.__name__)
    if attrs:
        for attr in attrs:
            res = '{}.{}'.format(res, attr)
    return res


def obj_from_path(path):
    """
    Retrieves an object from a given import path. The format is slightly different from the standard python one in
    order to be more expressive.

    Examples:
    >>> obj_from_path('fito')
    <module 'fito'>

    >>> obj_from_path('fito.specs')
    <module 'fito.specs'>

    >>> obj_from_path('fito.specs.base:Spec')
    fito.specs.base.Spec

    >>> obj_from_path('fito.specs.base:Spec.dict2spec')
    <function fito.specs.base.dict2spec>
    """
    parts = path.split(':')
    assert len(parts) <= 2

    obj_path = []
    full_path = parts[0]
    if len(parts) == 2:
        obj_path = parts[1].split('.')

    fromlist = '.'.join(full_path.split('.')[:-1])
    module = __import__(full_path, fromlist=fromlist)

    obj = module
    for attr in obj_path: obj = getattr(obj, attr)

    return obj
