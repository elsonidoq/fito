import ctypes
import inspect
import json
import os
import re
import traceback
import warnings
from functools import partial
from functools import total_ordering

from fito.specs.fields import KwargsField, ArgsField, Field, BaseSpecField, SpecCollection, UnboundField, \
    PrimitiveField
from fito.specs.utils import recursive_map, is_iterable
from memoized_property import memoized_property

try:
    from bson import json_util
    from bson import json_util
    from json import dumps, dump, load, loads


    def json_dumps(*args, **kwargs):
        kwargs['default'] = json_util.default
        return dumps(*args, **kwargs)


    def json_dump(*args, **kwargs):
        kwargs['default'] = json_util.default
        return dump(*args, **kwargs)


    def set_default_json_options():
        # how should we handle datetimes? This forces non timezone aware datetimes
        # TODO: Either throw exception when a tz aware datetime is received, or handle both correctly
        res = json_util.DEFAULT_JSON_OPTIONS = json_util.JSONOptions(tz_aware=False)
        return res


    def json_loads(*args, **kwargs):
        kwargs['object_hook'] = partial(json_util.object_hook, json_options=set_default_json_options())
        return loads(*args, **kwargs)


    def json_load(*args, **kwargs):
        kwargs['object_hook'] = partial(json_util.object_hook, json_options=set_default_json_options())
        return load(*args, **kwargs)


    json.dump = json_dump
    json.dumps = json_dumps
    json.load = json_load
    json.loads = json_loads

except ImportError:
    warnings.warn("Couldn't import json_util from bson, won't be able to handle datetime")

try:
    import yaml
except ImportError:
    warnings.warn("Couldn't yaml, some features won't be enabled")


class WeirdModulePathException(Exception): pass


class SpecMeta(type):
    def __new__(cls, name, bases, dct):
        """
        Called when the class is created (i.e. when it is loaded into the module)

        Checks whether the attributes spec makes sense or not.

        :return: New Spec subclass
        """
        res = type.__new__(cls, name, bases, dct)
        if res.__doc__ is None:
            res.__doc__ = res.get_default_doc_string()

        if '..' in repr(res):
            raise WeirdModulePathException(
                "Received a weird module path ({}). This seems to happen when ".format(repr(res)) +
                "a class is imported indirectly from a yaml"
            )
        fields = dict(res.get_fields())
        fields_pos = sorted([attr_type.pos for attr_name, attr_type in fields.iteritems() if attr_type.pos is not None])

        if fields_pos != range(len(fields_pos)):
            raise ValueError("Bad `pos` for attribute in class %s" % name)

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

    # There are a lot of subclasses of Spec that are dinamically created. Sometimes it's usefull to be able to know
    # whether that's the case or not
    # TODO: make it read only
    dinamically_created = False

    def __init__(self, *args, **kwargs):
        fields = dict(self.get_fields())
        self.initialize(fields, *args, **kwargs)

    def initialize(self, fields, *args, **kwargs):
        pos2name = {}
        kwargs_field = None
        args_field = None
        for attr_name, attr_type in fields.iteritems():
            if attr_type.pos is not None:
                pos2name[attr_type.pos] = attr_name

            elif isinstance(attr_type, KwargsField):
                if kwargs_field is not None:
                    raise RuntimeError(
                        "A spec can have at most one kwargs field, found {} and {}".format(attr_name, kwargs_field))
                kwargs_field = attr_name

            elif isinstance(attr_type, ArgsField):
                if args_field is not None:
                    raise RuntimeError(
                        "A spec can have at most one args field, found {} and {}".format(attr_name, kwargs_field))
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
                    "Class '{type_name}' was instanced with {given_args} positional arguments, but I only know how "
                    "to handle the first {specified_args} positional arguments.\n"
                    "Instance the fields with `pos` keyword argument (e.g. PrimitiveField(pos=0))"
                ).format(type_name=type(self).__name__, given_args=len(args), specified_args=max_nargs)
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
            if attr_type.has_default_value() and attr not in kwargs:
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
                    (
                        "Invalid value for parameter {attr} in {type_name}." +
                        "Received {val}, expected {expected_types}\n" +
                        "If you think {val} is an instance of any of the allowed classes ({expected_types}), then this " +
                        "might be an issue related to the having reloaded a module containing de definition of {val}"
                    ).format(
                        attr=attr,
                        type_name=type(self).__name__,
                        val=val,
                        expected_types=attr_type.allowed_types
                    )
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

        return self

    def copy(self):
        return type(self)._from_dict(self.to_dict(include_all=True))

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
        return '/%s' % json.dumps(self.__dict2key(self.to_dict(include_all=False)))

    def __setattr__(self, key, value):
        # invalidate key cache if you change the object
        if hasattr(self, '_key'): del self._key
        return super(Spec, self).__setattr__(key, value)

    def to_dict(self, include_all=False):
        """
        :param include_toggles: Wether to include or not toggle_fields, default=False
        """
        import_path = get_import_path(type(self))
        if inspect.getmodule(type(self)).__name__ == '__main__':
            warnings.warn(
                """
                The module of {} is __main__.
                It's likely that you are not going to be able to desserialize this spec
                """.format(type(self))
            )

        res = {'type': import_path}

        for attr, attr_type in type(self).get_fields():
            val = getattr(self, attr)

            if isinstance(attr_type, PrimitiveField) and (attr_type.serialize or include_all):
                if inspect.isfunction(val) or inspect.isclass(val):
                    val = 'import {}'.format(get_import_path(val))

                res[attr] = val

            elif isinstance(attr_type, BaseSpecField) and (include_all or attr_type.serialize):
                res[attr] = val if val is None else val.to_dict(include_all=include_all)

            elif isinstance(attr_type, SpecCollection) and (include_all or attr_type.serialize):
                def f(obj):
                    if isinstance(obj, Spec):
                        return obj.to_dict(include_all=include_all)
                    else:
                        return obj

                res[attr] = recursive_map(val, f)

        return res

    def to_kwargs(self, include_all=False):
        """
        Useful function to call f(**spec.to_kwargs())
        :param include_all: Whether to include the fields whose spec has serialize == False
        """
        res = self.to_dict(include_all=include_all)
        res.pop('type')
        return res

    # This is a little bit hacky, but I just want to write this short
    class Exporter(object):
        def __init__(self, module, what, **kwargs):
            self.what = what
            self.dump = lambda f: module.dump(what, f, **kwargs)
            self.dumps = lambda: module.dumps(what, **kwargs)

    @property
    def yaml(self, include_all=False):
        yaml.dumps = yaml.dump
        return Spec.Exporter(yaml, self.to_dict(include_all=include_all), default_flow_style=False)

    @property
    def json(self, include_all=False):
        return Spec.Exporter(json, self.to_dict(include_all=include_all), indent=2)

    class Importer(object):
        def __init__(self, cls, module):
            self.cls = cls
            self.module = module

        def load(self, path):
            with open(path) as f:
                return self.cls.dict2spec(self.module.load(f), path=os.path.abspath(os.path.dirname(path)))

        def loads(self, string): return self.cls.dict2spec(self.module.loads(string))

    @classmethod
    def from_json(cls):
        return Spec.Importer(cls, json)

    @classmethod
    def from_yaml(cls):
        yaml.loads = yaml.load
        return Spec.Importer(cls, yaml)

    @classmethod
    def get_fields(cls):
        for k in dir(cls):
            v = getattr(cls, k)
            if isinstance(v, Field) and not isinstance(v, UnboundField):
                yield k, v

    @classmethod
    def get_unbinded_fields(cls):
        for k in dir(cls):
            v = getattr(cls, k)
            if isinstance(v, UnboundField):
                yield k, v

    def bind(self, *args, **kwargs):
        fields = dict(self.get_unbinded_fields())
        return self.copy().initialize(fields, *args, **kwargs)

    def inplace_bind(self, *args, **kwargs):
        fields = dict(self.get_unbinded_fields())
        return self.initialize(fields, *args, **kwargs)

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
    def dict2spec(dict, path=None):
        """
        Loads a Spec from a dictionary
        :param dict: Dictionary to load it from
        :param path: Used to build relative paths when referencing other files, see Spec.Importer.load
        """
        cls = Spec.type2spec_class(dict['type'])
        if cls is None:
            raise ValueError(
                "Unknown spec type: {}\n".format(dict['type']) +
                "This might happen if you are referencing an Spec that hasn't been imported"
            )

        return cls._from_dict(dict, path=path)

    @staticmethod
    def key2spec(str):
        try:
            return Spec.dict2spec(Spec.key2dict(str))
        except ValueError, e:
            raise e
        except Exception, e:
            raise ValueError(e.args)

    def __hash__(self):
        return hash(self.key)

    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self.key == other.key
        )

    def __lt__(self, other):
        return self.key < other.key

    def __ne__(self, other):
        return self.key != other.key

    @classmethod
    def _from_dict(cls, kwargs, path=None):
        kwargs = kwargs.copy()
        kwargs.pop('type')
        args = tuple()

        for attr, attr_type in cls.get_fields():
            val = kwargs.get(attr, attr_type.default)

            if (isinstance(attr_type, PrimitiveField) and
                    isinstance(val, basestring) and
                    val.startswith('import ')):
                kwargs[attr] = obj_from_path(val[len('import '):])

            elif isinstance(attr_type, BaseSpecField) and val is not None and attr in kwargs:
                if isinstance(val, basestring):
                    if not os.path.exists(val) and path is not None:
                        if not os.path.exists(os.path.join(path, val)):
                            raise RuntimeError(
                                "Could not load referenced file ({}) for attribute {}".format(val, attr)
                            )
                        val = os.path.join(path, val)

                    if val.endswith('.yaml'):
                        with open(val) as f:
                            val = yaml.load(f)
                    elif val.endswith('.json'):
                        with open(val) as f:
                            val = json.load(f)
                    else:
                        raise RuntimeError('Invalid extension for referenced attribute {}, path: {}'.format(attr, val))

                # should be a dict
                kwargs[attr] = Spec.dict2spec(val, path=path)

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
    def key2dict(cls, str):
        if str.startswith('/'): str = str[1:]
        return cls._key2dict(json.loads(str))

    @classmethod
    def _key2dict(cls, obj):
        if isinstance(obj, dict) and obj.get('transformed') is True and 'dict' in obj:
            res = dict(obj['dict'])
            for k, v in res.iteritems():
                res[k] = cls._key2dict(v)
            return res
        else:
            return obj

    @classmethod
    def get_default_doc_string(cls):
        fields = list(cls.get_fields())
        fields.sort(key=lambda x: x[1].pos or len(fields))

        res = ['\n\t{} fields: '.format(cls.__name__)]
        for attr, attr_type in fields:
            res.append('\t\t{} = {}'.format(attr, attr_type))

        return '\n'.join(res) + '\n'


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

    if inspect.isclass(obj) or inspect.isfunction(obj):
        res = '{}:{}'.format(mod.__name__, obj.__name__)
    elif isinstance(obj, Spec):
        # TODO: this implies that I assume that the Spec key is enough to describe any Spec
        res = 'key: ({})'.format(obj.key)
    else:
        res = '{}:{}@{}'.format(mod.__name__, type(obj).__name__, id(obj))

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
    if path.startswith('key: '):
        gd = re.match('^key: \((?P<key>.*?)\)(\.(?P<attrs>.*?))?$', path).groupdict()

        res = Spec.key2spec(gd['key'])

        for attr in gd.get('attrs', '').split('.'):
            res = getattr(res, attr)
        return res

    else:
        parts = path.split(':')
        assert len(parts) <= 2

        obj_path = []
        full_path = parts[0]
        if len(parts) == 2:
            obj_path = parts[1].split('.')

        fromlist = '.'.join(full_path.split('.')[:-1])

        try:
            module = __import__(full_path, fromlist=fromlist)
        except WeirdModulePathException, e:
            traceback.print_exc()
            raise RuntimeError("Couldn't import {}".format(path) + '\n' + e.args[0])
        except ImportError:
            traceback.print_exc()
            raise RuntimeError("Couldn't import {}".format(path))

        obj = module
        for i, attr in enumerate(obj_path):
            if '@' in attr:
                assert i == 0
                attr, id = attr.split('@')
                klass = getattr(obj, attr)
                instance = load_object(int(id))
                assert isinstance(instance, klass)
                obj = instance
            else:
                obj = getattr(obj, attr)
        return obj


def load_object(id):
    return ctypes.cast(id, ctypes.py_object).value
