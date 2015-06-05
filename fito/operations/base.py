import inspect
import operator

import pandas as pd
import numpy as np
from collections import OrderedDict
import json

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

def as_operation(**args_specifications):
    # XXX ver bien que pasa con is_method y is_class, no anda bien

    is_method = args_specifications.pop('is_method', False)
    is_class = args_specifications.pop('is_class', False)
    out_type = args_specifications.pop('out_type', Operation)
    out_name = args_specifications.pop('out_name', None)
    def decorator(f):
        f_spec = inspect.getargspec(f)
        if f_spec[3] is None:
            default_values = {}
        else:
            args_with_defaults = f_spec[0][-len(f_spec[3]):]
            default_values = dict(zip(args_with_defaults, f_spec[3]))
        attrs = {}
        for i, arg in enumerate(f_spec[0]):
            if is_class and arg == 'self': continue

            if is_method and arg == 'self':
                arg = 'this'
            if arg in args_specifications:
                spec = args_specifications[arg]()
                spec.pos = len(attrs)
            else:
                spec = PrimitiveField(len(attrs))
            if arg in default_values: spec.default = default_values[arg]
            attrs[arg] = spec

        def get_this_args(self, data_store=None):
            this_args = {}
            for k, v in attrs.iteritems():
                value = getattr(self, k)
                if isinstance(v, OperationField) and data_store is not None:
                    value = data_store.execute(value)

                this_args[k] = value

            return this_args

        def _apply(self, data_store):
            this_args = self.get_this_args(data_store)
            if is_method:
                this = this_args.pop('this')
                return f(this, **this_args)
            else:
                return f(**this_args)

        def __repr__(self):
            this_args = self.get_this_args()
            args = ['%s=%s' % i for i in this_args.iteritems()]
            args = [e if len(e) < 20 else e[:17] + '...' for e in args]
            return '%s(%s)' % (out_name or f.__name__, ', '.join(args))

        cls_attrs = attrs.copy()
        cls_attrs['func'] = staticmethod(f)
        cls_attrs['_apply'] = _apply
        cls_attrs['__repr__'] = __repr__
        cls_attrs['get_this_args'] = get_this_args
        cls = type(out_name or f.__name__, (out_type,), cls_attrs)
        if is_method:
            def wrapped(*args, **kwargs):
                return cls(*args, **kwargs)
            return wrapped
        else:
            return cls
    return decorator


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
        pos2name = {attr_type.pos: attr_name for attr_name, attr_type in fields.iteritems() if attr_type.pos is not None}
        if len(pos2name) == 0: max_nargs = 0
        else: max_nargs = max(pos2name) + 1 + len([attr_type for attr_type in fields.itervalues() if attr_type.pos is None])
        assert len(args) <= max_nargs

        for i, arg in enumerate(args):
            kwargs[pos2name[i]] = arg

        for attr, attr_type in fields.iteritems():
            if attr_type.default is not _no_default and attr not in kwargs:
                kwargs[attr] = attr_type.default

        if len(kwargs) > len(fields):
            raise ValueError("Class %s does not take the following arguments: %s" % (type(self).__name__, ", ".join(f for f in kwargs if f not in fields)))
        elif len(kwargs) < len(fields) :
            raise ValueError("Missing arguments for class %s: %s" % (type(self).__name__, ", ".join(f for f in fields if f not in kwargs)))

        for attr, attr_type in fields.iteritems():
            assert not isinstance(attr_type, OperationField) or isinstance(kwargs[attr], Operation), "Parameter %s should be an Operation" % attr
            # if isinstance(attr_type, OperationCollection):
            #     for k, v in general_iterator(kwargs[attr]):
            #         assert isinstance(v, Operation), 'Collection %s should contain Operations' % attr

        for attr, attr_type in kwargs.iteritems():
            setattr(self, attr, attr_type)

        self._involved_series = None

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

    def resample(self, rule='D', how='mean'):
        return ResampleOperation(operation=self, rule=rule, how=how)

    def __getitem__(self, operation):
        return ConditionedOperation(main_operation=self, cond_operation=operation)

    def __and__(self, operation):
        return AndOperation(operation1=self, operation2=operation)

    def __or__(self, operation):
        return OrOperation(operation1=self, operation2=operation)

    def get_suboperations(self):
        res = {}
        for attr, attr_type in type(self).get_fields():
            if isinstance(attr_type, OperationField):
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
    def dict2operation(dict):
        for cls in Operation._get_all_subclasses():
            if cls.__name__ == dict['type']: break
        else:
            raise ValueError('Unknown operation type')

        return cls._from_dict(dict)

    @staticmethod
    def key2operation(str):
        if str.startswith('/'): str = str[1:]
        kwargs = Operation.__key2dict(json.loads(str))
        return Operation.dict2operation(kwargs)

    def involved_series(self):
        if self._involved_series is not None: return self._involved_series

        if not self.is_get:
            res = set()
            for k, field_type in type(self).get_fields():
                if not isinstance(field_type, OperationField): continue

                v = getattr(self, k)
                res.update(v.involved_series())
        else:
            return [self.series_name]

        res = sorted(res)
        self._involved_series = res
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
    series_name = PrimitiveField(0)

    def _apply(self, data_store):
        return data_store.get(self.series_name)

    def __compare(self, op, scalar):
        return BinaryOperation(operation=self, op_name=op, val=scalar)

    def __eq__(self, scalar):
        #XXX que onda cuando metemos esto en el hashmap
        return self.__compare('=', scalar)

    def __le__(self, scalar):
        return self.__compare('<=', scalar)

    def __ge__(self, scalar):
        return self.__compare('>=', scalar)

    def __lt__(self, scalar):
        return self.__compare('<', scalar)

    def __gt__(self, scalar):
        return self.__compare('>', scalar)

    def __ne__(self, scalar):
        return self.__compare('!=', scalar)

    def __repr__(self):
        return 'GO(%s)' % try_encode(self.series_name)


class ResampleOperation(Operation):
    operation = OperationField()
    how = PrimitiveField()
    rule = PrimitiveField()

    def _apply(self, data_store):
        series = data_store.execute(self.operation)
        if series.dtype == object:
            series = series.dropna().convert_objects()

        return series.resample(self.rule, how=self.how)

    def __repr__(self):
        return '(%s).resample(how=%s, rule=%s)' % (self.operation, try_encode(self.how), try_encode(self.rule))


class ConditionedOperation(Operation):
    main_operation = OperationField()
    cond_operation = OperationField()

    def _apply(self, data_store):
        return data_store.execute(self.main_operation)[data_store.execute(self.cond_operation)]

    def __repr__(self):
        return '(%s)[%s]' % (self.main_operation, self.cond_operation)


class AndOperation(Operation):
    operation1 = OperationField()
    operation2 = OperationField()

    def _apply(self, data_store):
        return data_store.execute(self.operation1) & data_store.execute(self.operation2)

    def __repr__(self):
        return '%s & %s' % (self.operation1, self.operation2)

class OrOperation(Operation):
    operation1 = OperationField()
    operation2 = OperationField()

    def _apply(self, data_store):
        return data_store.execute(self.operation1) | data_store.execute(self.operation2)

    def __repr__(self):
        return '%s | %s' % (self.operation1, self.operation2)


class IntervalOperation(Operation):
    operation = OperationField(0)
    lbound = PrimitiveField(1)
    ubound = PrimitiveField(2)

    def _apply(self, data_store):
        return (data_store.execute(self.operation) >= self.lbound) & (data_store.execute(self.operation) <= self.ubound)

    def __repr__(self):
        return '%s in [%s-%s]' % (self.operation, self.lbound, self.ubound)


class BinaryOperation(Operation):

    ops = {'=': operator.eq,
           '>=': operator.ge,
           '<=': operator.le,
           '>': operator.gt,
           '<': operator.lt,
           '!=': operator.ne}

    operation = OperationField()
    op_name = PrimitiveField()
    val = PrimitiveField()

    def _apply(self, data_store):
        if np.isscalar(self.val) and pd.isnull(self.val) and self.op_name in ('=', '!='):
            res = pd.isnull(data_store.execute(self.operation))
            if self.op_name == '!=':
                res = ~res
            return res
        else:
            op = self.ops[self.op_name]
            return op(data_store.execute(self.operation), self.val)

    def __repr__(self):
        return '%s %s %s' % (self.operation, try_encode(self.op_name), try_encode(self.val))


def try_encode(s):
    return s.encode('utf8') if isinstance(s, unicode) else s