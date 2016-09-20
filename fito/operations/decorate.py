from __future__ import print_function

from functools import wraps
from glob import glob
import traceback
import inspect
import os

from fito.operations import Operation
from fito.operations.base import PrimitiveField, OperationField

try:
    import cPickle
except ImportError:
    import pickle as cPickle
from filelock import FileLock

caches_dir = os.path.join(os.path.dirname(__file__), 'caches')
if not os.path.exists(caches_dir): os.makedirs(caches_dir)
VERBOSE = False


class as_operation(object):
    data = {}

    def __init__(self, **kwargs):
        """
        :param method_type: if is None, the decorated function is assumed to be a function, otherwise it is assumed
            to be a method. If method_type == 'instance' the function is assumed to be an instance method otherwise a
            classmethod
        """
        self.method_type = kwargs.pop('method_type', None)
        self.out_type = kwargs.pop('out_type', Operation)
        self.out_name = kwargs.pop('out_name', None)
        self.args_specifications = kwargs

    def __get__(self, instance, owner):
        if (instance is None and self.method_type == 'instance') or \
                (owner is None and self.method_type == 'class'):
            return self

        first_arg = instance if self.method_type == 'instance' else owner

        def new_f(*args, **kwargs):
            return self.func(first_arg, *args, **kwargs)

        cls = type(instance) if instance is not None else owner
        assert cls is not None
        return operation_from_func(
            f=new_f,
            out_type=self.out_type,
            out_name=self.out_name,
            args_specifications=self.args_specifications,
            f_spec=inspect.getargspec(self.func),
            method_type=self.method_type
        )

    def __call__(self, func):
        if self.method_type:
            self.func = func
            return self
        else:
            return operation_from_func(
                f=func,
                out_type=self.out_type,
                out_name=self.out_name,
                args_specifications=self.args_specifications
            )


def operation_from_func(f, out_type, out_name, args_specifications, f_spec=None, method_type=False):
    f_spec = f_spec or inspect.getargspec(f)
    if f_spec.defaults is None:
        default_values = {}
    else:
        args_with_defaults = f_spec.args[-len(f_spec.defaults):]
        default_values = dict(zip(args_with_defaults, f_spec.defaults))

    attrs = {}
    for i, arg in enumerate(f_spec.args):
        if method_type == 'instance' and arg == 'self': continue
        if method_type == 'class' and arg == 'cls': continue

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

    def _apply(self, data_store=None):
        this_args = self.get_this_args(data_store)
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
    return cls


class CachedFunc(object):
    memory_cache = {}

    def __init__(self, func, cache_fname):
        self.func = func
        self.cache_fname = cache_fname

    def __call__(self, *args, **kwargs):
        self.try_to_load()
        if self.in_memory:
            if VERBOSE: print("memory hit")
            result = CachedFunc.memory_cache[self.cache_fname]
        else:
            lock = FileLock(self.cache_fname + '.lock')
            print("acquiring lock for %s" % os.path.basename(self.cache_fname))
            with lock:
                self.try_to_load()
                if self.in_memory:
                    if VERBOSE: print("memory hit")
                    result = CachedFunc.memory_cache[self.cache_fname]
                else:
                    print("building for %s..." % os.path.basename(self.cache_fname))
                    result = CachedFunc.memory_cache[self.cache_fname] = self.func(*args, **kwargs)
                    self.save(result)

        return result

    @property
    def in_memory(self):
        return self.cache_fname in CachedFunc.memory_cache

    def try_to_load(self):
        if self.in_memory: return
        if not os.path.exists(self.cache_fname): return

        try:
            with open(self.cache_fname) as f:
                CachedFunc.memory_cache[self.cache_fname] = cPickle.load(f)
                if True or VERBOSE: print("\tloaded for %s" % os.path.basename(self.cache_fname))
        except Exception:
            print("Exception while loading %s" % self.cache_fname)
            traceback.print_exc()
            print("Deleting file")
            if os.path.exists(self.cache_fname): os.unlink(self.cache_fname)

    def save(self, data):
        with open(self.cache_fname, 'w') as f:
            cPickle.dump(data, f, 2)

    def clear_memory_cache(self):
        CachedFunc.memory_cache.pop(self.cache_fname, None)

    def clear_disk_cache(self):
        self.clear_memory_cache()
        if os.path.exists(self.cache_fname):
            os.unlink(self.cache_fname)
