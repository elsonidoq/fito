from __future__ import print_function

import inspect
import os

from fito.operations import Operation
from fito.operations.base import PrimitiveField, OperationField

try:
    import cPickle
except ImportError:
    import pickle as cPickle


class GenericDecorator(object):
    def __init__(self, **kwargs):
        """
        Abstracts all the boilerplate required to build a decorator that works on functions, instance methods and class methods


        :param method_type: if is None, the decorated function is assumed to be a function, otherwise it is assumed
            to be a method. If method_type == 'instance' the function is assumed to be an instance method otherwise a
            classmethod
        """
        self.method_type = kwargs.pop('method_type', None)

    def __get__(self, instance, owner):
        if (instance is None and self.method_type == 'instance') or \
                (owner is None and self.method_type == 'class'):
            return self

        first_arg = instance if self.method_type == 'instance' else owner

        def new_f(*args, **kwargs):
            return self.func(first_arg, *args, **kwargs)

        cls = type(instance) if instance is not None else owner
        assert cls is not None
        return self.create_decorated(self.func, new_f, inspect.getargspec(self.func))

    def __call__(self, func):
        if self.method_type:
            self.func = func
            return self
        else:
            return self.create_decorated(func, func)

    def create_decorated(self, to_wrap, func_to_execute, f_spec=None):
        """
        Abstract method that should be implemented in order to build a decorator

        The difference between `to_wrap` and `func_to_execute` is the fact that in the case of instance methods
        and class methods, `func_to_execute` has the first argument already binded.
        If `to_wrap` is just a function, then `to_wrap == func_to_execute`

        :param to_wrap: Original wrapped function
        :param func_to_execute: You should execute this function
        :param f_spec: The argspec of the function to be decorated, if None, it should be computed from to_wrap (TODO: remove this argument)

        """
        raise NotImplementedError()


class as_operation(GenericDecorator):
    """
    Creates an operation from a callable
    """
    def __init__(self, **kwargs):
        """
        :param out_type: Base class of the operation to be built. Defaults to `Operation`
        :param out_name: Name of the class to be built, deafults to the decorated function name.
        """
        self.out_type = kwargs.pop('out_type', Operation)
        self.out_name = kwargs.pop('out_name', None)
        self.args_specifications = kwargs
        super(as_operation, self).__init__(**kwargs)

    def create_decorated(self, to_wrap, func_to_execute, f_spec=None):
        f_spec = f_spec or inspect.getargspec(to_wrap)
        return operation_from_func(
            to_wrap=to_wrap,
            func_to_execute=func_to_execute,
            out_type=self.out_type,
            out_name=self.out_name,
            args_specifications=self.args_specifications,
            f_spec=f_spec,
            method_type=self.method_type
        )


def operation_from_func(to_wrap, func_to_execute, out_type, out_name, args_specifications, f_spec=None, method_type=False):
    """
    In the case of methods, to_wrap is not the same to func_to_execute
    :param to_wrap: See `GenericDecorator.create_decorated` for an explanation
    :param func_to_execute: See `GenericDecorator.create_decorated` for an explanation
    :return:
    """
    f_spec = f_spec or inspect.getargspec(to_wrap)
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
        return func_to_execute(**this_args)

    def __repr__(self):
        this_args = self.get_this_args()
        args = ['%s=%s' % i for i in this_args.iteritems()]
        args = [e if len(e) < 20 else e[:17] + '...' for e in args]
        return '%s(%s)' % (out_name or to_wrap.__name__, ', '.join(args))

    cls_attrs = attrs.copy()
    cls_attrs['func'] = staticmethod(func_to_execute)
    cls_attrs['_apply'] = _apply
    cls_attrs['__repr__'] = __repr__
    cls_attrs['get_this_args'] = get_this_args

    out_name = out_name or to_wrap.__name__
    cls = Operation.type2operation_class(out_name)
    if cls is None:
        # if the class does not exist, create it
        cls = type(out_name, (out_type,), cls_attrs)
    else:
        # otherwise update it
        for k, v in cls_attrs.iteritems():
            setattr(cls, k, v)



    return cls
