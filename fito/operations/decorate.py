from __future__ import print_function

import inspect
from functools import wraps

from fito.operations.operation import Operation
from fito.specs.base import PrimitiveField, BaseSpecField, Spec, KwargsField, SpecField, get_import_path

try:
    import cPickle
except ImportError:
    import pickle as cPickle


class GenericDecorator(Spec):
    """
    Abstracts all the boilerplate required to build a decorator that works on functions, instance methods and class methods


    :param method_type: if is None, the decorated function is assumed to be a function, otherwise it is assumed
        to be a method. If method_type == 'instance' the function is assumed to be an instance method otherwise a
        classmethod
    """
    method_type = PrimitiveField(0, default=None)

    def __get__(self, instance, owner):
        if (instance is None and self.method_type == 'instance') or \
                (owner is None and self.method_type == 'class'):
            return self

        first_arg = instance if self.method_type == 'instance' else owner

        @wraps(self.func)
        def new_f(*args, **kwargs):
            return self.func(first_arg, *args, **kwargs)

        cls = type(instance) if instance is not None else owner
        assert cls is not None
        return self.create_decorated(self.func, new_f, inspect.getargspec(self.func), first_arg=first_arg)

    def __call__(self, func):
        if self.method_type:
            self.func = func
            return self
        else:
            return self.create_decorated(func, func)

    def create_decorated(self, to_wrap, func_to_execute, f_spec=None, first_arg=None):
        """
        Abstract method that should be implemented in order to build a decorator

        The difference between `to_wrap` and `func_to_execute` is the fact that in the case of instance methods
        and class methods, `func_to_execute` has the first argument already binded.
        If `to_wrap` is just a function, then `to_wrap == func_to_execute`

        :param to_wrap: Original wrapped function
        :param func_to_execute: You should execute this function
        :param f_spec: The argspec of the function to be decorated, if None, it should be computed from to_wrap (TODO: remove this argument)
        :param first_arg: `self` if it's an instance method, `cls` if it's a classmethod, None otherwise

        """
        raise NotImplementedError()


class as_operation(GenericDecorator):
    """
    Creates an operation from a callable
    :param out_type: Base class of the operation to be built. Defaults to `Operation`
    :param out_name: Name of the class to be built, deafults to the decorated function name.
    """
    method_type = PrimitiveField(default=None)
    out_type = PrimitiveField(default=Operation)
    out_name = PrimitiveField(default=None)
    args_specifications = KwargsField()

    def create_decorated(self, to_wrap, func_to_execute, f_spec=None, first_arg=None):
        f_spec = f_spec or inspect.getargspec(to_wrap)
        return operation_from_func(
            to_wrap=to_wrap,
            func_to_execute=func_to_execute,
            out_type=self.out_type,
            out_name=self.out_name,
            args_specifications=self.args_specifications,
            f_spec=f_spec,
            method_type=self.method_type,
            first_arg=first_arg
        )

def operation_from_func(to_wrap, func_to_execute, out_type, out_name, args_specifications, f_spec=None,
                        method_type=None, first_arg=None):
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
            spec = args_specifications[arg]
            if inspect.isclass(spec) and issubclass(spec, Spec):
                spec = SpecField(base_type=spec)
            # It can be either a class, or the instance itself
            if inspect.isclass(spec) or inspect.isfunction(spec): spec = spec()

            spec.pos = len(attrs)
        else:
            spec = PrimitiveField(len(attrs))
        if arg in default_values: spec.default = default_values[arg]
        attrs[arg] = spec

    def get_this_args(self, runner=None):
        this_args = {}
        for k, v in attrs.iteritems():
            value = getattr(self, k)
            if isinstance(v, BaseSpecField) and runner is not None and isinstance(value, Operation):
                value = value.execute(runner)

            this_args[k] = value

        return this_args

    def to_dict(self):
        res = super(out_type, self).to_dict()
        res['type'] = get_import_path(func_to_execute)
        return res

    def apply(self, runner):
        this_args = self.get_this_args(runner)
        return func_to_execute(**this_args)

    def __repr__(self):
        this_args = self.get_this_args()
        args = ['%s=%s' % i for i in this_args.iteritems()]
        args = [e if len(e) < 20 else e[:17] + '...' for e in args]
        res = '%s(%s)' % (out_name or to_wrap.__name__, ', '.join(args))
        if first_arg is not None:
            if method_type == 'class':
                first_arg_name = first_arg.__name__
            else:
                first_arg_name = type(first_arg).__name__.lower()

            res = '{}.{}'.format(first_arg_name, res)
        return res

    cls_attrs = attrs.copy()
    cls_attrs['func'] = staticmethod(func_to_execute)
    cls_attrs['apply'] = apply
    cls_attrs['__repr__'] = __repr__
    cls_attrs['get_this_args'] = get_this_args
    cls_attrs['to_dict'] = to_dict

    out_name = out_name or to_wrap.__name__
    cls = Operation.type2spec_class(out_name)
    if cls is None:
        # if the class does not exist, create it
        cls = type(out_name, (out_type,), cls_attrs)
    else:
        # otherwise update it
        for k, v in cls_attrs.iteritems():
            setattr(cls, k, v)

    return cls
