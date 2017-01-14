from functools import wraps

from fito import Operation
from fito import PrimitiveField
from fito import Spec
from fito import SpecField
from fito.operation_runner import FifoCache, OperationRunner
from fito.operations.decorate import GenericDecorator, operation_from_func
from fito.specs.base import NumericField


class BaseDataStore(Spec):
    """
    Base class for all data stores, to implement a backend you need to implement
    _get, save and iteritems methods

    The _get is the actual get procedure, the caching strategy is part of the DataStore implementation

    """

    get_cache_size = NumericField(default=0)
    operation_runner = SpecField(default=OperationRunner())

    def __init__(self, *args, **kwargs):
        """
        Instances the data store.

        :param get_cache_size: Size of the FIFO cache for serialization
        """
        super(BaseDataStore, self).__init__(*args, **kwargs)
        if self.get_cache_size > 0:
            self.get_cache = FifoCache(self.get_cache_size)
        else:
            self.get_cache = None

        self.operation_runner = self.operation_runner or OperationRunner()

    def get(self, name_or_spec):
        """
        Gets an operation from this data store.
        If you provide a string, it is assumed to be a `Get`
        """
        if self.get_cache is None:
            return self._get(name_or_spec)
        else:
            res = self.get_cache.get(name_or_spec)
            if res is FifoCache.no_result:
                res = self._get(name_or_spec)
                self.get_cache.set(name_or_spec, res)
            return res

    def _get(self, name_or_spec):
        """
        Abstract method, actual implementation of the fetch from the data_store
        """
        raise NotImplementedError()

    def save(self, name_or_spec, object):
        """
        Actual implementation that saves an object associated with the name or operation
        """
        raise NotImplementedError()

    def iteritems(self):
        """
        Iterates over the datastore
        :return: An iterator over (operation, object) pairs
        """
        raise NotImplementedError()

    def __getitem__(self, name_or_spec):
        return self.get(name_or_spec)

    def __setitem__(self, name_or_spec, object):
        self.save(name_or_spec, object)

    def get_or_none(self, name_or_spec):
        try:
            return self.get(name_or_spec)
        except KeyError:
            return None

    def __contains__(self, name_or_spec):
        return self.get_or_none(name_or_spec) is not None

    def _get_spec(self, name_or_spec):
        if isinstance(name_or_spec, basestring) or isinstance(name_or_spec, int):
            return Get(name=name_or_spec, input=self)
        elif isinstance(name_or_spec, Spec):
            return name_or_spec
        else:
            raise ValueError("Can not convert {} to a Spec".format(name_or_spec))

    def _get_key(self, name_or_spec):
        operation = self._get_spec(name_or_spec)
        if isinstance(operation, Get):
            key = operation.name
        else:
            key = operation.key
        return key

    def get_or_execute(self, operation):
        """
        Base function for all autocaching

        :param operation:
        :return:
        """
        if operation not in self:
            res = self.operation_runner.execute(operation)
            self[operation] = res
        else:
            res = self.get(operation)
        return res

    def cache(self, **kwargs):
        """
        Decorates a function, instance method or class method for it to be autosaved on this data store

        See `GenericDecorator` for reference regarding available options for kwargs
        """
        kwargs['data_store'] = self
        return AutosavedFunction(**kwargs)

    def autosave(self, OperationClass):
        """
        Creates an automatic cache for a given OperationClass (i.e. a class whose parent is Spec):

        The returned function receives the same arguments that `OperationClass` declared, and uses the `self` to save the results

        :param OperationClass: A class whose parent is Spec
        """
        assert issubclass(OperationClass, Spec)

        def autosaved(*args, **kwargs):
            operation = OperationClass(*args, **kwargs)
            return self.get_or_execute(operation)
        return autosaved


class AutosavedFunction(GenericDecorator):
    def __init__(self, **kwargs):
        """
        For available arguments see `as_operation` decorator
        """
        self.data_store = kwargs.pop('data_store')
        self.out_type = kwargs.pop('out_type', Spec)
        self.out_name = kwargs.pop('out_name', None)
        self.args_specifications = kwargs
        super(AutosavedFunction, self).__init__(**kwargs)

    def create_decorated(self, to_wrap, func_to_execute, f_spec=None):
        OperationClass = operation_from_func(
            to_wrap=to_wrap,
            func_to_execute=func_to_execute,
            out_type=self.out_type,
            out_name=self.out_name,
            args_specifications=self.args_specifications,
            f_spec=f_spec,
            method_type=self.method_type
        )

        class FunctionWrapper(object):
            # just to make it declarative each time it is used
            def register_operation(self):
                pass

            @property
            def wrapped_function(self):
                return to_wrap

            @property
            def operation_class(self):
                return OperationClass

            @wraps(to_wrap)
            def __call__(_, *args, **kwargs):
                func = self.data_store.autosave(OperationClass)
                return func(*args, **kwargs)

        return FunctionWrapper()


class Get(Operation):
    name = PrimitiveField(0, base_type=basestring)
    input = SpecField(1, base_type=BaseDataStore)

    def apply(self, runner):
        return self.input[self]

    def __repr__(self):
        return '{}'.format(self.name)
