from functools import wraps
import traceback
import warnings

from fito import Operation
from fito import PrimitiveField
from fito import Spec
from fito import SpecField
from fito.operation_runner import FifoCache, OperationRunner
from fito.operations.decorate import GenericDecorator, operation_from_func
from fito.specs.base import NumericField, KwargsField


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

    def get(self, spec):
        """
        Gets an operation from this data store.
        If you provide a string, it is assumed to be a `Get`
        """
        if self.get_cache is None:
            return self._get(spec)
        else:
            res = self.get_cache.get(spec)
            if res is FifoCache.no_result:
                res = self._get(spec)
                self.get_cache.set(spec, res)
            return res

    def _get(self, spec):
        """
        Abstract method, actual implementation of the fetch from the data_store
        """
        raise NotImplementedError()

    def save(self, spec, object):
        """
        Actual implementation that saves an object associated with the id or operation
        """
        raise NotImplementedError()

    def iteritems(self):
        """
        Iterates over the datastore
        :return: An iterator over (operation, object) pairs
        """
        raise NotImplementedError()

    def __getitem__(self, spec):
        return self.get(spec)

    def __setitem__(self, spec, object):
        self.save(spec, object)

    def get_or_none(self, spec):
        try:
            return self.get(spec)
        except KeyError:
            return None

    def __contains__(self, spec):
        return self.get_or_none(spec) is not None

    def get_or_execute(self, operation, operation_runner=None):
        """
        Base function for all autocaching

        :param operation:
        :return:
        """
        if operation not in self:
            res = (operation_runner or self.operation_runner).execute(operation)
            self[operation] = res
        else:
            try:
                res = self.get(operation)
            except Exception, e:
                warnings.warn("There was an error loading from cache, executing again...")
                traceback.print_exc()

                res = (operation_runner or self.operation_runner).execute(operation)
                self[operation] = res

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
    data_store = PrimitiveField()
    out_type = PrimitiveField(default=Operation)
    out_name = PrimitiveField(default=None)
    args_specifications = KwargsField()

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

        # This operation will autosave to self.data_store
        OperationClass.out_data_store.default = self.data_store

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
