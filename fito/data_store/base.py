import traceback
import warnings
from functools import wraps

from fito import Operation
from fito import Spec
from fito import SpecField
from fito.operation_runner import FifoCache, OperationRunner
from fito.operations.decorate import GenericDecorator, as_operation, operation_from_func
from fito.specs.base import NumericField, PrimitiveField


class BaseDataStore(Spec):
    """
    Base class for all data stores, to implement a backend you need to implement
    _get, save and iteritems methods

    The _get is the actual get procedure, the caching strategy is part of the DataStore implementation

    """

    get_cache_size = NumericField(default=0)
    operation_runner = SpecField(default=OperationRunner())
    verbose = PrimitiveField(default=False, serialize=False)

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
                if self.verbose:
                    print "Getting cached version or {}".format(operation)
            except Exception, e:
                warnings.warn("There was an error loading from cache, executing again...")
                traceback.print_exc()

                res = (operation_runner or self.operation_runner).execute(operation)
                self[operation] = res

        return res

    def autosave(self, *args, **kwargs):
        kwargs['cache_on'] = self
        return AutosavedFunction(*args, **kwargs)


class AutosavedFunction(as_operation):
    def create_decorated(self, to_wrap, func_to_execute, f_spec=None, first_arg=None):
        OperationClass = super(AutosavedFunction, self).create_decorated(
            to_wrap, func_to_execute, f_spec=f_spec, first_arg=first_arg
        )

        class FunctionWrapper(object):
            @property
            def wrapped_function(self):
                return to_wrap

            @property
            def operation_class(self):
                return OperationClass

            @wraps(to_wrap)
            def __call__(_, *args, **kwargs):
                return OperationClass(*args, **kwargs).execute()

        return FunctionWrapper()
