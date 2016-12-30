from collections import OrderedDict
from functools import wraps

from fito.operations import GetOperation, Operation
from fito.operations.decorate import GenericDecorator, operation_from_func


class FifoCache(object):
    """
    Fifo caching strategy
    It is useful when there are operations that are costly to execute and you might need the result
    near in the future for computing another operation
    """

    no_result = object()

    def __init__(self, size=500, verbose=False):
        self.verbose = verbose
        self.queue = OrderedDict()
        self.size = size

    def get(self, operation):
        if operation not in self.queue:
            return self.no_result

        if self.verbose: print "Fifo hit!"
        res = self.queue.pop(operation)
        self.queue[operation] = res
        return res

    def set(self, operation, value):
        if isinstance(operation, basestring) or operation.is_get:
            return
        if len(self.queue) > self.size:
            if self.verbose: print "Fifo pop!"
            op, _ = self.queue.popitem(False)
        self.queue[operation] = value


class BaseDataStore(object):
    """
    Base class for all data stores, to implement a backend you need to implement
    _get, save and iteritems methods

    The _get is the actual get procedure, the caching strategy is part of the DataStore implementation

    """

    def __init__(self, get_cache_size=0, execute_cache_size=0):
        """
        Instances the data store.

        It provides a FIFO caching option when the results of the operation take time to execute
        (i.e. the `_apply` method is costly) or the operation takes time to serialize.

        In order to use the execute cache, you have to do `data_store.execute(operation)` instead of
        `operation.apply(data_store)`

        :param get_cache_size: Size of the FIFO cache for serialization
        :param execute_cache_size: Size of FIFO cache for execution

        """
        if get_cache_size > 0:
            self.get_cache = FifoCache(get_cache_size)
        else:
            self.get_cache = None

        if execute_cache_size > 0:
            self.execute_cache = FifoCache(execute_cache_size)
        else:
            self.execute_cache = None

    def get(self, name_or_operation):
        """
        Gets an operation from this data store.
        If you provide a string, it is assumed to be a `GetOperation`
        """
        if self.get_cache is None:
            return self._get(name_or_operation)
        else:
            res = self.get_cache.get(name_or_operation)
            if res is None:
                res = self._get(name_or_operation)
                self.get_cache.set(name_or_operation, res)
            return res

    def _get(self, name_or_operation):
        """
        Abstract method, actual implementation of the fetch from the data_store
        """
        raise NotImplementedError()

    def save(self, name_or_operation, object):
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

    def __getitem__(self, name_or_operation):
        return self.get(name_or_operation)

    def __setitem__(self, name_or_operation, object):
        self.save(name_or_operation, object)

    def get_or_none(self, name_or_operation):
        try:
            return self.get(name_or_operation)
        except ValueError:
            return None

    def get_or_execute(self, name_or_operation):
        op = self._get_operation(name_or_operation)
        if op in self:
            res = self[op]
        else:
            res = self.execute(op)
        return res

    def __contains__(self, name_or_operation):
        return self.get_or_none(name_or_operation) is not None

    @classmethod
    def _get_operation(cls, name_or_operation):
        if isinstance(name_or_operation, basestring):
            return GetOperation(name=name_or_operation)
        elif isinstance(name_or_operation, Operation):
            return name_or_operation
        else:
            raise ValueError("invalid argument")

    @classmethod
    def _get_key(cls, name_or_operation):
        operation = cls._get_operation(name_or_operation)
        if isinstance(operation, GetOperation):
            key = operation.name
        else:
            key = operation.key
        return key

    def execute(self, operation):
        """
        Executes an operation using this data store as input
        If this data store was configured to use an execute cache, it will be used
        """
        if self.execute_cache is None:
            return self._execute(operation)
        else:
            res = self.execute_cache.get(operation)
            if res is self.execute_cache.no_result:
                res = self._execute(operation)
                self.execute_cache.set(operation, res)
            return res

    def _execute(self, operation):
        return operation.apply(self)

    def cache(self, **kwargs):
        """
        Decorates a function, instance method or class method for it to be autosaved on this data store

        See `GenericDecorator` for reference regarding available options for kwargs
        """
        kwargs['data_store'] = self
        return AutosavedFunction(**kwargs)

    def autosave(self, OperationClass):
        """
        Creates an automatic cache for a given OperationClass (i.e. a class whose parent is Operation):

        The returned function receives the same arguments that `OperationClass` declared, and uses the `self` to save the results

        :param OperationClass: A class whose parent is Operation
        """
        assert issubclass(OperationClass, Operation)

        def autosaved(*args, **kwargs):
            operation = OperationClass(*args, **kwargs)
            if operation not in self:
                res = self.execute(operation)
                self[operation] = res
            else:
                res = self.get(operation)
            return res

        return autosaved


class AutosavedFunction(GenericDecorator):
    def __init__(self, **kwargs):
        """
        For available arguments see `as_operation` decorator
        """
        self.data_store = kwargs.pop('data_store')
        self.out_type = kwargs.pop('out_type', Operation)
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

