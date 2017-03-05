import warnings
from functools import wraps

from fito import Spec
from fito.operation_runner import FifoCache, OperationRunner
from fito.operations.decorate import as_operation
from fito.specs.base import get_import_path
from fito.specs.fields import NumericField, PrimitiveField


class BaseDataStore(OperationRunner):
    """
    Base class for all data stores, to implement a backend you need to implement
    _get, save and iteritems methods

    The _get is the actual get procedure, the caching strategy is part of the DataStore implementation

    """

    get_cache_size = NumericField(default=0)
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

    def get(self, spec):
        """
        Gets an operation from this data store.
        If you provide a string, it is assumed to be a `Get`
        """
        if self.get_cache is None:
            return self._get(spec)
        else:
            try:
                return self.get_cache[spec]
            except KeyError:
                res = self._get(spec)
                self.get_cache.set(spec, res)
                return res

    def _get(self, spec):
        """
        Abstract method, actual implementation of the fetch from the data_store
        """
        raise NotImplementedError()

    def get_by_id(self, id):
        """
        Fetches the value given some id. The id is implementation specific
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

    def iterkeys(self, raw=False):
        """
        Iterates over the keys of the data store
        :param raw: Whether to return raw documents or specs
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

    def autosave(self, *args, **kwargs):
        kwargs['cache_on'] = self
        return AutosavedFunction(*args, **kwargs)

    def refactor(self, refactor_operation, out_data_store, permissive=False):
        for id, doc in self.iterkeys(raw=True):
            try:
                refactored_doc = refactor_operation.bind(doc=doc).execute()
                spec = Spec.dict2spec(refactored_doc)
                out_data_store[spec] = self.get_by_id(id)
            except Exception, e:
                if permissive:
                    warnings.warn(' '.join(e.args))
                else:
                    raise e


class AutosavedFunction(as_operation):
    cache_on = PrimitiveField()  # make cache_on a required parameter

    def create_decorated(self, to_wrap, func_to_execute, f_spec=None, first_arg=None):
        OperationClass = super(AutosavedFunction, self).create_decorated(
            to_wrap, func_to_execute, f_spec=f_spec, first_arg=first_arg
        )

        class AutosavedOperation(OperationClass):
            def to_dict(self, include_all=False):
                res = super(AutosavedOperation, self).to_dict(include_all=include_all)

                if first_arg is not None:
                    res['type'] = get_import_path(first_arg, func_to_execute.__name__, 'operation_class')
                else:
                    res['type'] = get_import_path(func_to_execute, 'operation_class')

                return res

        class FunctionWrapper(object):
            @property
            def wrapped_function(self):
                return to_wrap

            @property
            def operation_class(self):
                return AutosavedOperation

            @wraps(to_wrap)
            def __call__(_, *args, **kwargs):
                return self.cache_on.execute(AutosavedOperation(*args, **kwargs))

        return FunctionWrapper()
