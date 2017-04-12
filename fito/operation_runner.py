from collections import OrderedDict

from fito import PrimitiveField
from fito import Spec
from fito.specs.fields import NumericField


class OperationRunner(Spec):
    execute_cache_size = NumericField(default=0)
    verbose = PrimitiveField(default=False)

    # Whether to force execution and ignore caches
    # Helps encapsulate the behaviour so the Operation.apply remains simple
    force = PrimitiveField(serialize=False, default=False)

    def __init__(self, *args, **kwargs):
        super(OperationRunner, self).__init__(*args, **kwargs)
        if self.execute_cache_size == 0:
            self.execute_cache = None
        else:
            self.execute_cache = FifoCache(self.execute_cache_size, self.verbose)

    def alias(self, **kwargs):
        """
        Same as self.replace, but keeps the same execute_cache
        """
        res = self.replace(**kwargs)
        if res.execute_cache is not None:
            res.execute_cache = self.execute_cache
        return res

    # TODO: The FifoCache can be casted into a FifoDataStore, and make this function an @autosave
    def execute(self, operation, force=False):
        """
        Executes an operation using this data store as input
        If this data store was configured to use an execute cache, it will be used

        :param force: Whether to ignore the current cached value of this operation
        """
        force = force or self.force
        if not force:
            # if not force, then check the caches out
            functions = [
                lambda: self._get_memory_cache(operation),
                lambda: self._get_data_store_cache(operation),
            ]
        else:
            functions = []

        functions.append(
            lambda: operation.apply(
                self.alias(force=force)
            )
        )

        for func in functions:
            res = func()
            if res is not None: break

        if self.execute_cache is not None:
            self.execute_cache.set(operation, res)

        out_data_store = operation.get_out_data_store()
        if out_data_store is not None:
            out_data_store[operation] = res

        return res

    def _get_memory_cache(self, operation):
        if self.execute_cache is not None:
            return self.execute_cache.get(operation)

    def _get_data_store_cache(self, operation):
        out_data_store = operation.get_out_data_store()
        if out_data_store is not None:
            return out_data_store.get_or_none(operation)


class FifoCache(object):
    """
    Fifo caching strategy
    It is useful when there are operations that are costly to execute and you might need the result
    near in the future for computing another spec
    """

    def __init__(self, size=500, verbose=False):
        self.verbose = verbose
        self.queue = OrderedDict()
        self.size = size

    def _get_key(self, spec_or_dict):
        if isinstance(spec_or_dict, Spec):
            return spec_or_dict.key
        elif isinstance(spec_or_dict, dict):
            return Spec._dict2key(spec_or_dict)
        else:
            # assume it's an id
            return spec_or_dict

    def get(self, spec):
        key = self._get_key(spec)
        if key in self.queue:
            if self.verbose: print "Fifo hit!"
            res = self.queue.pop(key)
            self.queue[key] = res
            return res

    def set(self, spec, value):
        key = self._get_key(spec)
        self.queue.pop(key, None)
        if len(self.queue) >= self.size:
            if self.verbose: print "Fifo pop!"
            op, _ = self.queue.popitem(False)
        self.queue[key] = value

    def __getitem__(self, spec):
        return self.queue[self._get_key(spec)]

    def remove(self, spec):
        self.queue.pop(self._get_key(spec), None)
