from collections import OrderedDict

from fito import PrimitiveField
from fito import Spec
from fito.specs.fields import NumericField


class OperationRunner(Spec):
    cache_size = NumericField(default=0)
    verbose = PrimitiveField(default=False)

    def __init__(self, *args, **kwargs):
        super(OperationRunner, self).__init__(*args, **kwargs)
        if self.cache_size == 0:
            self.cache = None
        else:
            self.cache = FifoCache(self.cache_size, self.verbose)

    # TODO: The FifoCache can be casted into a FifoDataStore, and make this function an @autosave
    def execute(self, operation):
        """
        Executes an operation using this data store as input
        If this data store was configured to use an execute cache, it will be used
        """

        functions = [
            lambda: self._get_memory_cache(operation),
            lambda: self._get_data_store_cache(operation),
            lambda: operation.apply(self)
        ]
        for func in functions:
            res = func()
            if res is not None: break

        if self.cache is not None:
            self.cache.set(operation, res)

        out_data_store = operation.get_out_data_store()
        if out_data_store is not None:
            out_data_store[operation] = res

        return res

    def _get_memory_cache(self, operation):
        if self.cache is not None:
            return self.cache.get(operation)

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

    def get(self, spec):
        if spec in self.queue:
            if self.verbose: print "Fifo hit!"
            res = self.queue.pop(spec)
            self.queue[spec] = res
            return res

    def set(self, spec, value):
        self.queue.pop(spec, None)
        if len(self.queue) >= self.size:
            if self.verbose: print "Fifo pop!"
            op, _ = self.queue.popitem(False)
        self.queue[spec] = value

    def __getitem__(self, spec):
        return self.queue[spec]
