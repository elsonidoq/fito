from collections import OrderedDict


class OperationRunner(object):
    def __init__(self, cache_size=0, verbose=False):
        if cache_size == 0:
            self.cache = None
        else:
            self.cache = FifoCache(cache_size, verbose)

    def execute(self, operation):
        """
        Executes an operation using this data store as input
        If this data store was configured to use an execute cache, it will be used
        """
        if self.cache is None:
            return operation.apply(self)
        else:
            res = self.cache.get(operation)
            if res is self.cache.no_result:
                res = operation.apply(self)
                self.cache.set(operation, res)
            return res


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
        if len(self.queue) >= self.size:
            if self.verbose: print "Fifo pop!"
            op, _ = self.queue.popitem(False)
        self.queue[operation] = value


