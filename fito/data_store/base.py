from inspect import isclass
from collections import defaultdict, OrderedDict

from fito.operations import GetOperation, Operation


class FifoCache(object):
    def __init__(self, size=500):
        self.queue = OrderedDict()
        self.size = size

    def get(self, operation):
        if operation in self.queue:
            res = self.queue.pop(operation)
            self.queue[operation] = res
            return res

    def set(self, operation, value):
        if isinstance(operation, basestring) or operation.is_get:
            return
        if len(self.queue) > self.size:
            op, _ = self.queue.popitem(False)
        self.queue[operation] = value


class BaseDataStore(object):
    def __init__(self, get_cache_size=0, execute_cache_size=0):
        if get_cache_size > 0:
            self.get_cache = FifoCache(get_cache_size)
        else:
            self.get_cache = None

        if execute_cache_size > 0:
            self.execute_cache = FifoCache(execute_cache_size)
        else:
            self.execute_cache = None

    def get(self, series_name_or_operation):
        if self.get_cache is None:
            return self._get(series_name_or_operation)
        else:
            res = self.get_cache.get(series_name_or_operation)
            if res is None:
                res = self._get(series_name_or_operation)
                self.get_cache.set(series_name_or_operation, res)
            return res

    def _get(self, series_name_or_operation):
        raise NotImplementedError()

    def save(self, series_name_or_operation, series):
        raise NotImplementedError()

    def __getitem__(self, series_name_or_operation):
        return self.get(series_name_or_operation)

    def __setitem__(self, series_name_or_operation, series):
        self.save(series_name_or_operation, series)

    def get_or_none(self, series_name_or_operation):
        try:
            return self.get(series_name_or_operation)
        except ValueError:
            return None

    def get_or_execute(self, series_name_or_operation):
        op = self._get_operation(series_name_or_operation)
        if op in self:
            res = self[op]
        else:
            res = self.execute(op)
        return res

    def __contains__(self, series_name_or_operation):
        return self.get_or_none(series_name_or_operation) is not None

    def search(self, query):
        raise NotImplementedError()

    def iteritems(self):
        raise NotImplementedError()

    @classmethod
    def _get_operation(cls, series_name_or_operation):
        if isinstance(series_name_or_operation, basestring):
            return GetOperation(series_name=series_name_or_operation)
        elif isinstance(series_name_or_operation, Operation):
            return series_name_or_operation
        else:
            raise ValueError("invalid argument")

    @classmethod
    def _get_key(cls, series_name_or_operation):
        operation = cls._get_operation(series_name_or_operation)
        if isinstance(operation, GetOperation):
            key = operation.series_name
        else:
            key = operation.key
        return key

    def execute(self, operation):
        if self.execute_cache is None:
            return self._execute(operation)
        else:
            res = self.execute_cache.get(operation)
            if res is None:
                res = self._execute(operation)
                self.execute_cache.set(operation, res)
            return res

    def _execute(self, operation):
        return operation.apply(self)


class StorageManager(BaseDataStore):
    def __init__(self, get_cache_size=0, execute_cache_size=0):
        super(StorageManager, self).__init__(get_cache_size, execute_cache_size)
        self.inputs = []
        self.outputs = []

    def iteritems(self):
        for _, ds, _ in self.outputs:
            for op, v in ds.iteritems():
                yield op, v

    def set_input(self, cls, ds):
        self.inputs.append((cls, ds))

    def set_output(self, cls, ds, autosave=False):
        self.outputs.append((cls, ds, autosave))

    def set_input_output(self, cls, ds, autosave=False):
        self.set_input(cls, ds)
        self.set_output(cls, ds, autosave=autosave)

    def _execute(self, operation):
        operation = self._get_operation(operation)
        in_ds = self._get_input_store(operation)
        if in_ds is None: raise ValueError("input store not found for operation %s" % operation)

        if in_ds.execute_cache is None:
            res = operation.apply(self)
        else:
            res = in_ds.execute_cache.get(operation)
            if res is None:
                res = operation.apply(self)
                in_ds.execute_cache.set(operation, res)

        out_ds, autosave = self._get_output_store(operation)
        if autosave:
            out_ds[operation] = res

        return res

    def __contains__(self, operation):
        operation = self._get_operation(operation)
        out_ds, autosave = self._get_output_store(operation)
        if out_ds is None: return False  # raise ValueError("output store not found for operation %s" % operation)
        return operation in out_ds

    def _get_store(self, operation, list):
        for elem in list:
            if isinstance(operation, elem[0]): return elem

    def _get_input_store(self, operation):
        res = self._get_store(operation, self.inputs)
        if res is not None: return res[1]

    def _get_output_store(self, operation):
        res = self._get_store(operation, self.outputs)
        if res is not None:
            return res[1:]
        else:
            return None, False

    def _get(self, operation):
        operation = self._get_operation(operation)
        out_ds, autosave = self._get_output_store(operation)
        if out_ds is None: raise ValueError("input store not found for operation %s" % operation)
        return out_ds[operation]

    def save(self, operation, value):
        operation = self._get_operation(operation)
        out_ds, autosave = self._get_output_store(operation)
        if out_ds is None: raise ValueError("output store not found for operation %s" % operation)
        return out_ds.save(operation, value)


class infinitedict(defaultdict):
    def __init__(self):
        super(infinitedict, self).__init__(infinitedict)

    def todict(self):
        res = {}
        for k, v in self.iteritems():
            if isinstance(v, infinitedict):
                res[k] = v.todict()
            else:
                res[k] = v
        return res


class Query(object):
    def __init__(self, **kwargs):
        self.dict = kwargs

    def set(self, key, val):
        if isclass(val): val = val.__name__
        self.dict[key] = val
        return self

    def matches(self, operation):
        return self._matches(self.todict(), operation.to_dict())

    def _matches(self, query_dict, op_dict):
        for k, v1 in query_dict.iteritems():

            if k not in op_dict: return False
            v2 = op_dict[k]
            if isinstance(v2, dict) != isinstance(v1, dict): return False
            if isinstance(v2, dict) and isinstance(v1, dict):
                return self._matches(v1, v2)
            elif v1 != v2:
                return False
        return True

    def todict(self):
        res = infinitedict()
        for key, val in self.dict.iteritems():
            d = res
            subkeys = key.split('.')
            for subkey in subkeys[:-1]:
                d = d[subkey]

            key = subkeys[-1]
            if isclass(val) and issubclass(val, Operation):
                d[key]['type'] = val.__name__
            else:
                d[key] = val

        return res.todict()
