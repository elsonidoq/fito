from base import BaseDataStore

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


