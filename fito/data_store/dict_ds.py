from fito.data_store.base import BaseDataStore


class DictDataStore(BaseDataStore):
    def __init__(self, *args, **kwargs):
        super(DictDataStore, self).__init__(*args, **kwargs)
        self.data = {}

    def iteritems(self):
        return self.data.iteritems()

    def save(self, name_or_operation, object):
        self.data[name_or_operation] = object

    def _get(self, name_or_operation):
        if name_or_operation not in self.data: raise KeyError("Spec not found: {}".format(name_or_operation))

        return self.data.get(name_or_operation)

    def iterkeys(self):
        return self.data.iterkeys()
