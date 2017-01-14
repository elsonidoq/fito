from fito.data_store.base import BaseDataStore


class DictDataStore(BaseDataStore):
    def __init__(self, *args, **kwargs):
        super(DictDataStore, self).__init__(*args, **kwargs)
        self.data = {}

    def iteritems(self):
        return self.data.iteritems()

    def save(self, spec, object):
        self.data[spec] = object

    def _get(self, spec):
        if spec not in self.data: raise KeyError("Spec not found: {}".format(spec))
        return self.data.get(spec)

    def iterkeys(self):
        return self.data.iterkeys()
