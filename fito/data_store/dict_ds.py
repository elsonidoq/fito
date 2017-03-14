from fito.data_store.base import BaseDataStore
from fito.specs import base


class DictDataStore(BaseDataStore):
    def __init__(self, *args, **kwargs):
        super(DictDataStore, self).__init__(*args, **kwargs)
        self.data = {}

    def iteritems(self):
        return self.data.iteritems()

    def _save(self, spec, object):
        self.data[spec] = object

    def _get(self, spec):
        if spec not in self.data: raise KeyError("Spec not found: {}".format(spec))
        return self.data.get(spec)

    def iterkeys(self, raw=False):
        for key in self.data.iterkeys():
            if raw:
                yield key, key.to_dict()
            else:
                yield key

    def clean(self):
        self.data = {}

    def get_by_id(self, spec):
        return self._get(spec)
