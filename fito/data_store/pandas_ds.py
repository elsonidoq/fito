from memoized_property import memoized_property
from fito.operations import GetOperation, Operation

import pandas as pd
from fito.data_store.base import BaseDataStore


class PandasDataStore(BaseDataStore):
    """
    DataStore implemented using a pandas data frame
    """

    def __init__(self, df=None):
        super(PandasDataStore, self).__init__()
        self.fdf = pd.DataFrame() if df is None else df

    @classmethod
    def read_hdf(cls, fname):
        return cls(pd.read_hdf(fname, 'df'))

    def _get(self, series_name_or_operation):
        key = self._get_key(series_name_or_operation)
        try:
            return self.fdf[key]
        except KeyError:
            raise ValueError("Operation not found")

    def save(self, operation, series):
        assert self.fdf.index.is_monotonic and series.index.is_monotonic

        if len(self.fdf) > 0 and \
                (self.fdf.index[0] > series.index[0] or self.fdf.index[-1] < series.index[-1]):
            # we would be losing data if we save the series with the current index
            new_index = self.fdf.index.join(series.index, how='outer')
            self.fdf = self.fdf.reindex(new_index, copy=False)
        self.fdf[self._get_key(operation)] = series

    def iteritems(self):
        for k, v in self.fdf.iteritems():
            try:
                op = Operation.key2operation(k)
            except ValueError:
                op = GetOperation(series_name=k)
            yield op, v

    @memoized_property
    def operations(self):
        res = []
        for k in self.fdf.keys():
            try:
                res.append(Operation.key2operation(k))
            except ValueError:
                res.append(GetOperation(k))
        return res

    def search(self, query):
        for op in self.operations:
            if query.matches(op):
                yield op, self.fdf[self._get_key(op)]

    def __len__(self):
        return len(self.fdf)


