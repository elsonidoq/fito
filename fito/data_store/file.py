import mmh3
import os
import shutil

import pickle
from fito.data_store.base import BaseDataStore
from fito.operations.base import Operation, GetOperation


class FileOperation(Operation):
    @classmethod
    def load(cls, dir):
        raise NotImplementedError()


def default_save(obj, subdir):
    with open(os.path.join(subdir, 'obj.pkl'), 'w') as f:
        pickle.dump(obj, f, 2)


def default_load(subdir):
    with open(os.path.join(subdir, 'obj.pkl')) as f:
        return pickle.load(f)


class FileDataStore(BaseDataStore):
    def __init__(self, path, get_cache_size=0, execute_cache_size=0, split_keys=True):
        super(FileDataStore, self).__init__(get_cache_size=get_cache_size, execute_cache_size=execute_cache_size)
        self.split_keys = split_keys
        self.path = path

    def clean(self, cls=None):
        for op in self.iterkeys():
            if cls is None or (cls is not None and isinstance(op, cls)):
                self.remove(op)

    def remove(self, op):
        subdir = self._get_subdir(op)
        shutil.rmtree(subdir)

    def iterkeys(self):
        for subdir, _, _ in os.walk(self.path):
            subdir = os.path.join(self.path, subdir)
            key_fname = os.path.join(subdir, 'key')
            if not os.path.exists(key_fname): continue

            with open(key_fname) as f:
                key = f.read()
            try:
                op = Operation.key2operation(key)
            except ValueError:
                op = self._get_operation(key)
            yield op

    def iteritems(self):
        for op in self.iterkeys():
            yield op, self.get(op)

    def _get_dir(self, series_name_or_operation):
        key = self._get_key(series_name_or_operation)
        h = str(mmh3.hash(key))
        if self.split_keys:
            fname = os.path.join(self.path, h[:3], h[3:6], h[6:])
        else:
            fname = os.path.join(self.path, h)
        return fname

    def _get_subdir(self, series_name_or_operation):
        dir = self._get_dir(series_name_or_operation)
        if not os.path.exists(dir): raise ValueError("Operation not found")
        op_key = self._get_key(series_name_or_operation)
        for subdir in os.listdir(dir):
            subdir = os.path.join(dir, subdir)
            if not os.path.isdir(subdir): continue
            with open(os.path.join(subdir, 'key')) as f:
                key = f.read()
            if key == op_key: break
        else:
            raise ValueError("Operation not found")
        return subdir

    def _get(self, series_name_or_operation):
        subdir = self._get_subdir(series_name_or_operation)
        if hasattr(series_name_or_operation, 'load'):
            return series_name_or_operation.load(subdir)
        else:
            return default_load(subdir)

    def save(self, series_name_or_operation, series):
        dir = self._get_dir(series_name_or_operation)
        if not os.path.exists(dir): os.makedirs(dir)
        op_key = self._get_key(series_name_or_operation)
        for subdir in os.listdir(dir):
            subdir = os.path.join(dir, subdir)
            with open(os.path.join(subdir, 'key')) as f:
                key = f.read()
            if key == op_key: break
        else:
            subdirs = map(int, os.listdir(dir))
            if len(subdirs) == 0:
                subdir = '0'
            else:
                subdir = str(max(subdirs) + 1)
            subdir = os.path.join(dir, subdir)
            os.makedirs(subdir)

        with open(os.path.join(subdir, 'key'), 'w') as f:
            f.write(op_key)

        if hasattr(series, 'save'):
            series.save(subdir)
        else:
            default_save(series, subdir)

    @classmethod
    def _get_key(cls, series_name_or_operation):
        operation = cls._get_operation(series_name_or_operation)
        return operation.key

    def __contains__(self, series_name_or_operation):
        try:
            self._get_subdir(series_name_or_operation)
            return True
        except ValueError:
            return False

    @classmethod
    def _get_operation(cls, series_name_or_operation):
        if isinstance(series_name_or_operation, basestring):
            return GetOperation(name=series_name_or_operation)
        elif isinstance(series_name_or_operation, Operation):
            return series_name_or_operation
        else:
            raise ValueError("invalid argument")
