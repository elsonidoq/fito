from time import time, sleep
import json
import mmh3
import os
import pickle
import shutil

from fito.data_store.base import BaseDataStore
from fito.operations.base import Operation, GetOperation


class Serializer(object):
    @classmethod
    def save(cls, obj, subdir): raise NotImplemented()

    @classmethod
    def load(cls, subdir): raise NotImplemented()

    @classmethod
    def exists(cls, subdir): raise NotImplemented()

    @classmethod
    def iter_subclasses(cls):
        queue = cls.__subclasses__()
        while len(queue) > 0:
            e = queue.pop()
            l = e.__subclasses__()
            yield e
            queue.extend(l)


class SingleFileSerializer(Serializer):
    @classmethod
    def get_fname(cls, subdir): raise NotImplemented()

    @classmethod
    def exists(cls, subdir):
        return os.path.exists(cls.get_fname(subdir))

class PickleSerializer(SingleFileSerializer):
    @classmethod
    def get_fname(cls, subdir):
        return os.path.join(subdir, 'obj.pkl')

    @classmethod
    def save(cls, obj, subdir):
        with open(cls.get_fname(subdir), 'w') as f:
            pickle.dump(obj, f, 2)

    @classmethod
    def load(cls, subdir):
        with open(cls.get_fname(subdir)) as f:
            return pickle.load(f)

class RawSerializer(SingleFileSerializer):
    @classmethod
    def get_fname(cls, subdir):
        return os.path.join(subdir, 'obj.raw')

    @classmethod
    def save(cls, obj, subdir):
        with open(cls.get_fname(subdir), 'w') as f:
            f.write(obj)

    @classmethod
    def load(cls, subdir):
        with open(cls.get_fname(subdir)) as f:
            return f.read()


class FileDataStore(BaseDataStore):
    def __init__(self, path, get_cache_size=0, execute_cache_size=0, split_keys=True, serializer=None):
        super(FileDataStore, self).__init__(get_cache_size=get_cache_size, execute_cache_size=execute_cache_size)
        self.split_keys = split_keys
        self.path = path
        if not os.path.exists(path): os.makedirs(path)

        conf_file = os.path.join(path, 'conf.json')
        if os.path.exists(conf_file):
            with open(conf_file) as f:
                conf = json.load(f)

            if serializer is not None:
                if conf['serializer'] != serializer.__name__:
                    raise ValueError(
                        'This store was initialized with {} Serializer, but now received {}'.format(conf['serializer'],
                                                                                                    serializer.__name__)
                    )
            else:
                for cls in Serializer.iter_subclasses():
                    if cls.__name__ == conf['serializer']:
                        serializer = cls
                        break
                else:
                    raise ValueError('Could not find serializer {}'.format(conf['serializer']))

        else:
            serializer = serializer or PickleSerializer
            this_conf = {
                # this should include the path...
                'serializer': serializer.__name__
            }

            with open(conf_file, 'w') as f:
                json.dump(this_conf, f)

        self.serializer = serializer

    def clean(self, cls=None):
        for op in self.iterkeys():
            if cls is None or (cls is not None and isinstance(op, cls)):
                self.remove(op)

    def remove(self, op):
        subdir = self._get_subdir(op)
        shutil.rmtree(subdir)

    def iterkeys(self):
        for subdir, _, _ in os.walk(self.path):
            key_fname = os.path.join(subdir, 'key')
            if not os.path.exists(key_fname): continue

            with open(key_fname) as f:
                key = f.read()

            try:
                op = Operation.key2operation(key)
            except ValueError, e: # there might be a key that is not a valid json
                if e.args[0] == 'Unknown operation type': raise e
                continue

            yield op

    def iteritems(self):
        for op in self.iterkeys():
            try:
                yield op, self.get(op)
            except:
                # TODO: check whether the file exists or not
                continue

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
        if not os.path.exists(dir): raise KeyError("Operation not found")

        op_key = self._get_key(series_name_or_operation)
        subdirs = os.listdir(dir)
        for subdir in subdirs:
            subdir = os.path.join(dir, subdir)
            if not os.path.isdir(subdir): continue
            key_fname = os.path.join(subdir, 'key')
            if not os.path.exists(key_fname): continue

            with open(key_fname) as f:
                key = f.read()

            if len(key) == 0 and time() - os.path.getctime(key_fname) < 0.5:
                sleep(0.5)
                print "sleep(0.5)"
                with open(key_fname) as f:
                    key = f.read()
            if key == op_key and self.serializer.exists(subdir): break
        else:
            raise KeyError("Operation not found")

        return subdir

    def _get(self, series_name_or_operation):
        subdir = self._get_subdir(series_name_or_operation)
        try: return self.serializer.load(subdir)
        except: raise KeyError('{} not found'.format(series_name_or_operation))

    def save(self, series_name_or_operation, series):
        dir = self._get_dir(series_name_or_operation)
        # this accounts for both checking if it not exists, and the fact that there might
        # be another process doing the same thing
        try: os.makedirs(dir)
        except OSError: pass
        op_key = self._get_key(series_name_or_operation)
        for subdir in os.listdir(dir):
            subdir = os.path.join(dir, subdir)
            key_fname = os.path.join(subdir, 'key')
            if not os.path.exists(key_fname): continue
            with open(key_fname) as f:
                key = f.read()
            if key == op_key: break
        else:
            while True:
                subdirs = map(int, os.listdir(dir))
                if len(subdirs) == 0:
                    subdir = '0'
                else:
                    subdir = str(max(subdirs) + 1)
                subdir = os.path.join(dir, subdir)
                try:
                    os.makedirs(subdir)
                    break
                except OSError:
                    pass

        with open(os.path.join(subdir, 'key'), 'w') as f:
            f.write(op_key)

        self.serializer.save(series, subdir)

    @classmethod
    def _get_key(cls, series_name_or_operation):
        operation = cls._get_operation(series_name_or_operation)
        return operation.key

    def __contains__(self, series_name_or_operation):
        try:
            subdir = self._get_subdir(series_name_or_operation)
            return self.serializer.exists(subdir)
        except KeyError:
            return False

    @classmethod
    def _get_operation(cls, series_name_or_operation):
        if isinstance(series_name_or_operation, basestring):
            return GetOperation(name=series_name_or_operation)
        elif isinstance(series_name_or_operation, Operation):
            return series_name_or_operation
        else:
            raise ValueError("invalid argument")
